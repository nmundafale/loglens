import os
import time
import asyncio
import json
import uvicorn
import logging
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from typing import Optional

from loglens.processors.apache_spark.error_diagnosis import process_error_diagnosis
from loglens.processors.apache_spark.cluster_utilization import process_cluster_utilization

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Loglens API",
    description="Event-driven REST server for analyzing AI logs.",
    version="1.0.0"
)

class AnalyzeRequest(BaseModel):
    storage_type: str = Field(
        default="local", 
        description="The storage driver to pull the log from (currently only 'local' is supported)."
    )
    log_path: str = Field(
        ..., 
        description="The path or URI to the log file (e.g. '/path/to/log.txt' for 'local' storage)."
    )
    system: str = Field(
        default="spark",
        description="The target system of the log file (e.g., 'spark')."
    )
    mode: str = Field(
        default="diagnosis",
        description="The mode of the AI agent: 'diagnosis' or 'utilization'."
    )
    intent: Optional[str] = Field(
        default=None,
        description="Specific instructions for the AI. If omitted, it derives a default from the mode."
    )
    github_repo: Optional[str] = Field(
        default=None,
        description="Optional Target GitHub Repository (e.g. 'nmundafale/loglens-demo-repo') to create a PR against."
    )
    etl_file_path: Optional[str] = Field(
        default=None,
        description="Optional Specific code file path inside the repository to analyze and replace (e.g. 'src/oom_job.py')."
    )

class AnalyzeResponse(BaseModel):
    analysis: str
    github_pr_url: Optional[str] = None
    execution_time_seconds: float

@app.post("/analyze", response_model=AnalyzeResponse)
async def analyze_log(request: AnalyzeRequest):
    start_time = time.time()
    
    # Storage Validation
    if request.storage_type != "local":
        raise HTTPException(status_code=400, detail=f"Unsupported storage_type: '{request.storage_type}'. Only 'local' is supported.")
    
    if not os.path.exists(request.log_path):
        raise HTTPException(status_code=404, detail=f"Log file not found at path: {request.log_path}")

    # Read File
    try:
        with open(request.log_path, "r", encoding="utf-8") as f:
            log_text = f.read()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read file: {str(e)}")

    # Intent Defaults
    intent = request.intent
    if not intent:
        if request.mode == "diagnosis":
            intent = "Identify the root cause of the failure and provide the PySpark code to fix it."
        else:
            intent = "Determine if the cluster is under-utilized or if tasks are skewed, and provide PySpark configuration settings."

    # Process based on System and Mode
    if request.system == "spark":
        if request.mode == "diagnosis":
            final_diagnosis = await process_error_diagnosis(
                log_text=log_text, 
                intent=intent, 
                github_repo=request.github_repo, 
                etl_file_path=request.etl_file_path
            )
        elif request.mode == "utilization":
            final_diagnosis = await process_cluster_utilization(
                log_text=log_text, 
                intent=intent,
                github_repo=request.github_repo, 
                etl_file_path=request.etl_file_path
            )
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported mode: '{request.mode}'")
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported system: '{request.system}'")
        
    execution_time = round(time.time() - start_time, 2)
    
    # Check if a PR was returned via a tuple (Analysis, PR_URL)
    pr_url = None
    if isinstance(final_diagnosis, tuple):
        final_diagnosis, pr_url = final_diagnosis

    return AnalyzeResponse(
        analysis=final_diagnosis,
        github_pr_url=pr_url,
        execution_time_seconds=execution_time
    )

@app.post("/analyze/stream")
async def analyze_log_stream(request: AnalyzeRequest, req: Request):
    """Event-driven stream for Map-Reduce phase updates via Server-Sent Events (SSE)."""
    # Storage Validation
    if request.storage_type != "local":
        raise HTTPException(status_code=400, detail=f"Unsupported storage_type: '{request.storage_type}'. Only 'local' is supported.")
    
    if not os.path.exists(request.log_path):
        raise HTTPException(status_code=404, detail=f"Log file not found at path: {request.log_path}")

    # Read File
    try:
        with open(request.log_path, "r", encoding="utf-8") as f:
            log_text = f.read()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read file: {str(e)}")

    # Intent Defaults
    intent = request.intent
    if not intent:
        if request.mode == "diagnosis":
            intent = "Identify the root cause of the failure and provide the PySpark code to fix it."
        else:
            intent = "Determine if the cluster is under-utilized or if tasks are skewed, and provide PySpark configuration settings."

    # Async Queue for SSE Broadcasting
    q = asyncio.Queue()

    async def status_callback(msg: str):
        # We wrap messages in a JSON envelope
        payload = {"type": "status", "message": msg}
        await q.put(payload)

    async def execute_task():
        start_time = time.time()
        try:
            if request.system == "spark":
                if request.mode == "diagnosis":
                    final_diagnosis = await process_error_diagnosis(
                        log_text=log_text, 
                        intent=intent, 
                        github_repo=request.github_repo, 
                        etl_file_path=request.etl_file_path,
                        status_callback=status_callback
                    )
                elif request.mode == "utilization":
                    final_diagnosis = await process_cluster_utilization(
                        log_text=log_text, 
                        intent=intent,
                        github_repo=request.github_repo, 
                        etl_file_path=request.etl_file_path,
                        status_callback=status_callback
                    )
                else:
                    await q.put({"type": "error", "message": f"Unsupported mode: '{request.mode}'"})
                    return
            else:
                await q.put({"type": "error", "message": f"Unsupported system: '{request.system}'"})
                return
                
            execution_time = round(time.time() - start_time, 2)
            pr_url = None
            if isinstance(final_diagnosis, tuple):
                final_diagnosis, pr_url = final_diagnosis

            # Send final payload
            await q.put({
                "type": "result", 
                "analysis": final_diagnosis,
                "github_pr_url": pr_url,
                "execution_time_seconds": execution_time
            })
            
        except Exception as e:
            logger.error(f"Streaming execution failed: {str(e)}", exc_info=True)
            await q.put({"type": "error", "message": f"Execution Failed: {str(e)}"})
        finally:
            await q.put(None) # Sentinel to close the stream

    # Start map reduce in the background
    task = asyncio.create_task(execute_task())

    async def event_generator():
        try:
            while True:
                # If client drops connection, stop processing
                if await req.is_disconnected():
                    task.cancel()
                    break
                    
                msg = await q.get()
                if msg is None: # Sentinel
                    break
                    
                yield f"data: {json.dumps(msg)}\n\n"
        except asyncio.CancelledError:
            task.cancel()
            
    return StreamingResponse(event_generator(), media_type="text/event-stream")

def start():
    """Entry point for the `loglens-server` CLI command."""
    print("Starting Loglens API Server...")
    uvicorn.run("loglens.api.server:app", host="0.0.0.0", port=8000, reload=False)

if __name__ == "__main__":
    start()
