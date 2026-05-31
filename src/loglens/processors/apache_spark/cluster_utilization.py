import asyncio
import time
import json
from typing import Optional, Tuple, Callable, Awaitable
from loglens.processors.apache_spark.config import APP_CONFIG
from loglens.processors.apache_spark.utils import engine, chunk_log_by_lines
from loglens.core.github_service import GitHubService

async def process_cluster_utilization(
    log_text: str, 
    intent: str,
    github_repo: Optional[str] = None,
    etl_file_path: Optional[str] = None,
    status_callback: Optional[Callable[[str], Awaitable[None]]] = None
) -> str | Tuple[str, str]:
    """Executes the Map-Reduce pipeline for Spark cluster utilization analysis."""
    async def log_status(msg: str):
        print(msg)
        if status_callback:
            await status_callback(msg)

    # Pull variables from config
    map_tier = APP_CONFIG["loglens_tasks"]["map_phase"]
    reduce_tier = APP_CONFIG["loglens_tasks"]["reduce_phase"]
    chunk_size = APP_CONFIG["processing"]["target_chunk_chars"]
    
    map_prompt = APP_CONFIG["prompts"]["utilization_map"]
    reduce_prompt = APP_CONFIG["prompts"]["utilization_reduce"].format(intent=intent)
    
    concurrency_limit = APP_CONFIG["ai_tiers"][map_tier].get("max_concurrent", 1)
    semaphore = asyncio.Semaphore(concurrency_limit)

    print(f"--- Map Phase [Utilization] (Tier {map_tier} | Concurrency: {concurrency_limit}) ---")
    log_chunks = chunk_log_by_lines(log_text, chunk_size)
    
    async def bounded_call(chunk_data, chunk_index):
        async with semaphore:
            print(f"Processing chunk {chunk_index + 1}/{len(log_chunks)}...")
            return await engine.call_model(map_tier, map_prompt, chunk_data)

    # 1. Map
    map_start_time = time.time()
    tasks = [bounded_call(chunk, i) for i, chunk in enumerate(log_chunks)]
    map_results = await asyncio.gather(*tasks)
    map_duration = time.time() - map_start_time
    
    # 2. Concatenate and validate map results
    valid_results = [r.strip() for r in map_results if r and r.strip() and r.strip() not in ("[]", "{}")]
    distilled_logs = "\n".join(valid_results)
    
    original_size = len(log_text)
    distilled_size = len(distilled_logs)
    reduction_pct = ((original_size - distilled_size) / original_size) * 100 if original_size > 0 else 0
    metrics_str = f"Context reduced by {reduction_pct:.1f}% ({original_size} -> {distilled_size} chars)"
    await log_status(f"--- Map Phase Complete in {map_duration:.2f}s. {metrics_str} ---")

    if not distilled_logs:
        return "No relevant Spark utilization telemetry was found in the logs."

    # 3. Retrieve Source Code (if GitHub Integration is requested)
    source_code = ""
    github_service = None
    if github_repo and etl_file_path:
        await log_status(f"--- Fetching Source Context from GitHub ({github_repo}/{etl_file_path}) ---")
        try:
            github_service = GitHubService()
            source_code = await asyncio.to_thread(
                github_service.fetch_file_content,
                repo_name=github_repo, 
                file_path=etl_file_path
            )
            distilled_logs = f"=== ORIGINAL SOURCE CODE ===\n{source_code}\n\n=== DISTILLED LOGS ===\n{distilled_logs}"
        except Exception as e:
            await log_status(f"Warning: Failed to fetch GitHub source code: {e}")

    # 4. Reduce
    await log_status(f"--- Reduce Phase [Utilization] (Tier {reduce_tier}) ---")
    reduce_start_time = time.time()
    
    # LiteLLM JSON Enforcement requires 'json' literal in the system prompt
    reduce_prompt += " You must output in strictly valid JSON format."
    raw_response = await engine.call_model(reduce_tier, reduce_prompt, distilled_logs)
    reduce_duration = time.time() - reduce_start_time
    
    total_duration = map_duration + reduce_duration
    await log_status(f"--- Reduce Phase Complete in {reduce_duration:.2f}s (Total Pipeline Time: {total_duration:.2f}s) ---")
    
    # 5. Parse JSON and Open PR
    try:
        parsed = json.loads(raw_response)
        final_analysis: str = str(parsed.get("explanation", raw_response))
        fixed_code: Optional[str] = parsed.get("fixed_code", None)
        
        if isinstance(fixed_code, str):
            fixed_code = fixed_code.strip()
            # Handle AI double-encoded JSON stringification
            if fixed_code.startswith('"') and fixed_code.endswith('"'):
                try:
                    fixed_code = json.loads(fixed_code)
                except json.JSONDecodeError:
                    fixed_code = fixed_code[1:-1].replace('\\n', '\n').replace('\\"', '"')
            # Handle accidental markdown code blocks inside the JSON string
            if fixed_code.startswith("```"):
                lines = fixed_code.split("\n")
                if lines[0].startswith("```"):
                    lines = lines[1:]
                if lines and lines[-1].strip().startswith("```"):
                    lines = lines[:-1]
                fixed_code = "\n".join(lines).strip()

        pr_url: Optional[str] = None
        if github_service and github_repo and etl_file_path and fixed_code:
            await log_status(f"Pushing Pull Request to {github_repo}...")
            pr_url = await asyncio.to_thread(
                github_service.create_pull_request,
                repo_name=github_repo,
                file_path=etl_file_path,
                new_code=fixed_code,
                pr_title=f"Loglens Utilization Optimization: {etl_file_path}",
                pr_body=f"Automated PR created by Loglens AI to optimize cluster utilization. \n\n### Pipeline Metrics\n- **{metrics_str}**\n- Map-Reduce Execution Time: {total_duration:.2f}s\n\n### Diagnosis\n{final_analysis}"
            )
            if pr_url:
                await log_status(f"Pull Request created successfully: {pr_url}")
                return final_analysis, pr_url
            
        return final_analysis
    except json.JSONDecodeError:
        await log_status("Warning: AI failed to output valid JSON. Returning raw string.")
        return raw_response
