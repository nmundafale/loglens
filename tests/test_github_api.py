import httpx
import asyncio
import json

async def test_api():
    print("Sending Analysis Request...")
    
    payload = {
        "log_path": r"C:\workspace\loglens-demo-repo\logs\oom_job.stderr",
        "storage_type": "local",
        "system": "spark",
        "mode": "diagnosis",
        "github_repo": "nmundafale/loglens-demo-repo",
        "etl_file_path": r"src\oom_job.py"
    }

    async with httpx.AsyncClient() as client:
        try:
            async with client.stream(
                "POST", 
                "http://localhost:8000/analyze/stream", 
                json=payload, 
                timeout=httpx.Timeout(600.0, read=None)
            ) as response:
                print(f"Status: {response.status_code}")
                async for line in response.aiter_lines():
                    if line.startswith("data: "):
                        data_str = line[6:]
                        try:
                            msg = json.loads(data_str)
                            if msg["type"] == "status":
                                print(f"[STREAM] {msg['message']}")
                            elif msg["type"] == "result":
                                print("\n[STREAM] === FINAL RESULT ===")
                                print(f"PR URL: {msg.get('github_pr_url')}")
                                print(f"Time: {msg.get('execution_time_seconds')}s")
                                print(f"Analysis:\n{msg.get('analysis')}")
                            elif msg["type"] == "error":
                                print(f"[STREAM ERROR] {msg['message']}")
                        except json.JSONDecodeError:
                            print(f"[STREAM RAW] {line}")
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_api())
