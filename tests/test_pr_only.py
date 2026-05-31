from loglens.core.github_service import GitHubService
import asyncio

async def test_pr():
    print("Initializing GitHubService...")
    service = GitHubService()
    
    print("Attempting to create PR...")
    try:
        # We wrapped this in asyncio.to_thread in the main class, but we can call it directly here since it's sync
        pr_url = service.create_pull_request(
            repo_name="nmundafale/loglens-demo-repo",
            file_path="src/oom_job.py",
            new_code="# Test comment\nprint('hello world')",
            pr_title="Test PR Connection",
            pr_body="Testing PyGithub Auth Flow"
        )
        print(f"SUCCESS! PR Created: {pr_url}")
    except Exception as e:
        print(f"FAILED: {e}")

if __name__ == "__main__":
    asyncio.run(test_pr())
