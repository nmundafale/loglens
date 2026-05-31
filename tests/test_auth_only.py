from loglens.core.github_service import GitHubService

def test_auth():
    print("Initializing GitHubService...")
    service = GitHubService()
    
    print("Requesting Installation Token...")
    try:
        token = service._get_installation_token()
        print(f"SUCCESS! Token retrieved: {token[:10]}...")
    except Exception as e:
        print(f"FAILED: {e}")

if __name__ == "__main__":
    test_auth()
