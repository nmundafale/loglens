import uuid
import logging
from typing import Optional, Any
from github import Github
import jwt
import time
import requests

from loglens.processors.apache_spark.config import settings

import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class GitHubService:
    """Wrapper for PyGithub to authenticate via GitHub App and create Pull Requests."""
    
    def __init__(self):
        self.app_id = settings.github_app_id
        self.private_key = settings.github_app_private_key
        self.private_key_path = settings.github_app_private_key_path
        self.installation_id = settings.github_installation_id
        
        if not self.private_key and self.private_key_path:
            import os
            if os.path.exists(self.private_key_path):
                with open(self.private_key_path, "r", encoding="utf-8") as f:
                    self.private_key = f.read().strip()
            else:
                raise ValueError(f"GitHub App Private Key file not found at: {self.private_key_path}")

        if not all([self.app_id, self.private_key, self.installation_id]):
            raise ValueError("Missing required GitHub App credentials in environment variables (APP_ID, INSTALLATION_ID, and either PRIVATE_KEY or PRIVATE_KEY_PATH).")
        
        # Format the private key cleanly if it was passed via single-line ENV var
        if "\\n" in self.private_key:
            self.private_key = self.private_key.replace("\\n", "\n")
            
        # Ensure the cryptography library has the required PEM headers
        if "-----BEGIN RSA PRIVATE KEY-----" not in self.private_key:
            self.private_key = f"-----BEGIN RSA PRIVATE KEY-----\n{self.private_key}\n-----END RSA PRIVATE KEY-----"

    def _get_installation_token(self) -> str:
        """Exchanges App Credentials for a short-lived Installation Access Token via explicit JWT and REST."""
        now = int(time.time())
        payload = {
            "iat": now - 60,
            "exp": now + (10 * 60),
            "iss": self.app_id
        }
        
        # 1. Create the JWT manually
        encoded_jwt = jwt.encode(payload, self.private_key, algorithm="RS256")
        
        # 2. Get the specific installation manually
        import requests
        headers = {
            "Authorization": f"Bearer {encoded_jwt}",
            "Accept": "application/vnd.github.v3+json"
        }
        
        resp = requests.post(
            f"https://api.github.com/app/installations/{self.installation_id}/access_tokens",
            headers=headers
        )
        resp.raise_for_status()
        return resp.json()["token"]

    def _get_client(self) -> Github:
        """Returns an authenticated PyGithub client using the installation token."""
        token = self._get_installation_token()
        return Github(token)

    def fetch_file_content(self, repo_name: str, file_path: str) -> str:
        """Downloads the source code of a specific file from the repository."""
        file_path = file_path.replace("\\", "/")
        logger.info(f"Fetching '{file_path}' from '{repo_name}'...")
        client = self._get_client()
        repo = client.get_repo(repo_name)
        
        # PyGithub get_contents returns ContentFile or List[ContentFile]. 
        # For a specific file, it is always a single ContentFile, but mypy doesn't know that.
        file_content: Any = repo.get_contents(file_path)
        
        if isinstance(file_content, list):
            raise ValueError(f"Expected a single file at '{file_path}', but found a directory.")
            
        return file_content.decoded_content.decode("utf-8")

    def create_pull_request(self, repo_name: str, file_path: str, new_code: str, pr_title: str, pr_body: str) -> Optional[str]:
        """Commits the new code to a fresh branch and opens a Pull Request."""
        file_path = file_path.replace("\\", "/")
        logger.info(f"Creating Pull Request in '{repo_name}' for '{file_path}'...")
        client = self._get_client()
        repo = client.get_repo(repo_name)
        
        # Extract default branch
        target_branch = repo.default_branch
        main_ref = repo.get_git_ref(f"heads/{target_branch}")
        
        # Create a unique branch name
        unique_id = str(uuid.uuid4())[:8]
        new_branch_name = f"loglens-fix-{unique_id}"
        
        # Create the new branch from main
        repo.create_git_ref(ref=f"refs/heads/{new_branch_name}", sha=main_ref.object.sha)
        logger.info(f"Created new branch: {new_branch_name}")
        
        # Update the file in the new branch
        original_file: Any = repo.get_contents(file_path, ref=target_branch)
        if isinstance(original_file, list):
            raise ValueError(f"Expected a single file at '{file_path}', but found a directory.")
            
        repo.update_file(
            path=file_path,
            message=pr_title,
            content=new_code,
            sha=original_file.sha,
            branch=new_branch_name
        )
        logger.info(f"Committed modified file to branch: {new_branch_name}")
        
        # Open the Pull Request
        pr = repo.create_pull(
            title=pr_title,
            body=pr_body,
            head=new_branch_name,
            base=target_branch
        )
        logger.info(f"Pull Request created successfully: {pr.html_url}")
        
        return pr.html_url
