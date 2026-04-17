"""Fetch GitHub commits from user's repositories."""

import requests
from typing import Iterator, Optional
from dataclasses import dataclass
from datetime import datetime


# Custom exception classes for GitHub API errors
class GitHubClientError(Exception):
    """Base exception for GitHub API client errors."""
    pass


class GitHubRateLimitError(GitHubClientError):
    """Raised when GitHub API rate limit is exceeded (403)."""
    pass


class GitHubAuthError(GitHubClientError):
    """Raised when GitHub API authentication fails (401)."""
    pass


class GitHubNotFoundError(GitHubClientError):
    """Raised when GitHub API resource is not found (404)."""
    pass


@dataclass
class Commit:
    repo_name: str
    sha: str
    message: str
    timestamp: datetime
    author: str
    url: str

    def to_dict(self) -> dict:
        return {
            "repo_name": self.repo_name,
            "sha": self.sha,
            "message": self.message,
            "timestamp": self.timestamp.isoformat(),
            "author": self.author,
            "url": self.url
        }


class GitHubClient:
    BASE_URL = "https://api.github.com"

    def __init__(self, token: str, username: str, timeout: int = 30):
        self.token = token
        self.username = username
        self.timeout = timeout
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github.v3+json"
        }

    def get_user_repos(self, include_forks: bool = False) -> list[dict]:
        """Get all repositories owned by the user (including private)."""
        repos = []
        page = 1

        while True:
            # Use /user/repos endpoint to include private repos
            try:
                response = requests.get(
                    f"{self.BASE_URL}/user/repos",
                    headers=self.headers,
                    params={
                        "affiliation": "owner",
                        "sort": "pushed",
                        "per_page": 100,
                        "page": page
                    },
                    timeout=self.timeout,
                )
                response.raise_for_status()
            except requests.exceptions.ConnectionError as e:
                raise GitHubClientError(f"Connection error: {e}") from e
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 401:
                    raise GitHubAuthError(f"Authentication failed: {e}") from e
                elif e.response.status_code == 403:
                    raise GitHubRateLimitError(f"Rate limit exceeded: {e}") from e
                elif e.response.status_code == 404:
                    raise GitHubNotFoundError(f"Resource not found: {e}") from e
                else:
                    raise GitHubClientError(f"HTTP error: {e}") from e

            data = response.json()

            if not data:
                break

            for repo in data:
                if not include_forks and repo.get("fork"):
                    continue
                repos.append(repo)

            page += 1

        return repos

    def get_repo_commits(
        self,
        repo_name: str,
        since: Optional[datetime] = None,
        limit: int = 100
    ) -> Iterator[Commit]:
        """Get commits from a specific repository."""
        params = {
            "author": self.username,
            "per_page": min(limit, 100)
        }
        if since:
            params["since"] = since.isoformat()

        try:
            response = requests.get(
                f"{self.BASE_URL}/repos/{self.username}/{repo_name}/commits",
                headers=self.headers,
                params=params,
                timeout=self.timeout,
            )

            if response.status_code == 409:  # Empty repository
                return

            response.raise_for_status()
        except requests.exceptions.ConnectionError as e:
            raise GitHubClientError(f"Connection error: {e}") from e
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                raise GitHubAuthError(f"Authentication failed: {e}") from e
            elif e.response.status_code == 403:
                raise GitHubRateLimitError(f"Rate limit exceeded: {e}") from e
            elif e.response.status_code == 404:
                raise GitHubNotFoundError(f"Resource not found: {e}") from e
            else:
                raise GitHubClientError(f"HTTP error: {e}") from e

        for commit_data in response.json():
            commit = commit_data["commit"]
            yield Commit(
                repo_name=repo_name,
                sha=commit_data["sha"],
                message=commit["message"],
                timestamp=datetime.fromisoformat(
                    commit["author"]["date"].replace("Z", "+00:00")
                ),
                author=commit["author"]["name"],
                url=commit_data["html_url"]
            )

    def get_all_recent_commits(
        self,
        since: Optional[datetime] = None,
        include_forks: bool = False
    ) -> Iterator[Commit]:
        """Get recent commits from all user repositories."""
        repos = self.get_user_repos(include_forks=include_forks)

        for repo in repos:
            try:
                for commit in self.get_repo_commits(repo["name"], since=since):
                    yield commit
            except (GitHubRateLimitError, GitHubNotFoundError):
                # Skip repos we can't access due to rate limits or not found
                pass


def poll_new_commits(
    token: str,
    username: str,
    since: datetime,
    db
) -> list[Commit]:
    """Poll for new commits and store them in the database.

    Returns list of newly discovered commits.
    """
    client = GitHubClient(token, username)
    new_commits = []

    for commit in client.get_all_recent_commits(since=since):
        if not db.is_commit_processed(commit.sha):
            db.insert_commit(
                repo_name=commit.repo_name,
                commit_sha=commit.sha,
                commit_message=commit.message,
                timestamp=commit.timestamp.isoformat(),
                author=commit.author
            )
            new_commits.append(commit)

    return new_commits
