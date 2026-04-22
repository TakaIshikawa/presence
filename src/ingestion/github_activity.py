"""Fetch recently updated GitHub issues, pull requests, and releases."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable, Iterator, Optional

import requests

from ingestion.github_commits import (
    GitHubAuthError,
    GitHubClientError,
    GitHubNotFoundError,
    GitHubRateLimitError,
)
from ingestion.redaction import Redactor

BODY_EXCERPT_MAX_CHARS = 1000


@dataclass
class GitHubActivity:
    repo_name: str
    activity_type: str
    number: int
    title: str
    state: str
    author: str
    url: str
    updated_at: datetime
    created_at: datetime
    body: str = ""
    closed_at: Optional[datetime] = None
    merged_at: Optional[datetime] = None
    labels: list[str] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)

    @property
    def activity_id(self) -> str:
        return f"{self.repo_name}#{self.number}:{self.activity_type}"

    def to_dict(self) -> dict:
        return {
            "repo_name": self.repo_name,
            "activity_type": self.activity_type,
            "number": self.number,
            "title": self.title,
            "state": self.state,
            "author": self.author,
            "url": self.url,
            "updated_at": self.updated_at.isoformat(),
            "created_at": self.created_at.isoformat(),
            "body": self.body,
            "closed_at": self.closed_at.isoformat() if self.closed_at else None,
            "merged_at": self.merged_at.isoformat() if self.merged_at else None,
            "labels": self.labels,
            "metadata": self.metadata,
        }


def _parse_github_datetime(value: str | None) -> Optional[datetime]:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _body_excerpt(value: str | None, max_len: int = BODY_EXCERPT_MAX_CHARS) -> str:
    cleaned = " ".join(str(value or "").split())
    if len(cleaned) <= max_len:
        return cleaned
    if max_len <= 3:
        return cleaned[:max_len]
    return cleaned[: max_len - 3].rstrip() + "..."


class GitHubActivityClient:
    BASE_URL = "https://api.github.com"
    GRAPHQL_URL = "https://api.github.com/graphql"

    def __init__(
        self,
        token: str,
        username: str,
        timeout: int = 30,
        redaction_patterns: Optional[Iterable[str | dict]] = None,
    ):
        self.token = token
        self.username = username
        self.timeout = timeout
        self.redactor = Redactor(redaction_patterns)
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github.v3+json",
        }
        self.graphql_headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
        }

    def _get(self, path: str, params: dict) -> list[dict]:
        try:
            response = requests.get(
                f"{self.BASE_URL}{path}",
                headers=self.headers,
                params=params,
                timeout=self.timeout,
            )
            response.raise_for_status()
        except requests.exceptions.ConnectionError as e:
            raise GitHubClientError(f"Connection error: {e}") from e
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                raise GitHubAuthError(f"Authentication failed: {e}") from e
            if e.response.status_code == 403:
                raise GitHubRateLimitError(f"Rate limit exceeded: {e}") from e
            if e.response.status_code == 404:
                raise GitHubNotFoundError(f"Resource not found: {e}") from e
            raise GitHubClientError(f"HTTP error: {e}") from e
        return response.json()

    def _get_optional_dict(self, path: str, params: dict | None = None) -> dict:
        try:
            data = self._get(path, params or {})
        except (GitHubClientError, GitHubNotFoundError, GitHubRateLimitError):
            return {}
        return data if isinstance(data, dict) else {}

    def _post_graphql(self, query: str, variables: dict) -> dict:
        try:
            response = requests.post(
                self.GRAPHQL_URL,
                headers=self.graphql_headers,
                json={"query": query, "variables": variables},
                timeout=self.timeout,
            )
            response.raise_for_status()
        except requests.exceptions.ConnectionError as e:
            raise GitHubClientError(f"Connection error: {e}") from e
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                raise GitHubAuthError(f"Authentication failed: {e}") from e
            if e.response.status_code == 403:
                raise GitHubRateLimitError(f"Rate limit exceeded: {e}") from e
            if e.response.status_code == 404:
                raise GitHubNotFoundError(f"Resource not found: {e}") from e
            raise GitHubClientError(f"HTTP error: {e}") from e

        payload = response.json()
        errors = payload.get("errors") or []
        if errors:
            messages = "; ".join(error.get("message", "") for error in errors)
            if "discussions" in messages.lower() and "repository" in messages.lower():
                return {"data": {"repository": {"discussions": {"nodes": [], "pageInfo": {}}}}}
            raise GitHubClientError(f"GraphQL error: {messages}")
        return payload

    def get_configured_repos(
        self,
        repositories: Optional[list[str | dict]] = None,
        include_forks: bool = False,
    ) -> list[dict]:
        """Return repository owner/name pairs from config or owned repositories."""
        if repositories:
            return [self._normalize_repo_config(repo) for repo in repositories]

        repos = []
        page = 1
        while True:
            data = self._get(
                "/user/repos",
                {
                    "affiliation": "owner",
                    "sort": "pushed",
                    "per_page": 100,
                    "page": page,
                },
            )
            if not data:
                break
            for repo in data:
                if not include_forks and repo.get("fork"):
                    continue
                repos.append(
                    {
                        "owner": repo.get("owner", {}).get("login", self.username),
                        "name": repo["name"],
                        "repo_name": repo.get("full_name") or repo["name"],
                    }
                )
            page += 1
        return repos

    def _normalize_repo_config(self, repo: str | dict) -> dict:
        if isinstance(repo, str):
            if "/" in repo:
                owner, name = repo.split("/", 1)
                return {"owner": owner, "name": name, "repo_name": repo}
            return {"owner": self.username, "name": repo, "repo_name": repo}

        owner = repo.get("owner") or self.username
        name = repo.get("name") or repo.get("repo") or repo.get("repository")
        if not name:
            raise ValueError("GitHub repository config requires a name")
        repo_name = repo.get("repo_name") or repo.get("full_name") or (
            f"{owner}/{name}" if owner != self.username else name
        )
        return {"owner": owner, "name": name, "repo_name": repo_name}

    def get_repo_issues(
        self,
        owner: str,
        repo: str,
        repo_name: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 100,
    ) -> Iterator[GitHubActivity]:
        """Yield recently updated issues, excluding pull requests."""
        yielded = 0
        page = 1
        while yielded < limit:
            params = {
                "state": "all",
                "sort": "updated",
                "direction": "desc",
                "per_page": min(100, limit - yielded),
                "page": page,
            }
            if since:
                params["since"] = since.isoformat()
            data = self._get(f"/repos/{owner}/{repo}/issues", params)
            if not data:
                break
            for item in data:
                if "pull_request" in item:
                    continue
                yield self._issue_to_activity(item, repo_name or repo)
                yielded += 1
                if yielded >= limit:
                    break
            page += 1

    def get_repo_pull_requests(
        self,
        owner: str,
        repo: str,
        repo_name: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 100,
    ) -> Iterator[GitHubActivity]:
        """Yield recently updated pull requests."""
        yielded = 0
        page = 1
        while yielded < limit:
            data = self._get(
                f"/repos/{owner}/{repo}/pulls",
                {
                    "state": "all",
                    "sort": "updated",
                    "direction": "desc",
                    "per_page": min(100, limit - yielded),
                    "page": page,
                },
            )
            if not data:
                break
            for item in data:
                updated_at = _parse_github_datetime(item.get("updated_at"))
                if since and updated_at and updated_at < since:
                    return
                if "changed_files" not in item:
                    detail = self._get_optional_dict(
                        f"/repos/{owner}/{repo}/pulls/{item.get('number')}"
                    )
                    if detail:
                        item = {**item, **detail}
                yield self._pull_request_to_activity(item, repo_name or repo)
                yielded += 1
                if yielded >= limit:
                    break
            page += 1

    def get_repo_releases(
        self,
        owner: str,
        repo: str,
        repo_name: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 100,
    ) -> Iterator[GitHubActivity]:
        """Yield recently published repository releases."""
        yielded = 0
        page = 1
        while yielded < limit:
            data = self._get(
                f"/repos/{owner}/{repo}/releases",
                {
                    "per_page": min(100, limit - yielded),
                    "page": page,
                },
            )
            if not data:
                break
            for item in data:
                activity_at = _parse_github_datetime(
                    item.get("published_at") or item.get("created_at")
                )
                if since and activity_at and activity_at < since:
                    return
                yield self._release_to_activity(item, repo_name or repo)
                yielded += 1
                if yielded >= limit:
                    break
            page += 1

    def get_repo_discussions(
        self,
        owner: str,
        repo: str,
        repo_name: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 100,
    ) -> Iterator[GitHubActivity]:
        """Yield recently updated repository discussions via GitHub GraphQL."""
        query = """
        query RecentDiscussions($owner: String!, $name: String!, $first: Int!, $after: String) {
          repository(owner: $owner, name: $name) {
            discussions(first: $first, after: $after, orderBy: {field: UPDATED_AT, direction: DESC}) {
              nodes {
                number
                title
                bodyText
                url
                createdAt
                updatedAt
                answerChosenAt
                answerChosenBy { login }
                author { login }
                category { name slug emoji }
                comments { totalCount }
                answer {
                  url
                  bodyText
                  author { login }
                  createdAt
                  updatedAt
                }
              }
              pageInfo {
                hasNextPage
                endCursor
              }
            }
          }
        }
        """
        yielded = 0
        cursor = None
        while yielded < limit:
            payload = self._post_graphql(
                query,
                {
                    "owner": owner,
                    "name": repo,
                    "first": min(100, limit - yielded),
                    "after": cursor,
                },
            )
            repository = (payload.get("data") or {}).get("repository") or {}
            discussions = repository.get("discussions") or {}
            nodes = discussions.get("nodes") or []
            if not nodes:
                break

            for item in nodes:
                updated_at = _parse_github_datetime(item.get("updatedAt"))
                if since and updated_at and updated_at < since:
                    return
                yield self._discussion_to_activity(item, repo_name or repo)
                yielded += 1
                if yielded >= limit:
                    break

            page_info = discussions.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")

    def get_all_recent_activity(
        self,
        since: Optional[datetime] = None,
        repositories: Optional[list[str | dict]] = None,
        include_forks: bool = False,
        limit_per_repo: int = 100,
        include_discussions: bool = False,
        include_pull_requests: bool = False,
    ) -> Iterator[GitHubActivity]:
        """Yield recent issues, releases, and optionally pull requests/discussions."""
        for repo in self.get_configured_repos(repositories, include_forks=include_forks):
            try:
                yield from self.get_repo_issues(
                    repo["owner"],
                    repo["name"],
                    repo_name=repo["repo_name"],
                    since=since,
                    limit=limit_per_repo,
                )
                if include_pull_requests:
                    yield from self.get_repo_pull_requests(
                        repo["owner"],
                        repo["name"],
                        repo_name=repo["repo_name"],
                        since=since,
                        limit=limit_per_repo,
                    )
                yield from self.get_repo_releases(
                    repo["owner"],
                    repo["name"],
                    repo_name=repo["repo_name"],
                    since=since,
                    limit=limit_per_repo,
                )
                if include_discussions:
                    yield from self.get_repo_discussions(
                        repo["owner"],
                        repo["name"],
                        repo_name=repo["repo_name"],
                        since=since,
                        limit=limit_per_repo,
                    )
            except (GitHubRateLimitError, GitHubNotFoundError):
                pass

    def _issue_to_activity(self, item: dict, repo_name: str) -> GitHubActivity:
        return GitHubActivity(
            repo_name=repo_name,
            activity_type="issue",
            number=item["number"],
            title=self.redactor.redact(item.get("title", "")),
            state=item.get("state", ""),
            author=(item.get("user") or {}).get("login", ""),
            url=item.get("html_url", ""),
            updated_at=_parse_github_datetime(item.get("updated_at")),
            created_at=_parse_github_datetime(item.get("created_at")),
            body=self.redactor.redact(item.get("body") or ""),
            closed_at=_parse_github_datetime(item.get("closed_at")),
            labels=[label.get("name", "") for label in item.get("labels", [])],
        )

    def _release_to_activity(self, item: dict, repo_name: str) -> GitHubActivity:
        tag_name = item.get("tag_name") or ""
        name = item.get("name") or tag_name
        published_at = _parse_github_datetime(item.get("published_at"))
        created_at = _parse_github_datetime(item.get("created_at"))
        if item.get("draft"):
            state = "draft"
        elif item.get("prerelease"):
            state = "prerelease"
        else:
            state = "published"

        metadata = {
            "release_id": item.get("id"),
            "tag_name": tag_name,
            "target_commitish": item.get("target_commitish"),
            "published_at": published_at.isoformat() if published_at else None,
            "created_at": created_at.isoformat() if created_at else None,
            "draft": bool(item.get("draft", False)),
            "prerelease": bool(item.get("prerelease", False)),
        }

        return GitHubActivity(
            repo_name=repo_name,
            activity_type="release",
            number=item["id"],
            title=self.redactor.redact(name),
            state=state,
            author=(item.get("author") or {}).get("login", ""),
            url=item.get("html_url", ""),
            updated_at=published_at or created_at,
            created_at=created_at,
            body=self.redactor.redact(item.get("body") or ""),
            metadata=metadata,
        )

    def _pull_request_to_activity(self, item: dict, repo_name: str) -> GitHubActivity:
        merged_at = _parse_github_datetime(item.get("merged_at"))
        metadata = {
            "merged": bool(item.get("merged") or merged_at),
            "changed_files": item.get("changed_files"),
            "additions": item.get("additions"),
            "deletions": item.get("deletions"),
            "commits": item.get("commits"),
            "draft": bool(item.get("draft", False)),
            "base": ((item.get("base") or {}).get("ref")),
            "head": ((item.get("head") or {}).get("ref")),
        }
        return GitHubActivity(
            repo_name=repo_name,
            activity_type="pull_request",
            number=item["number"],
            title=self.redactor.redact(item.get("title", "")),
            state=item.get("state", ""),
            author=(item.get("user") or {}).get("login", ""),
            url=item.get("html_url", ""),
            updated_at=_parse_github_datetime(item.get("updated_at")),
            created_at=_parse_github_datetime(item.get("created_at")),
            body=_body_excerpt(self.redactor.redact(item.get("body") or "")),
            closed_at=_parse_github_datetime(item.get("closed_at")),
            merged_at=merged_at,
            labels=[label.get("name", "") for label in item.get("labels", [])],
            metadata={key: value for key, value in metadata.items() if value is not None},
        )

    def _discussion_to_activity(self, item: dict, repo_name: str) -> GitHubActivity:
        category = item.get("category") or {}
        answer = item.get("answer") or {}
        answer_chosen_at = _parse_github_datetime(item.get("answerChosenAt"))
        answer_created_at = _parse_github_datetime(answer.get("createdAt"))
        answer_updated_at = _parse_github_datetime(answer.get("updatedAt"))
        metadata = {
            "category": {
                "name": category.get("name"),
                "slug": category.get("slug"),
                "emoji": category.get("emoji"),
            },
            "comments_count": (item.get("comments") or {}).get("totalCount", 0),
            "answer": {
                "chosen_at": answer_chosen_at.isoformat() if answer_chosen_at else None,
                "chosen_by": (item.get("answerChosenBy") or {}).get("login"),
                "url": answer.get("url"),
                "author": (answer.get("author") or {}).get("login"),
                "created_at": answer_created_at.isoformat() if answer_created_at else None,
                "updated_at": answer_updated_at.isoformat() if answer_updated_at else None,
                "body": self.redactor.redact(answer.get("bodyText") or ""),
            },
        }

        return GitHubActivity(
            repo_name=repo_name,
            activity_type="discussion",
            number=item["number"],
            title=self.redactor.redact(item.get("title", "")),
            state="answered" if answer_chosen_at else "open",
            author=(item.get("author") or {}).get("login", ""),
            url=item.get("url", ""),
            updated_at=_parse_github_datetime(item.get("updatedAt")),
            created_at=_parse_github_datetime(item.get("createdAt")),
            body=self.redactor.redact(item.get("bodyText") or ""),
            metadata=metadata,
        )


def poll_new_activity(
    token: str,
    username: str,
    since: datetime,
    db,
    repositories: Optional[list[str | dict]] = None,
    include_discussions: bool = False,
    include_pull_requests: bool = False,
    dry_run: bool = False,
    timeout: int = 30,
    redaction_patterns: Optional[Iterable[str | dict]] = None,
) -> list[GitHubActivity]:
    """Poll for recently updated GitHub issues/PRs and optionally persist them."""
    client = GitHubActivityClient(
        token,
        username,
        timeout=timeout,
        redaction_patterns=redaction_patterns,
    )
    new_activity = []

    for activity in client.get_all_recent_activity(
        since=since,
        repositories=repositories,
        include_discussions=include_discussions,
        include_pull_requests=include_pull_requests,
    ):
        if db.is_github_activity_processed(
            activity.repo_name,
            activity.activity_type,
            activity.number,
            activity.updated_at.isoformat(),
        ):
            continue

        if not dry_run:
            db.upsert_github_activity(**activity.to_dict())
        new_activity.append(activity)

    return new_activity
