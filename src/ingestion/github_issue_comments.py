"""Fetch GitHub issue comments as content signals."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable, Iterator, Optional

import requests

from ingestion.github_activity import BODY_EXCERPT_MAX_CHARS, _body_excerpt, _number_from_url
from ingestion.github_commits import (
    GitHubAuthError,
    GitHubClientError,
    GitHubNotFoundError,
    GitHubRateLimitError,
)
from ingestion.redaction import Redactor
from output.api_rate_guard import record_snapshot

ACTIVITY_TYPE = "github_issue_comment"


@dataclass
class GitHubIssueComment:
    repo: str
    issue_number: int | None
    comment_id: int
    author: str
    body: str
    url: str
    created_at: datetime | None
    updated_at: datetime
    source_type: str = ACTIVITY_TYPE
    metadata: dict = field(default_factory=dict)

    @property
    def activity_id(self) -> str:
        return f"{self.repo}#{self.comment_id}:{self.source_type}"

    def to_dict(self) -> dict:
        return {
            "source_type": self.source_type,
            "repo": self.repo,
            "issue_number": self.issue_number,
            "comment_id": self.comment_id,
            "author": self.author,
            "body": self.body,
            "url": self.url,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat(),
            "metadata": self.metadata,
        }

    def to_activity_dict(self) -> dict:
        title = "Issue comment"
        if self.issue_number is not None:
            title = f"{title} on #{self.issue_number}"

        metadata = {
            **self.metadata,
            "activity_id": self.activity_id,
            "source_type": self.source_type,
            "comment_id": self.comment_id,
            "issue_number": self.issue_number,
            "parent_issue_number": self.issue_number,
            "parent_number": self.issue_number,
            "parent_type": "issue",
        }
        return {
            "repo_name": self.repo,
            "activity_type": self.source_type,
            "number": self.comment_id,
            "title": title,
            "body": self.body,
            "state": "commented",
            "author": self.author,
            "url": self.url,
            "updated_at": self.updated_at.isoformat(),
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "labels": [],
            "metadata": {key: value for key, value in metadata.items() if value is not None},
        }


def _parse_github_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def normalize_issue_comment_payload(
    payload: dict,
    repo: str,
    redactor: Redactor | None = None,
) -> dict:
    """Normalize a GitHub issue comment payload into a deterministic dictionary."""
    redactor = redactor or Redactor()
    updated_at = _parse_github_datetime(payload.get("updated_at")) or _parse_github_datetime(
        payload.get("created_at")
    )
    if not updated_at:
        raise ValueError("GitHub issue comment payload is missing updated_at/created_at")

    comment_id = payload.get("id")
    if comment_id is None:
        raise ValueError("GitHub issue comment payload is missing id")

    metadata = {
        "issue_url": payload.get("issue_url"),
        "node_id": payload.get("node_id"),
    }
    return {
        "source_type": ACTIVITY_TYPE,
        "repo": repo,
        "issue_number": _number_from_url(payload.get("issue_url")),
        "comment_id": int(comment_id),
        "author": (payload.get("user") or {}).get("login", ""),
        "body": _body_excerpt(
            redactor.redact(payload.get("body") or ""),
            max_len=BODY_EXCERPT_MAX_CHARS,
        ),
        "url": payload.get("html_url") or payload.get("url") or "",
        "created_at": _parse_github_datetime(payload.get("created_at")),
        "updated_at": updated_at,
        "metadata": {key: value for key, value in metadata.items() if value is not None},
    }


class GitHubIssueCommentClient:
    BASE_URL = "https://api.github.com"

    def __init__(
        self,
        token: str,
        username: str,
        timeout: int = 30,
        redaction_patterns: Optional[Iterable[str | dict]] = None,
        db=None,
        session=None,
    ):
        self.token = token
        self.username = username
        self.timeout = timeout
        self.db = db
        self.session = session or requests
        self.redactor = Redactor(redaction_patterns)
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github.v3+json",
        }

    def _get(self, path: str, params: dict) -> list[dict]:
        try:
            response = self.session.get(
                f"{self.BASE_URL}{path}",
                headers=self.headers,
                params=params,
                timeout=self.timeout,
            )
            response.raise_for_status()
            self._record_rate_limit(response, endpoint=path)
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
        data = response.json()
        return data if isinstance(data, list) else []

    def _record_rate_limit(self, response, endpoint: str) -> None:
        try:
            record_snapshot(
                self.db,
                "github",
                headers=getattr(response, "headers", None),
                endpoint=endpoint,
            )
        except Exception:
            pass

    def get_configured_repos(
        self,
        repositories: Optional[list[str | dict]] = None,
        include_forks: bool = False,
    ) -> list[dict]:
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

    def get_repo_issue_comments(
        self,
        owner: str,
        repo: str,
        repo_name: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 100,
    ) -> Iterator[GitHubIssueComment]:
        yielded = 0
        page = 1
        while yielded < limit:
            per_page = min(100, limit - yielded)
            params = {
                "sort": "updated",
                "direction": "desc",
                "per_page": per_page,
                "page": page,
            }
            if since:
                params["since"] = since.isoformat()
            data = self._get(f"/repos/{owner}/{repo}/issues/comments", params)
            if not data:
                break
            for payload in data:
                updated_at = _parse_github_datetime(payload.get("updated_at")) or _parse_github_datetime(
                    payload.get("created_at")
                )
                if since and updated_at and updated_at < since:
                    return
                yield GitHubIssueComment(
                    **normalize_issue_comment_payload(
                        payload,
                        repo=repo_name or repo,
                        redactor=self.redactor,
                    )
                )
                yielded += 1
                if yielded >= limit:
                    break
            if len(data) < per_page:
                break
            page += 1

    def get_all_recent_issue_comments(
        self,
        since: Optional[datetime] = None,
        repositories: Optional[list[str | dict]] = None,
        include_forks: bool = False,
        limit_per_repo: int = 100,
    ) -> Iterator[GitHubIssueComment]:
        for repo in self.get_configured_repos(repositories, include_forks=include_forks):
            try:
                yield from self.get_repo_issue_comments(
                    repo["owner"],
                    repo["name"],
                    repo_name=repo["repo_name"],
                    since=since,
                    limit=limit_per_repo,
                )
            except (GitHubRateLimitError, GitHubNotFoundError):
                pass


def poll_new_issue_comments(
    token: str,
    username: str,
    since: datetime,
    db,
    repositories: Optional[list[str | dict]] = None,
    dry_run: bool = False,
    limit_per_repo: int = 100,
    timeout: int = 30,
    redaction_patterns: Optional[Iterable[str | dict]] = None,
    session=None,
) -> list[GitHubIssueComment]:
    """Poll for new or updated issue comments and optionally persist them."""
    client = GitHubIssueCommentClient(
        token,
        username,
        timeout=timeout,
        redaction_patterns=redaction_patterns,
        db=db,
        session=session,
    )
    comments = []
    seen: set[tuple[str, int]] = set()

    for comment in client.get_all_recent_issue_comments(
        since=since,
        repositories=repositories,
        limit_per_repo=limit_per_repo,
    ):
        identity = (comment.repo, comment.comment_id)
        if identity in seen:
            continue
        seen.add(identity)

        if db.is_github_activity_processed(
            comment.repo,
            comment.source_type,
            comment.comment_id,
            comment.updated_at.isoformat(),
        ):
            continue

        if not dry_run:
            db.upsert_github_activity(**comment.to_activity_dict())
        comments.append(comment)

    return comments
