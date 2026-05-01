"""Fetch GitHub Actions workflow runs as content and operations signals."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable, Iterator, Optional

import requests

from ingestion.github_activity import BODY_EXCERPT_MAX_CHARS, _body_excerpt
from ingestion.github_commits import (
    GitHubAuthError,
    GitHubClientError,
    GitHubNotFoundError,
    GitHubRateLimitError,
)
from ingestion.redaction import Redactor
from output.api_rate_guard import record_snapshot

ACTIVITY_TYPE = "workflow_run"


@dataclass
class GitHubWorkflowRun:
    repo: str
    run_id: int
    workflow_name: str
    run_number: int | None
    status: str
    conclusion: str | None
    event: str
    branch: str
    head_sha: str
    actor: str
    url: str
    created_at: datetime | None
    updated_at: datetime
    run_started_at: datetime | None = None
    display_title: str = ""
    body: str = ""
    metadata: dict = field(default_factory=dict)

    @property
    def activity_id(self) -> str:
        return f"{self.repo}#{self.run_id}:{ACTIVITY_TYPE}"

    @property
    def state(self) -> str:
        return self.conclusion or self.status or "unknown"

    def to_dict(self) -> dict:
        return {
            "repo": self.repo,
            "run_id": self.run_id,
            "workflow_name": self.workflow_name,
            "run_number": self.run_number,
            "status": self.status,
            "conclusion": self.conclusion,
            "event": self.event,
            "branch": self.branch,
            "head_sha": self.head_sha,
            "actor": self.actor,
            "url": self.url,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat(),
            "run_started_at": self.run_started_at.isoformat() if self.run_started_at else None,
            "display_title": self.display_title,
            "body": self.body,
            "metadata": self.metadata,
        }

    def to_activity_dict(self) -> dict:
        title = self.workflow_name or "GitHub Actions workflow"
        if self.run_number is not None:
            title = f"{title} #{self.run_number}"
        if self.conclusion or self.status:
            title = f"{title}: {self.state}"

        metadata = {
            **self.metadata,
            "activity_id": self.activity_id,
            "run_id": self.run_id,
            "workflow_name": self.workflow_name,
            "run_number": self.run_number,
            "status": self.status,
            "conclusion": self.conclusion,
            "event": self.event,
            "head_branch": self.branch,
            "branch": self.branch,
            "head_sha": self.head_sha,
            "html_url": self.url,
            "url": self.url,
            "run_started_at": self.run_started_at.isoformat() if self.run_started_at else None,
            "display_title": self.display_title,
        }
        return {
            "repo_name": self.repo,
            "activity_type": ACTIVITY_TYPE,
            "number": self.run_id,
            "title": title,
            "body": self.body,
            "state": self.state,
            "author": self.actor,
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


def _metadata_subset(payload: dict, keys: Iterable[str]) -> dict:
    return {key: payload.get(key) for key in keys if payload.get(key) is not None}


def normalize_workflow_run_payload(
    payload: dict,
    repo: str,
    redactor: Redactor | None = None,
) -> dict:
    """Normalize a GitHub workflow run payload into a deterministic dictionary."""
    redactor = redactor or Redactor()
    updated_at = (
        _parse_github_datetime(payload.get("updated_at"))
        or _parse_github_datetime(payload.get("run_started_at"))
        or _parse_github_datetime(payload.get("created_at"))
    )
    if not updated_at:
        raise ValueError("GitHub workflow run payload is missing updated_at/run_started_at/created_at")

    run_id = payload.get("id") or payload.get("run_id")
    if run_id is None:
        raise ValueError("GitHub workflow run payload is missing id")

    workflow_name = redactor.redact(payload.get("name") or payload.get("workflow_name") or "")
    display_title = redactor.redact(payload.get("display_title") or "")
    head_commit = payload.get("head_commit") or {}
    body = _body_excerpt(
        redactor.redact(head_commit.get("message") or display_title),
        max_len=BODY_EXCERPT_MAX_CHARS,
    )
    metadata = _metadata_subset(
        payload,
        (
            "check_suite_id",
            "check_suite_node_id",
            "head_repository_id",
            "head_repository",
            "head_repository_owner",
            "node_id",
            "path",
            "previous_attempt_url",
            "referenced_workflows",
            "run_attempt",
            "run_number",
            "triggering_actor",
            "workflow_id",
        ),
    )
    if head_commit:
        metadata["head_commit"] = {
            key: value
            for key, value in {
                "id": head_commit.get("id"),
                "tree_id": head_commit.get("tree_id"),
                "message": redactor.redact(head_commit.get("message") or ""),
                "timestamp": head_commit.get("timestamp"),
                "author": head_commit.get("author"),
                "committer": head_commit.get("committer"),
            }.items()
            if value not in (None, "")
        }

    return {
        "repo": repo,
        "run_id": int(run_id),
        "workflow_name": workflow_name,
        "run_number": payload.get("run_number"),
        "status": payload.get("status") or "",
        "conclusion": payload.get("conclusion"),
        "event": payload.get("event") or "",
        "branch": payload.get("head_branch") or "",
        "head_sha": payload.get("head_sha") or "",
        "actor": (payload.get("actor") or {}).get("login", ""),
        "url": payload.get("html_url") or payload.get("url") or "",
        "created_at": _parse_github_datetime(payload.get("created_at")),
        "updated_at": updated_at,
        "run_started_at": _parse_github_datetime(payload.get("run_started_at")),
        "display_title": display_title,
        "body": body,
        "metadata": {key: value for key, value in metadata.items() if value is not None},
    }


class GitHubWorkflowRunClient:
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

    def _get(self, path: str, params: dict):
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
        return response.json()

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
            if not isinstance(data, list) or not data:
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

    def get_repo_workflow_runs(
        self,
        owner: str,
        repo: str,
        repo_name: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 100,
    ) -> Iterator[GitHubWorkflowRun]:
        yielded = 0
        page = 1
        while yielded < limit:
            per_page = min(100, limit - yielded)
            params = {
                "per_page": per_page,
                "page": page,
            }
            if since:
                params["created"] = f">={since.isoformat()}"
            data = self._get(f"/repos/{owner}/{repo}/actions/runs", params)
            runs = data.get("workflow_runs") if isinstance(data, dict) else []
            if not isinstance(runs, list) or not runs:
                break
            for payload in runs:
                updated_at = (
                    _parse_github_datetime(payload.get("updated_at"))
                    or _parse_github_datetime(payload.get("run_started_at"))
                    or _parse_github_datetime(payload.get("created_at"))
                )
                if since and updated_at and updated_at < since:
                    return
                yield GitHubWorkflowRun(
                    **normalize_workflow_run_payload(
                        payload,
                        repo=repo_name or repo,
                        redactor=self.redactor,
                    )
                )
                yielded += 1
                if yielded >= limit:
                    break
            if len(runs) < per_page:
                break
            page += 1

    def get_all_recent_workflow_runs(
        self,
        since: Optional[datetime] = None,
        repositories: Optional[list[str | dict]] = None,
        include_forks: bool = False,
        limit_per_repo: int = 100,
    ) -> Iterator[GitHubWorkflowRun]:
        for repo in self.get_configured_repos(repositories, include_forks=include_forks):
            try:
                yield from self.get_repo_workflow_runs(
                    repo["owner"],
                    repo["name"],
                    repo_name=repo["repo_name"],
                    since=since,
                    limit=limit_per_repo,
                )
            except (GitHubRateLimitError, GitHubNotFoundError):
                pass


def poll_new_workflow_runs(
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
) -> list[GitHubWorkflowRun]:
    """Poll for new or updated workflow runs and optionally persist them."""
    client = GitHubWorkflowRunClient(
        token,
        username,
        timeout=timeout,
        redaction_patterns=redaction_patterns,
        db=db,
        session=session,
    )
    runs = []
    seen: set[tuple[str, int]] = set()

    for run in client.get_all_recent_workflow_runs(
        since=since,
        repositories=repositories,
        limit_per_repo=limit_per_repo,
    ):
        identity = (run.repo, run.run_id)
        if identity in seen:
            continue
        seen.add(identity)

        if db.is_github_activity_processed(
            run.repo,
            ACTIVITY_TYPE,
            run.run_id,
            run.updated_at.isoformat(),
        ):
            continue

        if not dry_run:
            db.upsert_github_activity(**run.to_activity_dict())
        runs.append(run)

    return runs
