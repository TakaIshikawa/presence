"""Fetch GitHub repository releases as synthesis evidence."""

from __future__ import annotations

from dataclasses import dataclass
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


@dataclass
class GitHubRelease:
    repo_name: str
    tag: str
    title: str
    body_excerpt: str
    url: str
    published_at: datetime
    author: str = ""
    state: str = "published"
    created_at: Optional[datetime] = None
    release_id: int | None = None
    target_commitish: str | None = None
    draft: bool = False
    prerelease: bool = False

    @property
    def activity_id(self) -> str:
        return f"{self.repo_name}#{self.tag}:release"

    def to_activity_dict(self) -> dict:
        return {
            "repo_name": self.repo_name,
            "activity_type": "release",
            "number": self.tag,
            "title": self.title,
            "body": self.body_excerpt,
            "state": self.state,
            "author": self.author,
            "url": self.url,
            "updated_at": self.published_at.isoformat(),
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "labels": [],
            "metadata": {
                "release_id": self.release_id,
                "tag_name": self.tag,
                "target_commitish": self.target_commitish,
                "published_at": self.published_at.isoformat(),
                "created_at": self.created_at.isoformat() if self.created_at else None,
                "draft": self.draft,
                "prerelease": self.prerelease,
                "activity_id": self.activity_id,
            },
        }

    def to_dict(self) -> dict:
        return {
            "repo_name": self.repo_name,
            "tag": self.tag,
            "title": self.title,
            "body_excerpt": self.body_excerpt,
            "url": self.url,
            "published_at": self.published_at.isoformat(),
            "author": self.author,
            "state": self.state,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "release_id": self.release_id,
            "target_commitish": self.target_commitish,
            "draft": self.draft,
            "prerelease": self.prerelease,
            "activity_id": self.activity_id,
        }


def _parse_github_datetime(value: str | None) -> Optional[datetime]:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class GitHubReleaseClient:
    BASE_URL = "https://api.github.com"

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
        data = response.json()
        return data if isinstance(data, list) else []

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

    def get_repo_releases(
        self,
        owner: str,
        repo: str,
        repo_name: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 100,
    ) -> Iterator[GitHubRelease]:
        yielded = 0
        page = 1
        while yielded < limit:
            per_page = min(100, limit - yielded)
            data = self._get(
                f"/repos/{owner}/{repo}/releases",
                {
                    "per_page": per_page,
                    "page": page,
                },
            )
            if not data:
                break
            for item in data:
                published_at = _parse_github_datetime(item.get("published_at"))
                if not published_at:
                    continue
                if since and published_at < since:
                    return
                yield self._release_from_payload(item, repo_name or repo)
                yielded += 1
                if yielded >= limit:
                    break
            if len(data) < per_page:
                break
            page += 1

    def get_all_recent_releases(
        self,
        since: Optional[datetime] = None,
        repositories: Optional[list[str | dict]] = None,
        include_forks: bool = False,
        limit_per_repo: int = 100,
    ) -> Iterator[GitHubRelease]:
        for repo in self.get_configured_repos(repositories, include_forks=include_forks):
            try:
                yield from self.get_repo_releases(
                    repo["owner"],
                    repo["name"],
                    repo_name=repo["repo_name"],
                    since=since,
                    limit=limit_per_repo,
                )
            except (GitHubRateLimitError, GitHubNotFoundError):
                pass

    def _release_from_payload(self, item: dict, repo_name: str) -> GitHubRelease:
        tag = item.get("tag_name") or ""
        name = item.get("name") or tag
        published_at = _parse_github_datetime(item.get("published_at"))
        if not published_at:
            raise ValueError("GitHub release payload is missing published_at")
        created_at = _parse_github_datetime(item.get("created_at"))
        draft = bool(item.get("draft", False))
        prerelease = bool(item.get("prerelease", False))
        if draft:
            state = "draft"
        elif prerelease:
            state = "prerelease"
        else:
            state = "published"

        return GitHubRelease(
            repo_name=repo_name,
            tag=tag,
            title=self.redactor.redact(name),
            body_excerpt=_body_excerpt(
                self.redactor.redact(item.get("body") or ""),
                max_len=BODY_EXCERPT_MAX_CHARS,
            ),
            url=item.get("html_url", ""),
            published_at=published_at,
            author=(item.get("author") or {}).get("login", ""),
            state=state,
            created_at=created_at,
            release_id=item.get("id"),
            target_commitish=item.get("target_commitish"),
            draft=draft,
            prerelease=prerelease,
        )


def poll_new_releases(
    token: str,
    username: str,
    since: datetime,
    db,
    repositories: Optional[list[str | dict]] = None,
    dry_run: bool = False,
    timeout: int = 30,
    redaction_patterns: Optional[Iterable[str | dict]] = None,
) -> list[GitHubRelease]:
    """Poll for newly published GitHub releases and optionally persist them."""
    client = GitHubReleaseClient(
        token,
        username,
        timeout=timeout,
        redaction_patterns=redaction_patterns,
    )
    new_releases = []

    for release in client.get_all_recent_releases(
        since=since,
        repositories=repositories,
    ):
        if db.is_github_release_processed(
            release.repo_name,
            release.tag,
            release.published_at.isoformat(),
        ):
            continue

        if not dry_run:
            db.upsert_github_release(release.to_activity_dict())
        new_releases.append(release)

    return new_releases
