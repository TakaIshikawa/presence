"""Ingest GitHub repository security advisories into github_activity."""

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

ACTIVITY_TYPE = "security_advisory"


@dataclass
class GitHubSecurityAdvisory:
    repo_name: str
    advisory_number: str
    title: str
    state: str
    severity: str
    url: str
    updated_at: datetime
    published_at: datetime | None = None
    withdrawn_at: datetime | None = None
    author: str = ""
    body: str = ""
    metadata: dict = field(default_factory=dict)

    @property
    def activity_id(self) -> str:
        return f"{self.repo_name}#{self.advisory_number}:{ACTIVITY_TYPE}"

    def to_activity_dict(self) -> dict:
        metadata = {
            **self.metadata,
            "activity_id": self.activity_id,
            "ghsa_id": self.advisory_number,
            "severity": self.severity,
            "advisory_url": self.url,
            "published_at": self.published_at.isoformat() if self.published_at else None,
            "updated_at": self.updated_at.isoformat(),
            "withdrawn_at": self.withdrawn_at.isoformat() if self.withdrawn_at else None,
        }
        return {
            "repo_name": self.repo_name,
            "activity_type": ACTIVITY_TYPE,
            "number": self.advisory_number,
            "title": self.title,
            "body": self.body,
            "state": self.state,
            "author": self.author,
            "url": self.url,
            "updated_at": self.updated_at.isoformat(),
            "created_at": self.published_at.isoformat() if self.published_at else None,
            "closed_at": self.withdrawn_at.isoformat() if self.withdrawn_at else None,
            "labels": [],
            "metadata": {key: value for key, value in metadata.items() if value is not None},
        }

    def to_dict(self) -> dict:
        return {
            "repo_name": self.repo_name,
            "advisory_number": self.advisory_number,
            "title": self.title,
            "state": self.state,
            "severity": self.severity,
            "url": self.url,
            "updated_at": self.updated_at.isoformat(),
            "published_at": self.published_at.isoformat() if self.published_at else None,
            "withdrawn_at": self.withdrawn_at.isoformat() if self.withdrawn_at else None,
            "author": self.author,
            "body": self.body,
            "metadata": self.metadata,
            "activity_id": self.activity_id,
        }


def _parse_github_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _advisory_number(payload: dict) -> str:
    value = payload.get("ghsa_id") or payload.get("id")
    if value in (None, ""):
        raise ValueError("GitHub security advisory payload is missing ghsa_id/id")
    return str(value)


def _identifier_values(payload: dict, identifier_type: str) -> list[str]:
    values = []
    for identifier in payload.get("identifiers") or []:
        if str(identifier.get("type") or "").upper() == identifier_type.upper():
            value = identifier.get("value")
            if value:
                values.append(str(value))
    return sorted(dict.fromkeys(values))


def _affected_packages(payload: dict) -> list[dict]:
    packages = []
    for vulnerability in payload.get("vulnerabilities") or []:
        package = vulnerability.get("package") or {}
        ecosystem = package.get("ecosystem")
        name = package.get("name")
        item = {
            "ecosystem": ecosystem,
            "name": name,
            "vulnerable_version_range": vulnerability.get("vulnerable_version_range"),
            "patched_versions": vulnerability.get("patched_versions"),
            "vulnerable_functions": vulnerability.get("vulnerable_functions") or [],
        }
        packages.append({key: value for key, value in item.items() if value not in (None, "", [])})
    return sorted(packages, key=lambda item: (item.get("ecosystem", ""), item.get("name", "")))


def normalize_security_advisory_payload(
    payload: dict,
    repo_name: str,
    redactor: Redactor | None = None,
) -> GitHubSecurityAdvisory:
    """Normalize a repository security advisory payload into github_activity shape."""
    redactor = redactor or Redactor()
    updated_at = (
        _parse_github_datetime(payload.get("updated_at"))
        or _parse_github_datetime(payload.get("published_at"))
        or _parse_github_datetime(payload.get("created_at"))
    )
    if not updated_at:
        raise ValueError("GitHub security advisory payload is missing updated_at/published_at/created_at")

    advisory_number = _advisory_number(payload)
    affected_packages = _affected_packages(payload)
    ecosystems = sorted(
        dict.fromkeys(package["ecosystem"] for package in affected_packages if package.get("ecosystem"))
    )
    package_names = sorted(
        dict.fromkeys(package["name"] for package in affected_packages if package.get("name"))
    )
    cves = _identifier_values(payload, "CVE")
    ghsa_ids = _identifier_values(payload, "GHSA")
    severity = str(payload.get("severity") or "").lower()
    published_at = _parse_github_datetime(payload.get("published_at"))
    withdrawn_at = _parse_github_datetime(payload.get("withdrawn_at"))
    metadata = {
        "id": payload.get("id"),
        "node_id": payload.get("node_id"),
        "ghsa_id": payload.get("ghsa_id") or advisory_number,
        "cves": cves,
        "ghsa_ids": ghsa_ids,
        "severity": severity,
        "cvss": payload.get("cvss") or {},
        "cwe_ids": payload.get("cwe_ids") or [],
        "ecosystem": ecosystems[0] if len(ecosystems) == 1 else None,
        "ecosystems": ecosystems,
        "affected_packages": affected_packages,
        "package_names": package_names,
        "advisory_url": payload.get("html_url") or payload.get("url") or "",
        "published_at": published_at.isoformat() if published_at else None,
        "updated_at": updated_at.isoformat(),
        "withdrawn_at": withdrawn_at.isoformat() if withdrawn_at else None,
    }

    return GitHubSecurityAdvisory(
        repo_name=repo_name,
        advisory_number=advisory_number,
        title=redactor.redact(payload.get("summary") or payload.get("ghsa_id") or advisory_number),
        state=payload.get("state") or ("withdrawn" if withdrawn_at else "published"),
        severity=severity,
        url=payload.get("html_url") or payload.get("url") or "",
        updated_at=updated_at,
        published_at=published_at,
        withdrawn_at=withdrawn_at,
        author=(payload.get("publisher") or {}).get("login", ""),
        body=_body_excerpt(redactor.redact(payload.get("description") or ""), BODY_EXCERPT_MAX_CHARS),
        metadata={key: value for key, value in metadata.items() if value is not None},
    )


class GitHubSecurityAdvisoryClient:
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
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
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
        except requests.exceptions.ConnectionError as exc:
            raise GitHubClientError(f"Connection error: {exc}") from exc
        except requests.exceptions.HTTPError as exc:
            if exc.response.status_code == 401:
                raise GitHubAuthError(f"Authentication failed: {exc}") from exc
            if exc.response.status_code == 403:
                raise GitHubRateLimitError(f"Rate limit exceeded: {exc}") from exc
            if exc.response.status_code == 404:
                raise GitHubNotFoundError(f"Resource not found: {exc}") from exc
            raise GitHubClientError(f"HTTP error: {exc}") from exc
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
            items = data if isinstance(data, list) else []
            if not items:
                break
            for repo in items:
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

    def get_repo_security_advisories(
        self,
        owner: str,
        repo: str,
        repo_name: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 100,
    ) -> Iterator[GitHubSecurityAdvisory]:
        yielded = 0
        page = 1
        while yielded < limit:
            per_page = min(100, limit - yielded)
            data = self._get(
                f"/repos/{owner}/{repo}/security-advisories",
                {
                    "per_page": per_page,
                    "page": page,
                    "direction": "desc",
                    "sort": "updated",
                },
            )
            items = data if isinstance(data, list) else []
            if not items:
                break
            for payload in items:
                advisory = normalize_security_advisory_payload(
                    payload,
                    repo_name=repo_name or f"{owner}/{repo}",
                    redactor=self.redactor,
                )
                if since and advisory.updated_at < since:
                    continue
                yield advisory
                yielded += 1
                if yielded >= limit:
                    break
            if len(items) < per_page:
                break
            page += 1

    def get_all_recent_security_advisories(
        self,
        since: Optional[datetime] = None,
        repositories: Optional[list[str | dict]] = None,
        include_forks: bool = False,
        limit_per_repo: int = 100,
    ) -> Iterator[GitHubSecurityAdvisory]:
        for repo in self.get_configured_repos(repositories, include_forks=include_forks):
            try:
                yield from self.get_repo_security_advisories(
                    repo["owner"],
                    repo["name"],
                    repo_name=repo["repo_name"],
                    since=since,
                    limit=limit_per_repo,
                )
            except (GitHubRateLimitError, GitHubNotFoundError):
                pass


def poll_security_advisories(
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
) -> list[GitHubSecurityAdvisory]:
    """Poll repository security advisories and optionally persist github_activity rows."""
    client = GitHubSecurityAdvisoryClient(
        token,
        username,
        timeout=timeout,
        redaction_patterns=redaction_patterns,
        db=db,
        session=session,
    )
    advisories = []
    seen: set[tuple[str, str]] = set()

    for advisory in client.get_all_recent_security_advisories(
        since=since,
        repositories=repositories,
        limit_per_repo=limit_per_repo,
    ):
        identity = (advisory.repo_name, advisory.advisory_number)
        if identity in seen:
            continue
        seen.add(identity)

        if db.is_github_activity_processed(
            advisory.repo_name,
            ACTIVITY_TYPE,
            advisory.advisory_number,
            advisory.updated_at.isoformat(),
        ):
            continue

        if not dry_run:
            db.upsert_github_activity(**advisory.to_activity_dict())
        advisories.append(advisory)

    return advisories
