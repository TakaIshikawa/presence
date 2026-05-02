"""Ingest GitHub Dependabot alerts into github_activity."""

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

ACTIVITY_TYPE = "dependabot_alert"
DEFAULT_STATE = "open"


@dataclass
class GitHubDependabotAlert:
    repo_name: str
    number: int
    package: str
    ecosystem: str
    severity: str
    state: str
    created_at: datetime
    url: str = ""
    ghsa_id: str = ""
    cve_id: str = ""
    fixed_at: datetime | None = None
    dismissed_at: datetime | None = None
    updated_at: datetime | None = None
    title: str = ""
    author: str = "dependabot"
    metadata: dict = field(default_factory=dict)

    @property
    def external_id(self) -> str:
        return f"dependabot_alert:{self.repo_name}:{self.number}"

    @property
    def activity_id(self) -> str:
        return f"{self.repo_name}#{self.number}:{ACTIVITY_TYPE}"

    def to_activity_dict(self) -> dict:
        updated_at = self.updated_at or self.fixed_at or self.dismissed_at or self.created_at
        title = self.title or _alert_title(self.package, self.ecosystem, self.severity)
        metadata = {
            **self.metadata,
            "activity_id": self.activity_id,
            "external_id": self.external_id,
            "alert_number": self.number,
            "package": self.package,
            "ecosystem": self.ecosystem,
            "severity": self.severity,
            "state": self.state,
            "ghsa_id": self.ghsa_id,
            "cve_id": self.cve_id,
            "created_at": self.created_at.isoformat(),
            "fixed_at": self.fixed_at.isoformat() if self.fixed_at else None,
            "dismissed_at": self.dismissed_at.isoformat() if self.dismissed_at else None,
            "updated_at": updated_at.isoformat(),
            "url": self.url,
        }
        return {
            "repo_name": self.repo_name,
            "activity_type": ACTIVITY_TYPE,
            "number": self.number,
            "title": title,
            "body": _body_excerpt(self.metadata.get("summary") or "", BODY_EXCERPT_MAX_CHARS),
            "state": self.state,
            "author": self.author,
            "url": self.url,
            "updated_at": updated_at.isoformat(),
            "created_at": self.created_at.isoformat(),
            "closed_at": (self.fixed_at or self.dismissed_at).isoformat()
            if self.fixed_at or self.dismissed_at
            else None,
            "labels": [label for label in (self.ecosystem, self.severity, self.state) if label],
            "metadata": {
                key: value for key, value in metadata.items() if value not in (None, "", [])
            },
        }

    def to_dict(self) -> dict:
        return {
            "repo_name": self.repo_name,
            "number": self.number,
            "package": self.package,
            "ecosystem": self.ecosystem,
            "severity": self.severity,
            "state": self.state,
            "ghsa_id": self.ghsa_id,
            "cve_id": self.cve_id,
            "created_at": self.created_at.isoformat(),
            "fixed_at": self.fixed_at.isoformat() if self.fixed_at else None,
            "dismissed_at": self.dismissed_at.isoformat() if self.dismissed_at else None,
            "updated_at": (self.updated_at or self.fixed_at or self.dismissed_at or self.created_at).isoformat(),
            "url": self.url,
            "title": self.title,
            "metadata": self.metadata,
            "external_id": self.external_id,
            "activity_id": self.activity_id,
        }


def _parse_github_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _alert_title(package: str, ecosystem: str, severity: str) -> str:
    subject = package or "dependency"
    if ecosystem:
        subject = f"{subject} ({ecosystem})"
    if severity:
        return f"Dependabot {severity} alert for {subject}"
    return f"Dependabot alert for {subject}"


def _first_identifier(advisory: dict, identifier_type: str) -> str:
    for identifier in advisory.get("identifiers") or []:
        if str(identifier.get("type") or "").upper() == identifier_type.upper():
            return str(identifier.get("value") or "")
    return ""


def _alert_updated_at(created_at: datetime, fixed_at: datetime | None, dismissed_at: datetime | None) -> datetime:
    candidates = [created_at]
    if fixed_at:
        candidates.append(fixed_at)
    if dismissed_at:
        candidates.append(dismissed_at)
    return max(candidates)


def normalize_dependabot_alert_payload(
    payload: dict,
    repo_name: str,
    redactor: Redactor | None = None,
) -> GitHubDependabotAlert:
    """Normalize a Dependabot alert payload into deterministic github_activity fields."""
    number = payload.get("number")
    if number is None:
        raise ValueError("GitHub Dependabot alert payload is missing number")

    created_at = _parse_github_datetime(payload.get("created_at"))
    if not created_at:
        raise ValueError("GitHub Dependabot alert payload is missing created_at")

    dependency = payload.get("dependency") or {}
    package_info = dependency.get("package") or {}
    security_advisory = payload.get("security_advisory") or {}
    security_vulnerability = payload.get("security_vulnerability") or {}
    vulnerability_package = security_vulnerability.get("package") or {}
    redactor = redactor or Redactor()

    package_name = str(package_info.get("name") or vulnerability_package.get("name") or "")
    ecosystem = str(package_info.get("ecosystem") or vulnerability_package.get("ecosystem") or "")
    severity = str(
        security_advisory.get("severity") or security_vulnerability.get("severity") or ""
    ).lower()
    state = str(payload.get("state") or DEFAULT_STATE).lower()
    fixed_at = _parse_github_datetime(payload.get("fixed_at"))
    dismissed_at = _parse_github_datetime(payload.get("dismissed_at"))
    updated_at = _alert_updated_at(created_at, fixed_at, dismissed_at)
    ghsa_id = str(security_advisory.get("ghsa_id") or _first_identifier(security_advisory, "GHSA"))
    cve_id = str(security_advisory.get("cve_id") or _first_identifier(security_advisory, "CVE"))
    summary = redactor.redact(security_advisory.get("summary") or "")

    metadata = {
        "dependency_scope": dependency.get("scope"),
        "manifest_path": dependency.get("manifest_path"),
        "vulnerable_requirements": dependency.get("vulnerable_requirements"),
        "advisory_url": security_advisory.get("url"),
        "advisory_summary": summary,
        "summary": summary,
        "cvss": security_advisory.get("cvss") or {},
        "cwes": security_advisory.get("cwes") or [],
        "vulnerable_version_range": security_vulnerability.get("vulnerable_version_range"),
        "patched_versions": security_vulnerability.get("patched_versions"),
        "dismissed_reason": payload.get("dismissed_reason"),
        "dismissed_comment": redactor.redact(payload.get("dismissed_comment") or ""),
        "html_url": payload.get("html_url"),
    }

    return GitHubDependabotAlert(
        repo_name=repo_name,
        number=int(number),
        package=redactor.redact(package_name),
        ecosystem=ecosystem,
        severity=severity,
        state=state,
        created_at=created_at,
        url=payload.get("html_url") or payload.get("url") or "",
        ghsa_id=ghsa_id,
        cve_id=cve_id,
        fixed_at=fixed_at,
        dismissed_at=dismissed_at,
        updated_at=updated_at,
        title=redactor.redact(_alert_title(package_name, ecosystem, severity)),
        metadata={key: value for key, value in metadata.items() if value not in (None, "", [])},
    )


class GitHubDependabotAlertClient:
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

    def get_repo_dependabot_alerts(
        self,
        owner: str,
        repo: str,
        repo_name: Optional[str] = None,
        state: str | None = None,
        limit: int = 100,
    ) -> Iterator[GitHubDependabotAlert]:
        yielded = 0
        page = 1
        while yielded < limit:
            per_page = min(100, limit - yielded)
            params = {
                "per_page": per_page,
                "page": page,
                "sort": "created",
                "direction": "desc",
            }
            if state:
                params["state"] = state
            data = self._get(f"/repos/{owner}/{repo}/dependabot/alerts", params)
            items = data if isinstance(data, list) else []
            if not items:
                break
            for payload in items:
                yield normalize_dependabot_alert_payload(
                    payload,
                    repo_name=repo_name or f"{owner}/{repo}",
                    redactor=self.redactor,
                )
                yielded += 1
                if yielded >= limit:
                    break
            if len(items) < per_page:
                break
            page += 1

    def get_all_dependabot_alerts(
        self,
        repositories: Optional[list[str | dict]] = None,
        state: str | None = None,
        include_forks: bool = False,
        limit_per_repo: int = 100,
    ) -> Iterator[GitHubDependabotAlert]:
        for repo in self.get_configured_repos(repositories, include_forks=include_forks):
            try:
                yield from self.get_repo_dependabot_alerts(
                    repo["owner"],
                    repo["name"],
                    repo_name=repo["repo_name"],
                    state=state,
                    limit=limit_per_repo,
                )
            except (GitHubRateLimitError, GitHubNotFoundError):
                pass


def poll_dependabot_alerts(
    token: str,
    username: str,
    db,
    repositories: Optional[list[str | dict]] = None,
    state: str | None = None,
    dry_run: bool = False,
    limit_per_repo: int = 100,
    timeout: int = 30,
    redaction_patterns: Optional[Iterable[str | dict]] = None,
    session=None,
) -> list[GitHubDependabotAlert]:
    """Poll Dependabot alerts and optionally persist github_activity rows."""
    client = GitHubDependabotAlertClient(
        token,
        username,
        timeout=timeout,
        redaction_patterns=redaction_patterns,
        db=db,
        session=session,
    )
    alerts = []
    seen: set[str] = set()

    for alert in client.get_all_dependabot_alerts(
        repositories=repositories,
        state=state,
        limit_per_repo=limit_per_repo,
    ):
        if alert.external_id in seen:
            continue
        seen.add(alert.external_id)
        activity = alert.to_activity_dict()

        if db.is_github_activity_processed(
            alert.repo_name,
            ACTIVITY_TYPE,
            alert.number,
            activity["updated_at"],
        ):
            continue

        if not dry_run:
            db.upsert_github_activity(**activity)
        alerts.append(alert)

    return alerts
