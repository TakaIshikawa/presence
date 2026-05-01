"""Seed content ideas from repeated failed/cancelled GitHub workflow runs."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
from typing import Any


SOURCE_NAME = "github_workflow_run_seed"
ELIGIBLE_CONCLUSIONS = frozenset({"failure", "cancelled"})
DEFAULT_DAYS = 14
DEFAULT_MIN_FAILURES = 2


@dataclass(frozen=True)
class WorkflowRunIdeaCandidate:
    fingerprint: str
    repo_name: str
    workflow_name: str
    branch: str
    failure_count: int
    latest_run_url: str
    latest_commit_sha: str | None
    latest_activity_id: str
    latest_updated_at: str
    conclusions: tuple[str, ...]
    run_numbers: tuple[int, ...]
    activity_ids: tuple[str, ...]
    topic: str
    note: str
    priority: str
    source_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class WorkflowRunIdeaSeedResult:
    status: str
    fingerprint: str
    repo_name: str
    workflow_name: str
    branch: str
    failure_count: int
    idea_id: int | None
    reason: str
    topic: str
    note: str
    source_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _metadata_value(row: dict[str, Any], *keys: str) -> Any:
    metadata = row.get("metadata") or {}
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
        if isinstance(metadata, dict):
            value = metadata.get(key)
            if value not in (None, ""):
                return value
    return None


def _workflow_name(row: dict[str, Any]) -> str:
    return str(_metadata_value(row, "workflow_name", "name") or row.get("title") or "workflow")


def _branch(row: dict[str, Any]) -> str:
    return str(_metadata_value(row, "branch", "head_branch", "ref") or "unknown")


def _conclusion(row: dict[str, Any]) -> str:
    return str(_metadata_value(row, "conclusion") or row.get("state") or "").strip().lower()


def _run_url(row: dict[str, Any]) -> str:
    return str(_metadata_value(row, "run_url", "html_url", "url") or row.get("url") or "")


def _commit_sha(row: dict[str, Any]) -> str | None:
    value = _metadata_value(row, "commit_sha", "head_sha", "sha")
    return str(value) if value not in (None, "") else None


def _run_number(row: dict[str, Any]) -> int:
    value = _metadata_value(row, "run_number", "run_id") or row.get("number") or 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _activity_id(row: dict[str, Any]) -> str:
    value = row.get("activity_id") or _metadata_value(row, "activity_id")
    if value:
        return str(value)
    return f"{row.get('repo_name', '')}#{row.get('number', '')}:{row.get('activity_type', '')}"


def _workflow_run_fingerprint(repo_name: str, workflow_name: str, branch: str) -> str:
    payload = {
        "branch": branch.strip().lower(),
        "repo_name": repo_name.strip().lower(),
        "source": SOURCE_NAME,
        "workflow_name": workflow_name.strip().lower(),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _eligible_recent_rows(
    db,
    *,
    days: int,
    now: datetime,
) -> list[dict[str, Any]]:
    if days <= 0:
        return []
    cutoff = now - timedelta(days=days)
    rows = db.get_github_workflow_runs(limit=None)
    eligible = []
    for row in rows:
        updated_at = _parse_datetime(row.get("updated_at"))
        if updated_at is None or updated_at < cutoff:
            continue
        if str(row.get("activity_type") or "") != "workflow_run":
            continue
        if _conclusion(row) not in ELIGIBLE_CONCLUSIONS:
            continue
        eligible.append(row)
    return sorted(
        eligible,
        key=lambda item: (
            str(item.get("repo_name") or ""),
            _workflow_name(item).lower(),
            _branch(item).lower(),
            _parse_datetime(item.get("updated_at")) or datetime.min.replace(tzinfo=timezone.utc),
            _activity_id(item),
        ),
    )


def build_workflow_run_idea_candidates(
    db,
    *,
    days: int = DEFAULT_DAYS,
    min_failures: int = DEFAULT_MIN_FAILURES,
    now: datetime | None = None,
) -> list[WorkflowRunIdeaCandidate]:
    """Build deterministic idea candidates from repeated workflow run failures."""
    if days <= 0 or min_failures <= 0:
        return []
    now = now or datetime.now(timezone.utc)
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in _eligible_recent_rows(db, days=days, now=now):
        key = (str(row.get("repo_name") or ""), _workflow_name(row), _branch(row))
        groups.setdefault(key, []).append(row)

    candidates: list[WorkflowRunIdeaCandidate] = []
    for (repo_name, workflow_name, branch), rows in sorted(groups.items()):
        if len(rows) < min_failures:
            continue
        latest = max(
            rows,
            key=lambda item: (
                _parse_datetime(item.get("updated_at")) or datetime.min.replace(tzinfo=timezone.utc),
                _activity_id(item),
            ),
        )
        fingerprint = _workflow_run_fingerprint(repo_name, workflow_name, branch)
        conclusions = tuple(sorted({_conclusion(row) for row in rows}))
        activity_ids = tuple(sorted(_activity_id(row) for row in rows))
        run_numbers = tuple(sorted(_run_number(row) for row in rows))
        latest_run_url = _run_url(latest)
        latest_commit_sha = _commit_sha(latest)
        topic = f"{repo_name}: repeated {workflow_name} workflow failures on {branch}"
        note = (
            f"{len(rows)} failed/cancelled GitHub Actions runs for {workflow_name} "
            f"on {branch} in {repo_name}. Latest run: {latest_run_url or 'none'}. "
            f"Latest commit SHA: {latest_commit_sha or 'unknown'}. "
            "Review the repeated CI/debugging work for a concrete content angle."
        )
        source_metadata = {
            "source": SOURCE_NAME,
            "workflow_run_fingerprint": fingerprint,
            "repo_name": repo_name,
            "workflow_name": workflow_name,
            "branch": branch,
            "failure_count": len(rows),
            "conclusions": list(conclusions),
            "latest_activity_id": _activity_id(latest),
            "activity_ids": list(activity_ids),
            "run_numbers": list(run_numbers),
            "latest_run_url": latest_run_url,
            "run_url": latest_run_url,
            "latest_commit_sha": latest_commit_sha,
            "commit_sha": latest_commit_sha,
            "latest_updated_at": latest.get("updated_at"),
            "min_failures": min_failures,
            "days": days,
        }
        candidates.append(
            WorkflowRunIdeaCandidate(
                fingerprint=fingerprint,
                repo_name=repo_name,
                workflow_name=workflow_name,
                branch=branch,
                failure_count=len(rows),
                latest_run_url=latest_run_url,
                latest_commit_sha=latest_commit_sha,
                latest_activity_id=_activity_id(latest),
                latest_updated_at=str(latest.get("updated_at") or ""),
                conclusions=conclusions,
                run_numbers=run_numbers,
                activity_ids=activity_ids,
                topic=topic,
                note=note,
                priority="high" if len(rows) >= max(3, min_failures + 1) else "normal",
                source_metadata=source_metadata,
            )
        )
    return candidates


def _open_idea_for_fingerprint(db, fingerprint: str) -> dict[str, Any] | None:
    conn = getattr(db, "conn", None)
    if conn is not None:
        cursor = conn.execute(
            """SELECT * FROM content_ideas
               WHERE status = 'open'
                 AND source = ?
                 AND source_metadata IS NOT NULL
               ORDER BY created_at ASC, id ASC""",
            (SOURCE_NAME,),
        )
        rows = [dict(row) for row in cursor.fetchall()]
    elif hasattr(db, "get_content_ideas"):
        rows = db.get_content_ideas(status="open", limit=1000, include_snoozed=True)
        rows = [row for row in rows if row.get("source") == SOURCE_NAME]
    else:
        rows = []

    for item in rows:
        try:
            metadata = json.loads(item.get("source_metadata") or "{}")
        except (TypeError, ValueError):
            continue
        if metadata.get("workflow_run_fingerprint") == fingerprint:
            return item
    return None


def seed_workflow_run_ideas(
    db,
    *,
    days: int = DEFAULT_DAYS,
    min_failures: int = DEFAULT_MIN_FAILURES,
    dry_run: bool = False,
    now: datetime | None = None,
) -> list[WorkflowRunIdeaSeedResult]:
    """Create content ideas for repeated failed/cancelled workflow runs."""
    candidates = build_workflow_run_idea_candidates(
        db,
        days=days,
        min_failures=min_failures,
        now=now,
    )
    results: list[WorkflowRunIdeaSeedResult] = []
    for candidate in candidates:
        existing = _open_idea_for_fingerprint(db, candidate.fingerprint)
        if existing:
            results.append(
                WorkflowRunIdeaSeedResult(
                    status="skipped",
                    fingerprint=candidate.fingerprint,
                    repo_name=candidate.repo_name,
                    workflow_name=candidate.workflow_name,
                    branch=candidate.branch,
                    failure_count=candidate.failure_count,
                    idea_id=existing["id"],
                    reason="open duplicate",
                    topic=candidate.topic,
                    note=candidate.note,
                    source_metadata=candidate.source_metadata,
                )
            )
            continue
        if dry_run:
            results.append(
                WorkflowRunIdeaSeedResult(
                    status="proposed",
                    fingerprint=candidate.fingerprint,
                    repo_name=candidate.repo_name,
                    workflow_name=candidate.workflow_name,
                    branch=candidate.branch,
                    failure_count=candidate.failure_count,
                    idea_id=None,
                    reason="dry run",
                    topic=candidate.topic,
                    note=candidate.note,
                    source_metadata=candidate.source_metadata,
                )
            )
            continue
        idea_id = db.add_content_idea(
            note=candidate.note,
            topic=candidate.topic,
            priority=candidate.priority,
            source=SOURCE_NAME,
            source_metadata=candidate.source_metadata,
        )
        results.append(
            WorkflowRunIdeaSeedResult(
                status="created",
                fingerprint=candidate.fingerprint,
                repo_name=candidate.repo_name,
                workflow_name=candidate.workflow_name,
                branch=candidate.branch,
                failure_count=candidate.failure_count,
                idea_id=idea_id,
                reason="created",
                topic=candidate.topic,
                note=candidate.note,
                source_metadata=candidate.source_metadata,
            )
        )
    return results
