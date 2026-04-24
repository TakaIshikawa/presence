"""Seed reviewable content ideas from stale open or recently closed GitHub issues."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal


IssueSeedMode = Literal["stale_open", "closed"]

STALE_OPEN_SOURCE_NAME = "github_issue_stale_seed"
CLOSED_SOURCE_NAME = "github_issue_seed"
BODY_EXCERPT_CHARS = 360
DEFAULT_LABELS = frozenset(
    {
        "bug",
        "docs",
        "documentation",
        "question",
        "enhancement",
        "user-feedback",
        "user feedback",
        "feedback",
    }
)

HIGH_PRIORITY_LABELS = {
    "blocker",
    "critical",
    "customer",
    "incident",
    "p0",
    "p1",
    "regression",
    "security",
    "sev1",
    "sev2",
}
NORMAL_PRIORITY_LABELS = {
    "bug",
    "docs",
    "documentation",
    "enhancement",
    "feature",
    "help wanted",
    "performance",
    "reliability",
    "support",
    "ux",
}
LOW_PRIORITY_LABELS = {
    "chore",
    "dependencies",
    "dependency",
    "duplicate",
    "maintenance",
    "question",
    "wontfix",
}


@dataclass(frozen=True)
class IssueIdeaCandidate:
    repo_name: str
    number: int
    activity_id: str
    title: str
    url: str
    labels: list[str]
    topic: str
    note: str
    source_metadata: dict[str, Any]
    priority: str = "normal"
    updated_at: str = ""
    stale_days: int | None = None
    closed_at: str | None = None
    angle: str | None = None


@dataclass(frozen=True)
class IssueIdeaSeedResult:
    status: str
    repo_name: str
    number: int
    idea_id: int | None
    reason: str
    topic: str
    note: str
    source_metadata: dict[str, Any] | None = None
    priority: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


IssueSeedResult = IssueIdeaSeedResult


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _shorten(text: str | None, width: int = 70) -> str:
    value = " ".join((text or "").split())
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."


def _body_excerpt(text: str | None, width: int = BODY_EXCERPT_CHARS) -> str:
    return _shorten(text, width) or "No issue body provided."


def _normalize_label(label: str) -> str:
    return re.sub(r"[\s_]+", "-", label.strip().lower())


def _matches_labels(labels: list[str], wanted: set[str]) -> bool:
    normalized = {_normalize_label(label) for label in labels}
    normalized_wanted = {_normalize_label(label) for label in wanted}
    return bool(normalized & normalized_wanted)


def _normalized_labels(labels: list[str]) -> set[str]:
    return {label.strip().lower() for label in labels if label.strip()}


def _priority_from_labels(labels: list[str]) -> str:
    normalized = _normalized_labels(labels)
    if normalized & HIGH_PRIORITY_LABELS:
        return "high"
    if normalized and normalized <= LOW_PRIORITY_LABELS:
        return "low"
    if normalized & NORMAL_PRIORITY_LABELS:
        return "normal"
    if normalized & LOW_PRIORITY_LABELS:
        return "low"
    return "normal"


def _clean_title(title: str) -> str:
    value = re.sub(
        r"^\s*(bug|fix|docs|feat|feature|chore)(\(.+?\))?:\s*",
        "",
        title,
        flags=re.I,
    )
    value = re.sub(r"\s+", " ", value).strip(" .")
    return value or title.strip() or "Closed issue"


def issue_to_candidate(
    row: dict,
    *,
    now: datetime | None = None,
) -> IssueIdeaCandidate | None:
    now = now or datetime.now(timezone.utc)
    updated_at = _parse_datetime(row.get("updated_at"))
    if updated_at is None:
        return None
    stale_days = max(0, int((now - updated_at).total_seconds() // 86400))
    repo_name = str(row.get("repo_name") or "")
    number = int(row.get("number"))
    title = str(row.get("title") or f"Issue #{number}")
    url = str(row.get("url") or "")
    labels = [str(label) for label in (row.get("labels") or []) if str(label).strip()]
    topic = f"{repo_name}: {title}".strip(": ")
    body_excerpt = _body_excerpt(row.get("body"), width=280)
    note = (
        f"Stale open issue #{number} in {repo_name}: {title}. "
        f"Last updated {stale_days} days ago. "
        f"Labels: {', '.join(labels) if labels else 'none'}. "
        f"URL: {url or 'none'}. "
        f"Body excerpt: {body_excerpt} "
        "Suggested angle: turn the unresolved user pain into a practical lesson, "
        "diagnostic checklist, or project-direction update."
    )
    metadata = {
        "source": STALE_OPEN_SOURCE_NAME,
        "repo_name": repo_name,
        "number": number,
        "activity_id": row.get("activity_id"),
        "labels": labels,
        "url": url,
        "stale_days": stale_days,
        "title": title,
        "updated_at": row.get("updated_at"),
    }
    return IssueIdeaCandidate(
        repo_name=repo_name,
        number=number,
        activity_id=str(row.get("activity_id") or ""),
        title=title,
        url=url,
        labels=labels,
        topic=topic,
        note=note,
        source_metadata=metadata,
        priority="normal",
        updated_at=str(row.get("updated_at") or ""),
        stale_days=stale_days,
    )


def closed_issue_to_candidate(row: dict) -> IssueIdeaCandidate:
    repo_name = str(row.get("repo_name") or "")
    number = int(row.get("number"))
    title = str(row.get("title") or "")
    clean_title = _clean_title(title)
    labels = [str(label) for label in (row.get("labels") or []) if str(label).strip()]
    priority = _priority_from_labels(labels)
    url = str(row.get("url") or "")
    metadata_value = row.get("metadata") or {}
    closed_at = row.get("closed_at") or (
        metadata_value.get("closed_at") if isinstance(metadata_value, dict) else None
    )
    updated_at = str(row.get("updated_at") or "")
    body_excerpt = _body_excerpt(row.get("body"))
    topic = (
        f"{repo_name}: resolved {clean_title}"
        if repo_name
        else f"Resolved {clean_title}"
    )
    angle = (
        f"Turn closed issue #{number} into a resolution story: the user-facing problem, "
        "what changed, and the lesson worth reusing."
    )
    note = (
        f"Closed issue #{number} in {repo_name}: {title}. "
        f"Labels: {', '.join(labels) if labels else 'none'}. "
        f"URL: {url or 'none'}. "
        f"Body excerpt: {body_excerpt} "
        f"Suggested angle: {angle}"
    )
    metadata = {
        "source": CLOSED_SOURCE_NAME,
        "activity_id": row.get("activity_id"),
        "repo_name": repo_name,
        "issue_number": number,
        "number": number,
        "title": title,
        "url": url,
        "labels": labels,
        "closed_at": closed_at,
        "updated_at": updated_at,
        "body_excerpt": body_excerpt,
        "priority": priority,
    }
    return IssueIdeaCandidate(
        repo_name=repo_name,
        number=number,
        activity_id=str(row.get("activity_id") or ""),
        title=title,
        url=url,
        labels=labels,
        topic=topic,
        note=note,
        source_metadata=metadata,
        priority=priority,
        updated_at=updated_at,
        closed_at=closed_at,
        angle=angle,
    )


def _stale_open_issue_rows(
    db,
    *,
    days_stale: int,
    repo: str | None,
    limit: int | None,
    now: datetime,
) -> list[dict]:
    if days_stale <= 0 or (limit is not None and limit <= 0):
        return []
    cutoff = (now - timedelta(days=days_stale)).isoformat()
    params: list[object] = [cutoff]
    repo_filter = ""
    if repo:
        repo_filter = " AND repo_name = ?"
        params.append(repo)
    limit_clause = ""
    if limit is not None:
        limit_clause = " LIMIT ?"
        params.append(limit)

    rows = db.conn.execute(
        f"""SELECT * FROM github_activity
            WHERE activity_type = 'issue'
              AND LOWER(COALESCE(state, '')) = 'open'
              AND updated_at <= ?{repo_filter}
            ORDER BY updated_at ASC, id ASC{limit_clause}""",
        tuple(params),
    ).fetchall()
    return [db._github_activity_from_row(row) for row in rows]


def seed_issue_ideas(
    db,
    *,
    mode: IssueSeedMode = "stale_open",
    days_stale: int = 30,
    days: int = 14,
    limit: int | None = 10,
    labels: list[str] | None = None,
    repo: str | None = None,
    priority: str = "normal",
    dry_run: bool = False,
    now: datetime | None = None,
) -> list[IssueIdeaSeedResult]:
    """Create content ideas from either stale open or recently closed issues."""
    now = now or datetime.now(timezone.utc)

    if mode == "closed":
        if days <= 0 or (limit is not None and limit <= 0):
            return []
        rows = db.get_recent_closed_github_issues(
            days=days,
            repo_name=repo,
            limit=limit,
            now=now,
        )
        results: list[IssueIdeaSeedResult] = []
        for row in rows:
            candidate = closed_issue_to_candidate(row)
            existing = db.find_active_content_idea_for_source_metadata(
                note=candidate.note,
                topic=candidate.topic,
                source=CLOSED_SOURCE_NAME,
                source_metadata=candidate.source_metadata,
            )
            if existing:
                results.append(
                    IssueIdeaSeedResult(
                        status="skipped",
                        repo_name=candidate.repo_name,
                        number=candidate.number,
                        idea_id=existing["id"],
                        reason=f"{existing['status']} duplicate",
                        topic=candidate.topic,
                        note=candidate.note,
                        source_metadata=candidate.source_metadata,
                        priority=candidate.priority,
                    )
                )
                continue
            if dry_run:
                results.append(
                    IssueIdeaSeedResult(
                        status="proposed",
                        repo_name=candidate.repo_name,
                        number=candidate.number,
                        idea_id=None,
                        reason="dry run",
                        topic=candidate.topic,
                        note=candidate.note,
                        source_metadata=candidate.source_metadata,
                        priority=candidate.priority,
                    )
                )
                continue
            idea_id = db.add_content_idea(
                note=candidate.note,
                topic=candidate.topic,
                priority=candidate.priority,
                source=CLOSED_SOURCE_NAME,
                source_metadata=candidate.source_metadata,
            )
            results.append(
                IssueIdeaSeedResult(
                    status="created",
                    repo_name=candidate.repo_name,
                    number=candidate.number,
                    idea_id=idea_id,
                    reason="created",
                    topic=candidate.topic,
                    note=candidate.note,
                    source_metadata=candidate.source_metadata,
                    priority=candidate.priority,
                )
            )
        return results

    if days_stale <= 0 or (limit is not None and limit <= 0):
        return []
    wanted_labels = set(labels or DEFAULT_LABELS)
    rows = _stale_open_issue_rows(
        db,
        days_stale=days_stale,
        repo=repo,
        limit=limit,
        now=now,
    )

    results: list[IssueIdeaSeedResult] = []
    for row in rows:
        candidate = issue_to_candidate(row, now=now)
        if candidate is None:
            continue
        if not _matches_labels(candidate.labels, wanted_labels):
            results.append(
                IssueIdeaSeedResult(
                    status="skipped",
                    repo_name=candidate.repo_name,
                    number=candidate.number,
                    idea_id=None,
                    reason="label filter",
                    topic=candidate.topic,
                    note=candidate.note,
                    source_metadata=candidate.source_metadata,
                    priority=priority,
                )
            )
            continue

        existing = db.find_active_content_idea_for_source_metadata(
            note=candidate.note,
            topic=candidate.topic,
            source=STALE_OPEN_SOURCE_NAME,
            source_metadata=candidate.source_metadata,
        )
        if existing:
            results.append(
                IssueIdeaSeedResult(
                    status="duplicate",
                    repo_name=candidate.repo_name,
                    number=candidate.number,
                    idea_id=existing["id"],
                    reason=f"{existing['status']} duplicate",
                    topic=candidate.topic,
                    note=candidate.note,
                    source_metadata=candidate.source_metadata,
                    priority=priority,
                )
            )
            continue

        if dry_run:
            results.append(
                IssueIdeaSeedResult(
                    status="proposed",
                    repo_name=candidate.repo_name,
                    number=candidate.number,
                    idea_id=None,
                    reason="dry run",
                    topic=candidate.topic,
                    note=candidate.note,
                    source_metadata=candidate.source_metadata,
                    priority=priority,
                )
            )
            continue

        idea_id = db.add_content_idea(
            candidate.note,
            topic=candidate.topic,
            priority=priority,
            source=STALE_OPEN_SOURCE_NAME,
            source_metadata=candidate.source_metadata,
        )
        results.append(
            IssueIdeaSeedResult(
                status="created",
                repo_name=candidate.repo_name,
                number=candidate.number,
                idea_id=idea_id,
                reason="created",
                topic=candidate.topic,
                note=candidate.note,
                source_metadata=candidate.source_metadata,
                priority=priority,
            )
        )

    return results
