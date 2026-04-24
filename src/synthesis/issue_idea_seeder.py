"""Seed reviewable content ideas from stale open GitHub issues."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


SOURCE_NAME = "github_issue_stale_seed"
BODY_EXCERPT_CHARS = 280
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


@dataclass(frozen=True)
class IssueIdeaCandidate:
    repo_name: str
    number: int
    activity_id: str
    title: str
    url: str
    labels: list[str]
    updated_at: str
    stale_days: int
    topic: str
    note: str
    source_metadata: dict[str, Any]

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class IssueIdeaSeedResult:
    status: str
    repo_name: str
    number: int
    idea_id: int | None
    reason: str
    topic: str
    note: str
    source_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


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


def _body_excerpt(text: str | None) -> str:
    return _shorten(text, BODY_EXCERPT_CHARS) or "No issue body provided."


def _normalize_label(label: str) -> str:
    return re.sub(r"[\s_]+", "-", label.strip().lower())


def _matches_labels(labels: list[str], wanted: set[str]) -> bool:
    normalized = {_normalize_label(label) for label in labels}
    normalized_wanted = {_normalize_label(label) for label in wanted}
    return bool(normalized & normalized_wanted)


def issue_to_candidate(row: dict, *, now: datetime | None = None) -> IssueIdeaCandidate | None:
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
    body_excerpt = _body_excerpt(row.get("body"))
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
        "source": SOURCE_NAME,
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
        updated_at=str(row.get("updated_at") or ""),
        stale_days=stale_days,
        topic=topic,
        note=note,
        source_metadata=metadata,
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
    days_stale: int = 30,
    limit: int | None = 10,
    labels: list[str] | None = None,
    repo: str | None = None,
    priority: str = "normal",
    dry_run: bool = False,
    now: datetime | None = None,
) -> list[IssueIdeaSeedResult]:
    """Create content ideas for stale open GitHub issues matching label filters."""
    if days_stale <= 0 or (limit is not None and limit <= 0):
        return []
    now = now or datetime.now(timezone.utc)
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
                )
            )
            continue

        existing = db.find_active_content_idea_for_source_metadata(
            note=candidate.note,
            topic=candidate.topic,
            source=SOURCE_NAME,
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
                )
            )
            continue

        idea_id = db.add_content_idea(
            candidate.note,
            topic=candidate.topic,
            priority=priority,
            source=SOURCE_NAME,
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
            )
        )

    return results
