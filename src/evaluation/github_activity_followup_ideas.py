"""Build and seed follow-up content ideas from completed GitHub activity."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


SOURCE_NAME = "github_activity_followup"
DEFAULT_DAYS = 14
DEFAULT_LIMIT = 10
DEFAULT_ACTIVITY_TYPES = ("pull_request", "release", "issue")


@dataclass(frozen=True)
class GitHubActivityFollowupIdea:
    activity_id: str
    repo_name: str
    activity_type: str
    title: str
    url: str
    suggested_topic: str
    note: str
    priority: str
    duplicate_reason: str | None = None
    idea_id: int | None = None
    status: str = "candidate"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def source_metadata(self) -> dict[str, Any]:
        return {
            "source": SOURCE_NAME,
            "repo": self.repo_name,
            "repo_name": self.repo_name,
            "activity_id": self.activity_id,
            "source_activity_id": self.activity_id,
            "activity_type": self.activity_type,
            "title": self.title,
            "url": self.url,
        }


@dataclass(frozen=True)
class GitHubActivityFollowupIdeaReport:
    generated_at: str
    filters: dict[str, Any]
    candidates: tuple[GitHubActivityFollowupIdea, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        candidates = [candidate.to_dict() for candidate in self.candidates]
        for item, candidate in zip(candidates, self.candidates, strict=True):
            item["source_metadata"] = candidate.source_metadata
        return {
            "artifact_type": "github_activity_followup_ideas",
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "summary": {
                "candidate": sum(1 for item in self.candidates if item.status == "candidate"),
                "created": sum(1 for item in self.candidates if item.status == "created"),
                "skipped": sum(1 for item in self.candidates if item.status == "skipped"),
                "total": len(self.candidates),
            },
            "candidates": candidates,
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
        }


def build_github_activity_followup_idea_candidates(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    activity_types: tuple[str, ...] | list[str] | None = DEFAULT_ACTIVITY_TYPES,
    repo: str | None = None,
    limit: int | None = DEFAULT_LIMIT,
    dry_run: bool = True,
    now: datetime | None = None,
) -> GitHubActivityFollowupIdeaReport:
    """Return deterministic follow-up idea payloads for recent completed activity."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive")
    normalized_types = _normalize_filters(activity_types or DEFAULT_ACTIVITY_TYPES)
    if not normalized_types:
        raise ValueError("activity_types must include at least one value")
    normalized_repo = _normalize_text(repo) or None

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "cutoff": cutoff.isoformat(),
        "activity_types": list(normalized_types),
        "repo": normalized_repo,
        "limit": limit,
        "dry_run": dry_run,
    }

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or _has_missing_required_columns(missing_columns):
        return GitHubActivityFollowupIdeaReport(
            generated_at=generated_at.isoformat(),
            filters=filters,
            candidates=(),
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    rows = _load_completed_activity_rows(
        conn,
        cutoff=cutoff,
        now=generated_at,
        activity_types=normalized_types,
        repo=normalized_repo,
    )
    generated_refs = _generated_activity_refs(conn, schema)
    idea_refs = _idea_activity_refs(conn, schema)

    candidates = [
        _row_to_candidate(row, generated_refs=generated_refs, idea_refs=idea_refs)
        for row in rows
    ]
    candidates.sort(key=_candidate_sort_key)
    if limit is not None:
        candidates = candidates[:limit]

    return GitHubActivityFollowupIdeaReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        candidates=tuple(candidates),
        missing_columns=missing_columns,
    )


def seed_github_activity_followup_ideas(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    activity_types: tuple[str, ...] | list[str] | None = DEFAULT_ACTIVITY_TYPES,
    repo: str | None = None,
    limit: int | None = DEFAULT_LIMIT,
    dry_run: bool = True,
    now: datetime | None = None,
) -> GitHubActivityFollowupIdeaReport:
    """Preview or insert follow-up content ideas for eligible GitHub activity."""
    report = build_github_activity_followup_idea_candidates(
        db_or_conn,
        days=days,
        activity_types=activity_types,
        repo=repo,
        limit=limit,
        dry_run=dry_run,
        now=now,
    )
    if dry_run or report.missing_tables or _has_missing_required_columns(report.missing_columns or {}):
        return report

    seeded: list[GitHubActivityFollowupIdea] = []
    for candidate in report.candidates:
        if candidate.duplicate_reason:
            seeded.append(candidate)
            continue
        idea_id = _insert_content_idea(db_or_conn, candidate)
        seeded.append(
            GitHubActivityFollowupIdea(
                activity_id=candidate.activity_id,
                repo_name=candidate.repo_name,
                activity_type=candidate.activity_type,
                title=candidate.title,
                url=candidate.url,
                suggested_topic=candidate.suggested_topic,
                note=candidate.note,
                priority=candidate.priority,
                idea_id=idea_id,
                status="created",
            )
        )
    return GitHubActivityFollowupIdeaReport(
        generated_at=report.generated_at,
        filters={**report.filters, "dry_run": False},
        candidates=tuple(seeded),
        missing_tables=report.missing_tables,
        missing_columns=report.missing_columns,
    )


def format_github_activity_followup_ideas_json(
    report: GitHubActivityFollowupIdeaReport,
) -> str:
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_github_activity_followup_ideas_text(
    report: GitHubActivityFollowupIdeaReport,
) -> str:
    payload = report.to_dict()
    summary = payload["summary"]
    lines = [
        (
            f"candidate={summary['candidate']} created={summary['created']} "
            f"skipped={summary['skipped']}"
        ),
        f"{'Status':9s}  {'Priority':8s}  {'Activity':32s}  Topic / reason",
        f"{'-' * 9:9s}  {'-' * 8:8s}  {'-' * 32:32s}  {'-' * 48}",
    ]
    if not report.candidates:
        lines.append("none       -         -                                 no eligible GitHub activity")
        return "\n".join(lines)
    for candidate in report.candidates:
        status = "skipped" if candidate.duplicate_reason else candidate.status
        reason = candidate.duplicate_reason or candidate.suggested_topic
        activity = f"{candidate.repo_name} {candidate.activity_type}"
        lines.append(
            f"{status:9s}  {candidate.priority:8s}  "
            f"{_shorten(activity, 32):32s}  {_shorten(reason, 48)}"
        )
    return "\n".join(lines)


def _load_completed_activity_rows(
    conn: sqlite3.Connection,
    *,
    cutoff: datetime,
    now: datetime,
    activity_types: tuple[str, ...],
    repo: str | None,
) -> list[dict[str, Any]]:
    placeholders = ", ".join("?" for _ in activity_types)
    where = [
        f"activity_type IN ({placeholders})",
        """
        (
          (activity_type = 'pull_request' AND merged_at IS NOT NULL AND TRIM(merged_at) != '')
          OR (activity_type = 'release')
          OR (LOWER(COALESCE(state, '')) = 'closed')
          OR (closed_at IS NOT NULL AND TRIM(closed_at) != '')
        )
        """,
        "datetime(COALESCE(NULLIF(merged_at, ''), NULLIF(closed_at, ''), updated_at)) >= datetime(?)",
        "datetime(COALESCE(NULLIF(merged_at, ''), NULLIF(closed_at, ''), updated_at)) <= datetime(?)",
    ]
    params: list[Any] = [*activity_types, cutoff.isoformat(), now.isoformat()]
    if repo:
        where.append("repo_name = ?")
        params.append(repo)
    cursor = conn.execute(
        f"""SELECT *
            FROM github_activity
            WHERE {' AND '.join(where)}
            ORDER BY
                datetime(COALESCE(NULLIF(merged_at, ''), NULLIF(closed_at, ''), updated_at)) DESC,
                id DESC""",
        params,
    )
    return [_row_to_dict(row) for row in cursor.fetchall()]


def _row_to_candidate(
    row: dict[str, Any],
    *,
    generated_refs: set[str],
    idea_refs: set[str],
) -> GitHubActivityFollowupIdea:
    repo_name = _normalize_text(row.get("repo_name"))
    activity_type = _normalize_text(row.get("activity_type"))
    title = _normalize_text(row.get("title") or f"GitHub {activity_type} activity")
    url = _normalize_text(row.get("url") or _metadata(row.get("metadata")).get("url"))
    activity_id = _activity_id(row)
    refs = _activity_refs(row, activity_id)
    duplicate_reason = None
    if refs & generated_refs:
        duplicate_reason = "already referenced by generated_content"
    elif refs & idea_refs:
        duplicate_reason = "already referenced by content_ideas"
    priority = _priority(row)
    suggested_topic = _suggested_topic(row)
    return GitHubActivityFollowupIdea(
        activity_id=activity_id,
        repo_name=repo_name,
        activity_type=activity_type,
        title=title,
        url=url,
        suggested_topic=suggested_topic,
        note=_note(row, suggested_topic=suggested_topic, activity_id=activity_id, url=url),
        priority=priority,
        duplicate_reason=duplicate_reason,
        status="skipped" if duplicate_reason else "candidate",
    )


def _activity_id(row: dict[str, Any]) -> str:
    metadata = _metadata(row.get("metadata"))
    explicit = _normalize_text(
        metadata.get("activity_id")
        or metadata.get("source_activity_id")
        or row.get("activity_id")
    )
    if explicit:
        return explicit
    return (
        f"{_normalize_text(row.get('repo_name'))}#"
        f"{_normalize_text(row.get('number'))}:"
        f"{_normalize_text(row.get('activity_type'))}"
    )


def _activity_refs(row: dict[str, Any], activity_id: str) -> set[str]:
    metadata = _metadata(row.get("metadata"))
    refs = {
        _normalize_text(row.get("id")),
        activity_id,
        _normalize_text(metadata.get("activity_id")),
        _normalize_text(metadata.get("source_activity_id")),
        _normalize_text(metadata.get("github_activity_id")),
    }
    return {ref for ref in refs if ref}


def _generated_activity_refs(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> set[str]:
    if "generated_content" not in schema or "source_activity_ids" not in schema["generated_content"]:
        return set()
    refs: set[str] = set()
    rows = conn.execute(
        """SELECT source_activity_ids
           FROM generated_content
           WHERE source_activity_ids IS NOT NULL
             AND TRIM(source_activity_ids) != ''"""
    ).fetchall()
    for row in rows:
        refs.update(_json_list(row["source_activity_ids"]))
    return refs


def _idea_activity_refs(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> set[str]:
    if "content_ideas" not in schema or "source_metadata" not in schema["content_ideas"]:
        return set()
    refs: set[str] = set()
    rows = conn.execute(
        """SELECT source_metadata
           FROM content_ideas
           WHERE source_metadata IS NOT NULL
             AND TRIM(source_metadata) != ''"""
    ).fetchall()
    for row in rows:
        metadata = _metadata(row["source_metadata"])
        for key in (
            "activity_id",
            "source_activity_id",
            "github_activity_id",
            "github_activity_ids",
            "source_activity_ids",
        ):
            value = metadata.get(key)
            if isinstance(value, list):
                refs.update(_normalize_text(item) for item in value if _normalize_text(item))
            elif _normalize_text(value):
                refs.add(_normalize_text(value))
    return refs


def _insert_content_idea(db_or_conn: Any, candidate: GitHubActivityFollowupIdea) -> int:
    add_idea = getattr(db_or_conn, "add_content_idea", None) or getattr(db_or_conn, "insert_content_idea", None)
    if callable(add_idea):
        return int(
            add_idea(
                note=candidate.note,
                topic=candidate.suggested_topic,
                priority=candidate.priority,
                source=SOURCE_NAME,
                source_metadata=candidate.source_metadata,
            )
        )
    conn = _connection(db_or_conn)
    cursor = conn.execute(
        """INSERT INTO content_ideas
           (note, topic, priority, status, source, source_metadata)
           VALUES (?, ?, ?, 'open', ?, ?)""",
        (
            candidate.note,
            candidate.suggested_topic,
            candidate.priority,
            SOURCE_NAME,
            json.dumps(candidate.source_metadata, sort_keys=True),
        ),
    )
    conn.commit()
    return int(cursor.lastrowid)


def _priority(row: dict[str, Any]) -> str:
    activity_type = _normalize_text(row.get("activity_type"))
    if activity_type == "pull_request" and _normalize_text(row.get("merged_at")):
        return "high"
    if activity_type == "release":
        return "normal"
    return "normal"


def _suggested_topic(row: dict[str, Any]) -> str:
    activity_type = _normalize_text(row.get("activity_type")).replace("_", " ")
    repo_name = _normalize_text(row.get("repo_name"))
    if row.get("activity_type") == "pull_request" and _normalize_text(row.get("merged_at")):
        return f"Merged work in {repo_name}"
    if row.get("activity_type") == "release":
        return f"Release follow-up for {repo_name}"
    return f"Closed {activity_type} follow-up for {repo_name}"


def _note(
    row: dict[str, Any],
    *,
    suggested_topic: str,
    activity_id: str,
    url: str,
) -> str:
    activity_type = _normalize_text(row.get("activity_type")).replace("_", " ")
    event = "merged" if row.get("activity_type") == "pull_request" and row.get("merged_at") else "completed"
    evidence = url or "stored GitHub activity"
    body = _excerpt(row.get("body") or _metadata(row.get("metadata")).get("body") or row.get("title"))
    return (
        f"{suggested_topic}: turn the recently {event} GitHub {activity_type} "
        f"'{_normalize_text(row.get('title'))}' into future social or newsletter material. "
        f"Use {evidence} and source activity {activity_id}. Relevant detail: {body}"
    )


def _candidate_sort_key(candidate: GitHubActivityFollowupIdea) -> tuple[Any, ...]:
    return (
        candidate.duplicate_reason is not None,
        {"high": 0, "normal": 1, "low": 2}.get(candidate.priority, 3),
        candidate.repo_name,
        candidate.activity_type,
        candidate.activity_id,
    )


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    expected = {
        "github_activity": {
            "id",
            "repo_name",
            "activity_type",
            "number",
            "title",
            "state",
            "url",
            "updated_at",
            "closed_at",
            "merged_at",
            "metadata",
        },
        "generated_content": {"source_activity_ids"},
    }
    missing_tables = tuple(table for table in expected if table not in schema)
    missing_columns = {
        table: tuple(sorted(columns - schema.get(table, set())))
        for table, columns in expected.items()
        if table in schema and columns - schema[table]
    }
    return missing_tables, missing_columns


def _has_missing_required_columns(missing_columns: dict[str, tuple[str, ...]]) -> bool:
    required = {"id", "repo_name", "activity_type", "number", "title", "updated_at"}
    return bool(required & set(missing_columns.get("github_activity", ())))


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    return {
        table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        for table in tables
        if table
    }


def _json_list(value: Any) -> set[str]:
    if isinstance(value, list):
        return {_normalize_text(item) for item in value if _normalize_text(item)}
    if not isinstance(value, str) or not value.strip():
        return set()
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {_normalize_text(value)}
    if not isinstance(parsed, list):
        return set()
    return {_normalize_text(item) for item in parsed if _normalize_text(item)}


def _metadata(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _row_to_dict(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return dict(row)
    if hasattr(row, "keys"):
        return {key: row[key] for key in row.keys()}
    return dict(row)


def _normalize_filters(values: tuple[str, ...] | list[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(item for value in values if (item := _normalize_text(value))))


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _excerpt(value: Any, limit: int = 220) -> str:
    text = " ".join(_normalize_text(value).split())
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "..."


def _shorten(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: max(0, limit - 1)].rstrip() + "..."


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
