"""Rank stale, unpublished content ideas for rescue."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from pathlib import Path
from typing import Any, Iterable, Mapping


DEFAULT_STALE_DAYS = 30
DEFAULT_LIMIT = 10
DEFAULT_STATUS = "open"
STATUSES = ("open", "promoted", "dismissed")

PRIORITY_WEIGHTS = {
    "high": 100,
    "normal": 50,
    "low": 0,
}


@dataclass(frozen=True)
class ContentIdeaAgePriorityItem:
    """One stale content idea priority row."""

    idea_id: int
    age_days: int
    last_touched_days: int
    score: int
    priority: str
    status: str | None
    topic: str | None
    note: str
    created_at: str | None
    last_touched_at: str | None
    campaign_relevance: str
    campaign_score: int
    dependency_status: str
    dependency_summary: str | None
    score_components: dict[str, int]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ContentIdeaAgePriorityReport:
    """Ranked stale ideas plus blocked stale ideas."""

    generated_at: str
    stale_days: int
    limit: int
    status: str | None
    total_candidates: int
    stale_count: int
    blocked_count: int
    ideas: tuple[ContentIdeaAgePriorityItem, ...]
    blocked_ideas: tuple[ContentIdeaAgePriorityItem, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "content_idea_age_priority",
            "blocked_count": self.blocked_count,
            "blocked_ideas": [item.to_dict() for item in self.blocked_ideas],
            "generated_at": self.generated_at,
            "ideas": [item.to_dict() for item in self.ideas],
            "limit": self.limit,
            "stale_count": self.stale_count,
            "stale_days": self.stale_days,
            "status": self.status,
            "total_candidates": self.total_candidates,
        }


def build_content_idea_age_priority_report(
    db_or_conn: Any,
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
    limit: int = DEFAULT_LIMIT,
    status: str | None = DEFAULT_STATUS,
    now: datetime | None = None,
) -> ContentIdeaAgePriorityReport:
    """Return a deterministic report of stale open ideas that need attention."""
    if stale_days <= 0:
        raise ValueError("stale_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    if status is not None and status not in STATUSES:
        raise ValueError(f"status must be one of: {', '.join(STATUSES)}")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _ensure_aware(now or datetime.now(timezone.utc))
    rows = _load_db_ideas(conn, schema, status=status)
    campaign_ids = _active_campaign_ids(conn, schema, generated_at)
    published_idea_ids = _published_idea_ids(conn, schema)

    return _build_report_from_rows(
        rows,
        stale_days=stale_days,
        limit=limit,
        status=status,
        now=generated_at,
        active_campaign_ids=campaign_ids,
        published_idea_ids=published_idea_ids,
    )


def build_content_idea_age_priority_report_from_fixture(
    fixture_path: str | Path,
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
    limit: int = DEFAULT_LIMIT,
    status: str | None = DEFAULT_STATUS,
    now: datetime | None = None,
) -> ContentIdeaAgePriorityReport:
    """Build the report from fixture JSON containing an ideas list or raw list."""
    if stale_days <= 0:
        raise ValueError("stale_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    if status is not None and status not in STATUSES:
        raise ValueError(f"status must be one of: {', '.join(STATUSES)}")

    payload = json.loads(Path(fixture_path).read_text(encoding="utf-8"))
    rows = payload.get("ideas", payload) if isinstance(payload, Mapping) else payload
    if not isinstance(rows, list):
        raise ValueError("fixture must be a JSON list or object with an ideas list")
    active_campaign_ids = {
        int(value)
        for value in (payload.get("active_campaign_ids", []) if isinstance(payload, Mapping) else [])
    }
    published_idea_ids = {
        int(value)
        for value in (payload.get("published_idea_ids", []) if isinstance(payload, Mapping) else [])
    }
    generated_at = _ensure_aware(now or datetime.now(timezone.utc))
    return _build_report_from_rows(
        [dict(row) for row in rows if isinstance(row, Mapping)],
        stale_days=stale_days,
        limit=limit,
        status=status,
        now=generated_at,
        active_campaign_ids=active_campaign_ids,
        published_idea_ids=published_idea_ids,
    )


def format_content_idea_age_priority_json(report: ContentIdeaAgePriorityReport) -> str:
    """Render a content idea age priority report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_content_idea_age_priority_text(report: ContentIdeaAgePriorityReport) -> str:
    """Render a content idea age priority report for terminal review."""
    lines = [
        "Content Idea Age Priority",
        f"Generated: {report.generated_at}",
        f"Stale threshold: {report.stale_days} days",
        (
            f"Candidates: {report.total_candidates} "
            f"stale={report.stale_count} blocked={report.blocked_count}"
        ),
        "",
        "Priority ideas",
    ]
    if not report.ideas:
        lines.append("  - no stale unblocked content ideas found")
    for item in report.ideas:
        lines.append(_format_item(item))

    lines.append("")
    lines.append("Blocked ideas")
    if not report.blocked_ideas:
        lines.append("  - no stale blocked content ideas found")
    for item in report.blocked_ideas:
        lines.append(_format_item(item))
        if item.dependency_summary:
            lines.append(f"    blocker={item.dependency_summary}")
    return "\n".join(lines)


def _build_report_from_rows(
    rows: list[dict[str, Any]],
    *,
    stale_days: int,
    limit: int,
    status: str | None,
    now: datetime,
    active_campaign_ids: set[int],
    published_idea_ids: set[int],
) -> ContentIdeaAgePriorityReport:
    cutoff = now - timedelta(days=stale_days)
    candidates: list[ContentIdeaAgePriorityItem] = []
    blocked: list[ContentIdeaAgePriorityItem] = []
    total_candidates = 0

    for row in rows:
        idea_id = _int_or_none(row.get("id") or row.get("idea_id"))
        if idea_id is None:
            continue
        if status is not None and _text(row.get("status") or "open") != status:
            continue
        if idea_id in published_idea_ids or _row_is_published(row):
            continue

        total_candidates += 1
        created_at = _parse_timestamp(row.get("created_at"))
        if created_at is None or created_at > cutoff:
            continue

        item = _rank_item(row, now=now, active_campaign_ids=active_campaign_ids)
        if item.dependency_status == "blocked":
            blocked.append(item)
        else:
            candidates.append(item)

    ranked = tuple(sorted(candidates, key=_rank_key)[:limit])
    ranked_blocked = tuple(sorted(blocked, key=_rank_key)[:limit])
    return ContentIdeaAgePriorityReport(
        generated_at=now.isoformat(),
        stale_days=stale_days,
        limit=limit,
        status=status,
        total_candidates=total_candidates,
        stale_count=len(candidates) + len(blocked),
        blocked_count=len(blocked),
        ideas=ranked,
        blocked_ideas=ranked_blocked,
    )


def _rank_item(
    row: Mapping[str, Any],
    *,
    now: datetime,
    active_campaign_ids: set[int],
) -> ContentIdeaAgePriorityItem:
    metadata = _decode_json_object(row.get("source_metadata")) or {}
    created_at = _parse_timestamp(row.get("created_at")) or now
    last_touched_at = (
        _parse_timestamp(metadata.get("last_touched_at"))
        or _parse_timestamp(metadata.get("touched_at"))
        or _parse_timestamp(metadata.get("last_seen_at"))
        or _parse_timestamp(row.get("updated_at"))
        or created_at
    )
    age_days = _age_days(created_at, now)
    last_touched_days = _age_days(last_touched_at, now)
    priority = _priority(row.get("priority"))
    dependency_status, dependency_summary = _dependency_status(metadata)
    campaign_score, campaign_relevance = _campaign_score(row, metadata, active_campaign_ids)
    components = {
        "age": min(age_days, 120),
        "campaign": campaign_score,
        "dependency": -40 if dependency_status == "unknown_dependency" else 0,
        "last_touched": min(last_touched_days, 30),
        "priority": PRIORITY_WEIGHTS[priority],
    }
    score = sum(components.values())
    return ContentIdeaAgePriorityItem(
        idea_id=int(row.get("id") or row.get("idea_id")),
        age_days=age_days,
        last_touched_days=last_touched_days,
        score=score,
        priority=priority,
        status=_none_if_blank(row.get("status")),
        topic=_none_if_blank(row.get("topic")),
        note=str(row.get("note") or "").strip(),
        created_at=created_at.isoformat() if created_at else None,
        last_touched_at=last_touched_at.isoformat() if last_touched_at else None,
        campaign_relevance=campaign_relevance,
        campaign_score=campaign_score,
        dependency_status=dependency_status,
        dependency_summary=dependency_summary,
        score_components=components,
    )


def _load_db_ideas(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    status: str | None,
) -> list[dict[str, Any]]:
    if "content_ideas" not in schema:
        return []
    columns = schema["content_ideas"]
    selected = [
        _column_expr(columns, "id"),
        _column_expr(columns, "note"),
        _column_expr(columns, "topic"),
        _column_expr(columns, "priority", "'normal'"),
        _column_expr(columns, "status", "'open'"),
        _column_expr(columns, "source"),
        _column_expr(columns, "source_metadata"),
        _column_expr(columns, "created_at"),
        _column_expr(columns, "updated_at"),
    ]
    params: list[Any] = []
    where = ""
    if status is not None and "status" in columns:
        where = "WHERE status = ?"
        params.append(status)
    rows = conn.execute(
        f"""SELECT {', '.join(selected)}
            FROM content_ideas
            {where}
            ORDER BY created_at ASC, id ASC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _published_idea_ids(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> set[int]:
    linked: dict[int, set[int]] = {}
    if "planned_topics" in schema:
        columns = schema["planned_topics"]
        if {"content_id", "source_material"}.issubset(columns):
            for row in conn.execute(
                "SELECT content_id, source_material FROM planned_topics ORDER BY id ASC"
            ).fetchall():
                metadata = _decode_json_object(row["source_material"]) or {}
                idea_id = _int_or_none(metadata.get("content_idea_id"))
                content_id = _int_or_none(row["content_id"])
                if idea_id is not None and content_id is not None:
                    linked.setdefault(idea_id, set()).add(content_id)

    if "content_ideas" in schema and "source_metadata" in schema["content_ideas"]:
        for row in conn.execute("SELECT id, source_metadata FROM content_ideas ORDER BY id ASC"):
            idea_id = _int_or_none(row["id"])
            metadata = _decode_json_object(row["source_metadata"]) or {}
            if idea_id is None:
                continue
            for key in ("content_id", "generated_content_id", "source_content_id"):
                content_id = _int_or_none(metadata.get(key))
                if content_id is not None:
                    linked.setdefault(idea_id, set()).add(content_id)

    if "generated_content" in schema:
        metadata_columns = [
            column
            for column in ("metadata", "source_metadata")
            if column in schema["generated_content"]
        ]
        if metadata_columns:
            for row in conn.execute(
                f"SELECT id, {', '.join(metadata_columns)} FROM generated_content ORDER BY id ASC"
            ).fetchall():
                content_id = _int_or_none(row["id"])
                if content_id is None:
                    continue
                for column in metadata_columns:
                    metadata = _decode_json_object(row[column]) or {}
                    idea_id = _int_or_none(metadata.get("content_idea_id"))
                    if idea_id is not None:
                        linked.setdefault(idea_id, set()).add(content_id)

    published_content = _published_content_ids(conn, schema)
    return {
        idea_id
        for idea_id, content_ids in linked.items()
        if any(content_id in published_content for content_id in content_ids)
    }


def _published_content_ids(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> set[int]:
    published: set[int] = set()
    if "generated_content" in schema:
        columns = schema["generated_content"]
        published_expr = "published" if "published" in columns else "0"
        published_at_expr = "published_at" if "published_at" in columns else "NULL"
        for row in conn.execute(
            f"""SELECT id, {published_expr} AS published, {published_at_expr} AS published_at
                FROM generated_content
                ORDER BY id ASC"""
        ).fetchall():
            if int(row["published"] or 0) == 1 or row["published_at"]:
                published.add(int(row["id"]))
    if "content_publications" in schema:
        columns = schema["content_publications"]
        if {"content_id", "status"}.issubset(columns):
            for row in conn.execute(
                """SELECT DISTINCT content_id
                   FROM content_publications
                   WHERE status = 'published'
                   ORDER BY content_id ASC"""
            ).fetchall():
                published.add(int(row["content_id"]))
    return published


def _active_campaign_ids(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    now: datetime,
) -> set[int]:
    if "content_campaigns" not in schema:
        return set()
    columns = schema["content_campaigns"]
    if "id" not in columns:
        return set()
    status_expr = "status" if "status" in columns else "'active'"
    start_expr = "start_date" if "start_date" in columns else "NULL"
    end_expr = "end_date" if "end_date" in columns else "NULL"
    active: set[int] = set()
    for row in conn.execute(
        f"""SELECT id, {status_expr} AS status, {start_expr} AS start_date, {end_expr} AS end_date
            FROM content_campaigns
            ORDER BY id ASC"""
    ).fetchall():
        status = _text(row["status"])
        start = _parse_timestamp(row["start_date"])
        end = _parse_timestamp(row["end_date"])
        if status in {"active", "in_progress", "planned"} and (
            start is None or start <= now
        ) and (end is None or end >= now):
            active.add(int(row["id"]))
    return active


def _campaign_score(
    row: Mapping[str, Any],
    metadata: Mapping[str, Any],
    active_campaign_ids: set[int],
) -> tuple[int, str]:
    campaign_id = _int_or_none(metadata.get("campaign_id") or row.get("campaign_id"))
    if campaign_id is not None and campaign_id in active_campaign_ids:
        return 30, f"active_campaign:{campaign_id}"
    if campaign_id is not None:
        return 15, f"campaign:{campaign_id}"
    if any(key in metadata for key in ("planned_topic_id", "campaign_topic_id")):
        return 12, "planned_topic"
    if _text(row.get("source")) in {"campaign", "campaign_topic_import", "stale_topic_resurfacer"}:
        return 10, str(row.get("source"))
    return 0, "none"


def _dependency_status(metadata: Mapping[str, Any]) -> tuple[str, str | None]:
    blockers = list(_dependency_values(metadata))
    if not blockers:
        return "clear", None
    unresolved = [value for value in blockers if not _dependency_resolved(value)]
    if unresolved:
        return "blocked", _dependency_summary(unresolved)
    return "resolved_dependency", _dependency_summary(blockers)


def _dependency_values(metadata: Mapping[str, Any]) -> Iterable[Any]:
    dependency_keys = {
        "blocked_by",
        "blockers",
        "dependencies",
        "depends_on",
        "dependency",
        "prerequisite",
        "waiting_for",
        "wait_for",
    }
    for key, value in metadata.items():
        normalized = str(key).casefold()
        if normalized in dependency_keys or any(token in normalized for token in dependency_keys):
            if isinstance(value, list):
                yield from value
            else:
                yield value


def _dependency_resolved(value: Any) -> bool:
    if isinstance(value, Mapping):
        status = _text(value.get("status") or value.get("state"))
        if value.get("resolved") is True:
            return True
        if status in {"done", "closed", "merged", "resolved", "published", "generated"}:
            return True
        return False
    return False


def _dependency_summary(values: list[Any]) -> str:
    labels = []
    for value in values[:3]:
        if isinstance(value, Mapping):
            labels.append(str(value.get("ref") or value.get("id") or value.get("name") or value))
        else:
            labels.append(str(value))
    suffix = f" (+{len(values) - 3} more)" if len(values) > 3 else ""
    return ", ".join(labels) + suffix


def _row_is_published(row: Mapping[str, Any]) -> bool:
    if row.get("published") is True or row.get("published") == 1:
        return True
    metadata = _decode_json_object(row.get("source_metadata")) or {}
    return bool(metadata.get("published") is True or metadata.get("published_at"))


def _rank_key(item: ContentIdeaAgePriorityItem) -> tuple[Any, ...]:
    priority_order = {"high": 0, "normal": 1, "low": 2}
    return (
        -item.score,
        priority_order.get(item.priority, 3),
        -item.age_days,
        item.last_touched_at or "",
        item.idea_id,
    )


def _format_item(item: ContentIdeaAgePriorityItem) -> str:
    return (
        "  - "
        f"idea_id={item.idea_id} score={item.score} age_days={item.age_days} "
        f"priority={item.priority} last_touched_days={item.last_touched_days} "
        f"campaign={item.campaign_relevance} topic={_shorten(item.topic or '-', 36)} "
        f"note={_shorten(item.note, 72)}"
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("db_or_conn must be a sqlite3.Connection or expose .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        row["name"]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
        ).fetchall()
    }
    return {table: _table_columns(conn, table) for table in tables}


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({_quote_identifier(table)})")}


def _column_expr(columns: set[str], column: str, fallback: str = "NULL") -> str:
    if column in columns:
        return _quote_identifier(column)
    return f"{fallback} AS {_quote_identifier(column)}"


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _decode_json_object(value: Any) -> dict[str, Any] | None:
    if isinstance(value, Mapping):
        return dict(value)
    if not value:
        return None
    try:
        decoded = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    return decoded if isinstance(decoded, dict) else None


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return _ensure_aware(datetime.fromisoformat(text))
    except ValueError:
        return None


def _ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _age_days(value: datetime, now: datetime) -> int:
    return max(0, int((now - value).total_seconds() // 86400))


def _priority(value: Any) -> str:
    text = _text(value)
    return text if text in PRIORITY_WEIGHTS else "normal"


def _text(value: Any) -> str:
    return str(value or "").strip().casefold()


def _none_if_blank(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _int_or_none(value: Any) -> int | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _shorten(value: str, limit: int) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."
