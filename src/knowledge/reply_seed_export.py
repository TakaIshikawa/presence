"""Export high-quality reply drafts as knowledge seed candidates."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_MIN_QUALITY = 7.0
EXCLUDED_QUALITY_FLAGS = frozenset({"generic", "spam", "sycophantic"})
QUALIFYING_STATUSES = frozenset({"approved", "posted"})
QUALIFYING_EVENT_TYPES = frozenset({"approved", "posted"})
SUGGESTED_SOURCE_TYPE = "reply_queue"


@dataclass(frozen=True)
class ReplyKnowledgeSeed:
    """One reply that can be reviewed for knowledge ingestion later."""

    source_reply_id: int
    source_id: str
    source_type: str
    author_handle: str | None
    inbound_context: dict[str, Any]
    draft_text: str
    quality_score: float
    quality_flags: tuple[str, ...]
    metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["quality_flags"] = list(self.quality_flags)
        data["metadata"] = dict(self.metadata)
        return data


@dataclass(frozen=True)
class ReplyKnowledgeSeedExport:
    """Read-only export of reply knowledge seed candidates."""

    generated_at: str
    filters: dict[str, Any]
    summary: dict[str, int]
    seeds: tuple[ReplyKnowledgeSeed, ...]
    missing_tables: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "summary": dict(self.summary),
            "seeds": [seed.to_dict() for seed in self.seeds],
            "missing_tables": list(self.missing_tables),
        }


def build_reply_knowledge_seed_export(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_quality: float = DEFAULT_MIN_QUALITY,
    now: datetime | None = None,
) -> ReplyKnowledgeSeedExport:
    """Return high-quality approved or posted replies as knowledge seed records."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_quality < 0 or min_quality > 10:
        raise ValueError("min-quality must be between 0 and 10")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "min_quality": min_quality,
        "activity_since": cutoff.isoformat(),
        "excluded_quality_flags": sorted(EXCLUDED_QUALITY_FLAGS),
        "statuses": sorted(QUALIFYING_STATUSES),
    }
    missing_tables = tuple(
        table
        for table in ("reply_queue", "reply_review_events", "reply_knowledge_links")
        if table not in schema
    )
    if "reply_queue" in missing_tables:
        return _empty_export(generated_at, filters, missing_tables)

    rows = _load_reply_rows(conn, schema, cutoff=cutoff, min_quality=min_quality)
    events_by_reply = _load_review_events(conn, schema, [int(row["id"]) for row in rows])
    links_by_reply = _load_reply_knowledge_links(
        conn,
        schema,
        [int(row["id"]) for row in rows],
    )

    seeds: list[ReplyKnowledgeSeed] = []
    excluded_flags = 0
    excluded_outcome = 0
    for row in rows:
        reply_id = int(row["id"])
        events = events_by_reply.get(reply_id, ())
        if not _is_qualified_outcome(row, events):
            excluded_outcome += 1
            continue
        flags = _parse_quality_flags(row.get("quality_flags"))
        if EXCLUDED_QUALITY_FLAGS.intersection(flags):
            excluded_flags += 1
            continue
        seeds.append(_seed_from_row(row, flags=flags, events=events, links=links_by_reply.get(reply_id, ())))

    seeds = sorted(
        seeds,
        key=lambda seed: (
            -seed.quality_score,
            str(seed.metadata.get("activity_at") or ""),
            seed.source_reply_id,
        ),
    )
    return ReplyKnowledgeSeedExport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        summary={
            "seed_count": len(seeds),
            "candidate_count": len(rows),
            "excluded_by_quality_flag": excluded_flags,
            "excluded_by_outcome": excluded_outcome,
            "missing_tables": len(missing_tables),
        },
        seeds=tuple(seeds),
        missing_tables=missing_tables,
    )


def format_reply_knowledge_seed_export_json(export: ReplyKnowledgeSeedExport) -> str:
    """Serialize a reply knowledge seed export as deterministic JSON."""
    return json.dumps(export.to_dict(), indent=2, sort_keys=True)


def format_reply_knowledge_seed_export_text(export: ReplyKnowledgeSeedExport) -> str:
    """Format reply knowledge seed candidates for quick terminal review."""
    lines = [
        "Reply Knowledge Seeds",
        f"Generated: {export.generated_at}",
        (
            f"Activity since: {export.filters['activity_since']} "
            f"(days={export.filters['days']}, min_quality={export.filters['min_quality']})"
        ),
        (
            "Summary: "
            f"seeds={export.summary['seed_count']} "
            f"candidates={export.summary['candidate_count']} "
            f"flagged={export.summary['excluded_by_quality_flag']} "
            f"outcome_excluded={export.summary['excluded_by_outcome']}"
        ),
    ]
    if export.missing_tables:
        lines.append("Missing tables: " + ", ".join(export.missing_tables))
    if not export.seeds:
        lines.append("No reply knowledge seeds found.")
        return "\n".join(lines)

    lines.append("Seeds:")
    for seed in export.seeds:
        handle = seed.author_handle or "unknown"
        linked = seed.metadata.get("linked_knowledge_ids") or []
        linked_text = ", ".join(str(item) for item in linked) if linked else "none"
        preview = _preview(seed.draft_text, 120)
        lines.append(
            f"- reply_queue:{seed.source_reply_id} @{handle} "
            f"quality={seed.quality_score:.1f} status={seed.metadata.get('status')}"
        )
        lines.append(f"  source_type: {seed.source_type}")
        lines.append(f"  inbound: {_preview(seed.inbound_context.get('text'), 100)}")
        lines.append(f"  draft: {preview}")
        lines.append(f"  linked_knowledge_ids: {linked_text}")
    return "\n".join(lines)


def _empty_export(
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: tuple[str, ...],
) -> ReplyKnowledgeSeedExport:
    return ReplyKnowledgeSeedExport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        summary={
            "seed_count": 0,
            "candidate_count": 0,
            "excluded_by_quality_flag": 0,
            "excluded_by_outcome": 0,
            "missing_tables": len(missing_tables),
        },
        seeds=(),
        missing_tables=missing_tables,
    )


def _load_reply_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    min_quality: float,
) -> list[dict[str, Any]]:
    columns = schema.get("reply_queue", set())
    selected = [
        _column_expr(columns, "id"),
        _column_expr(columns, "inbound_tweet_id"),
        _column_expr(columns, "platform"),
        _column_expr(columns, "inbound_author_handle"),
        _column_expr(columns, "inbound_author_id"),
        _column_expr(columns, "inbound_text"),
        _column_expr(columns, "inbound_url"),
        _column_expr(columns, "inbound_cid"),
        _column_expr(columns, "our_tweet_id"),
        _column_expr(columns, "our_platform_id"),
        _column_expr(columns, "our_content_id"),
        _column_expr(columns, "our_post_text"),
        _column_expr(columns, "draft_text"),
        _column_expr(columns, "intent"),
        _column_expr(columns, "priority"),
        _column_expr(columns, "relationship_context"),
        _column_expr(columns, "quality_score"),
        _column_expr(columns, "quality_flags"),
        _column_expr(columns, "status"),
        _column_expr(columns, "posted_tweet_id"),
        _column_expr(columns, "posted_platform_id"),
        _column_expr(columns, "platform_metadata"),
        _column_expr(columns, "detected_at"),
        _column_expr(columns, "reviewed_at"),
        _column_expr(columns, "posted_at"),
    ]
    time_expr = _coalesce_expr(columns, ("posted_at", "reviewed_at", "detected_at"))
    cursor = conn.execute(
        f"""SELECT {', '.join(selected)}
            FROM reply_queue
            WHERE quality_score IS NOT NULL
              AND quality_score >= ?
              AND draft_text IS NOT NULL
              AND TRIM(draft_text) != ''
              AND datetime({time_expr}) >= datetime(?)
            ORDER BY quality_score DESC, datetime({time_expr}) DESC, id ASC""",
        (min_quality, cutoff.isoformat()),
    )
    return [dict(row) for row in cursor.fetchall()]


def _load_review_events(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    reply_ids: list[int],
) -> dict[int, tuple[dict[str, Any], ...]]:
    if not reply_ids or "reply_review_events" not in schema:
        return {}
    placeholders = ", ".join("?" for _ in reply_ids)
    cursor = conn.execute(
        f"""SELECT reply_queue_id, event_type, actor, old_status, new_status, notes, created_at, id
            FROM reply_review_events
            WHERE reply_queue_id IN ({placeholders})
            ORDER BY reply_queue_id ASC, datetime(created_at) ASC, id ASC""",
        reply_ids,
    )
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in cursor.fetchall():
        item = dict(row)
        grouped[int(item["reply_queue_id"])].append(item)
    return {reply_id: tuple(items) for reply_id, items in grouped.items()}


def _load_reply_knowledge_links(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    reply_ids: list[int],
) -> dict[int, tuple[dict[str, Any], ...]]:
    if not reply_ids or "reply_knowledge_links" not in schema:
        return {}
    placeholders = ", ".join("?" for _ in reply_ids)
    cursor = conn.execute(
        f"""SELECT reply_queue_id, knowledge_id, relevance_score, created_at
            FROM reply_knowledge_links
            WHERE reply_queue_id IN ({placeholders})
            ORDER BY reply_queue_id ASC, relevance_score DESC, knowledge_id ASC""",
        reply_ids,
    )
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in cursor.fetchall():
        item = dict(row)
        grouped[int(item["reply_queue_id"])].append(item)
    return {reply_id: tuple(items) for reply_id, items in grouped.items()}


def _seed_from_row(
    row: dict[str, Any],
    *,
    flags: tuple[str, ...],
    events: tuple[dict[str, Any], ...],
    links: tuple[dict[str, Any], ...],
) -> ReplyKnowledgeSeed:
    reply_id = int(row["id"])
    quality_score = float(row["quality_score"])
    activity_at = row.get("posted_at") or row.get("reviewed_at") or row.get("detected_at")
    linked_ids = tuple(int(link["knowledge_id"]) for link in links if link.get("knowledge_id") is not None)
    return ReplyKnowledgeSeed(
        source_reply_id=reply_id,
        source_id=f"reply_queue:{reply_id}",
        source_type=SUGGESTED_SOURCE_TYPE,
        author_handle=row.get("inbound_author_handle"),
        inbound_context={
            "platform": row.get("platform") or "x",
            "inbound_tweet_id": row.get("inbound_tweet_id"),
            "inbound_author_id": row.get("inbound_author_id"),
            "inbound_url": row.get("inbound_url"),
            "inbound_cid": row.get("inbound_cid"),
            "text": row.get("inbound_text") or "",
            "our_tweet_id": row.get("our_tweet_id"),
            "our_platform_id": row.get("our_platform_id"),
            "our_content_id": row.get("our_content_id"),
            "our_post_text": row.get("our_post_text"),
        },
        draft_text=str(row.get("draft_text") or "").strip(),
        quality_score=quality_score,
        quality_flags=flags,
        metadata={
            "status": row.get("status"),
            "intent": row.get("intent"),
            "priority": row.get("priority"),
            "detected_at": row.get("detected_at"),
            "reviewed_at": row.get("reviewed_at"),
            "posted_at": row.get("posted_at"),
            "activity_at": activity_at,
            "posted_tweet_id": row.get("posted_tweet_id"),
            "posted_platform_id": row.get("posted_platform_id"),
            "relationship_context": _parse_json_object(row.get("relationship_context")),
            "platform_metadata": _parse_json_object(row.get("platform_metadata")),
            "review_events": [_event_summary(event) for event in events],
            "linked_knowledge_ids": list(linked_ids),
            "reply_knowledge_links": [_link_summary(link) for link in links],
        },
    )


def _is_qualified_outcome(
    row: dict[str, Any],
    events: tuple[dict[str, Any], ...],
) -> bool:
    status = str(row.get("status") or "").lower()
    if status in QUALIFYING_STATUSES:
        return True
    for event in events:
        event_type = str(event.get("event_type") or "").lower()
        new_status = str(event.get("new_status") or "").lower()
        if event_type in QUALIFYING_EVENT_TYPES or new_status in QUALIFYING_STATUSES:
            return True
    return False


def _event_summary(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_type": event.get("event_type"),
        "actor": event.get("actor"),
        "old_status": event.get("old_status"),
        "new_status": event.get("new_status"),
        "notes": event.get("notes"),
        "created_at": event.get("created_at"),
    }


def _link_summary(link: dict[str, Any]) -> dict[str, Any]:
    return {
        "knowledge_id": link.get("knowledge_id"),
        "relevance_score": link.get("relevance_score"),
        "created_at": link.get("created_at"),
    }


def _parse_quality_flags(flags_json: Any) -> tuple[str, ...]:
    if not flags_json:
        return ()
    try:
        parsed = json.loads(flags_json) if isinstance(flags_json, str) else flags_json
    except (TypeError, json.JSONDecodeError):
        parsed = [str(flags_json)]
    if not isinstance(parsed, list):
        return ()
    return tuple(sorted({str(item).strip().lower() for item in parsed if str(item).strip()}))


def _parse_json_object(value: Any) -> dict[str, Any] | None:
    if not value:
        return None
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return {"raw": str(value)}
    return parsed if isinstance(parsed, dict) else {"value": parsed}


def _preview(value: Any, limit: int) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _column_expr(columns: set[str], name: str) -> str:
    if name in columns:
        return name
    return f"NULL AS {name}"


def _coalesce_expr(columns: set[str], names: tuple[str, ...]) -> str:
    available = [name for name in names if name in columns]
    if not available:
        return "'1970-01-01T00:00:00+00:00'"
    if len(available) == 1:
        return available[0]
    return f"COALESCE({', '.join(available)})"


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    table_rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table'"
    ).fetchall()
    schema: dict[str, set[str]] = {}
    for row in table_rows:
        table = row["name"] if isinstance(row, sqlite3.Row) else row[0]
        schema[table] = {column[1] for column in conn.execute(f"PRAGMA table_info({table})")}
    return schema


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
