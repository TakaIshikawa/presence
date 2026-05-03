"""Build incident-focused publication replay bundles."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any


BUNDLE_VERSION = 2
DEFAULT_ENGAGEMENT_LIMIT = 5
REDACTED = "[REDACTED]"

URL_RE = re.compile(r"https?://[^\s)>\]\"']+", re.IGNORECASE)
HANDLE_RE = re.compile(r"(?<![\w.])@[A-Za-z0-9_]{1,30}\b")

FREE_TEXT_KEYS = {
    "annotation_text",
    "body",
    "content",
    "error",
    "eval_feedback",
    "hold_reason",
    "image_alt_text",
    "image_prompt",
    "insight",
    "message",
    "note",
    "notes",
    "reason",
    "rationale",
    "replacement_text",
    "text",
    "title",
}
JSON_COLUMNS = {
    "metadata",
    "response_metadata",
    "source_activity_ids",
    "source_commits",
    "source_messages",
}


def build_publication_replay_bundle(
    db_or_conn: Any,
    *,
    content_id: int | None = None,
    queue_id: int | None = None,
    redact: bool = False,
    engagement_limit: int = DEFAULT_ENGAGEMENT_LIMIT,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    """Assemble a compact diagnostic bundle for one publication incident."""
    if (content_id is None) == (queue_id is None):
        raise ValueError("provide exactly one of content_id or queue_id")
    if content_id is not None and content_id <= 0:
        raise ValueError("content_id must be positive")
    if queue_id is not None and queue_id <= 0:
        raise ValueError("queue_id must be positive")
    if engagement_limit <= 0:
        raise ValueError("engagement_limit must be positive")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    resolved_queue = _fetch_queue_by_id(conn, schema, queue_id) if queue_id is not None else None
    if queue_id is not None and resolved_queue is None:
        raise ValueError(f"publish_queue id {queue_id} does not exist")
    resolved_content_id = int(resolved_queue["content_id"]) if resolved_queue else int(content_id)

    content = _fetch_content(conn, schema, resolved_content_id)
    if content is None:
        raise ValueError(f"generated_content id {resolved_content_id} does not exist")

    queue = resolved_queue or _fetch_latest_queue_for_content(conn, schema, resolved_content_id)
    attempts = _fetch_attempts(
        conn,
        schema,
        content_id=resolved_content_id,
        queue_id=int(queue_id) if queue_id is not None else None,
    )
    bundle = {
        "artifact_type": "publication_replay_bundle",
        "bundle_version": BUNDLE_VERSION,
        "generated_at": _aware(generated_at or datetime.now(timezone.utc)).isoformat(),
        "lookup": {
            "content_id": content_id,
            "queue_id": queue_id,
            "resolved_content_id": resolved_content_id,
            "resolved_queue_id": int(queue["id"]) if queue else None,
            "redacted": redact,
        },
        "generated_content": content,
        "publish_queue": queue,
        "publication_attempts": attempts,
        "content_variants": _fetch_rows(conn, schema, "content_variants", resolved_content_id),
        "content_publications": _fetch_rows(conn, schema, "content_publications", resolved_content_id),
        "content_knowledge_links_summary": _fetch_knowledge_summary(
            conn,
            schema,
            resolved_content_id,
        ),
        "recent_post_engagement": _fetch_recent_engagement(
            conn,
            schema,
            resolved_content_id,
            limit=engagement_limit,
        ),
    }
    return redact_publication_replay_bundle(bundle) if redact else _json_ready(bundle)


def format_publication_replay_bundle_json(bundle: dict[str, Any]) -> str:
    """Render a replay bundle as deterministic JSON."""
    return json.dumps(bundle, indent=2, sort_keys=True, default=str)


def format_publication_replay_bundle_text(bundle: dict[str, Any]) -> str:
    """Render a concise diagnostic view for terminal use."""
    lookup = bundle["lookup"]
    content = bundle["generated_content"]
    queue = bundle.get("publish_queue") or {}
    attempts = bundle["publication_attempts"]
    publications = bundle["content_publications"]
    knowledge = bundle["content_knowledge_links_summary"]
    engagement = bundle["recent_post_engagement"]
    lines = [
        "Publication Replay Bundle",
        f"Generated: {bundle['generated_at']}",
        (
            "Lookup: "
            f"content_id={lookup['resolved_content_id']} "
            f"queue_id={lookup['resolved_queue_id'] or '-'} "
            f"redacted={lookup['redacted']}"
        ),
        (
            "Content: "
            f"type={content.get('content_type', '-')} "
            f"published={content.get('published', '-')} "
            f"created_at={content.get('created_at', '-')}"
        ),
        (
            "Queue: "
            f"status={queue.get('status', '-')} "
            f"platform={queue.get('platform', '-')} "
            f"scheduled_at={queue.get('scheduled_at', '-')}"
        ),
        (
            "Counts: "
            f"attempts={len(attempts)} variants={len(bundle['content_variants'])} "
            f"publications={len(publications)} knowledge_links={knowledge['link_count']} "
            f"engagement_rows={len(engagement)}"
        ),
    ]
    if attempts:
        lines.append("")
        lines.append("Attempts:")
        for attempt in attempts:
            lines.append(
                "- "
                f"id={attempt.get('id')} platform={attempt.get('platform')} "
                f"success={attempt.get('success')} attempted_at={attempt.get('attempted_at')} "
                f"category={attempt.get('error_category') or '-'}"
            )
    return "\n".join(lines)


def redact_publication_replay_bundle(bundle: dict[str, Any]) -> dict[str, Any]:
    """Redact free-text URLs and handles while preserving structural fields."""
    return _redact_value(bundle, key_path=())


def _fetch_content(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_id: int,
) -> dict[str, Any] | None:
    if "generated_content" not in schema:
        raise ValueError("generated_content table does not exist")
    row = conn.execute("SELECT * FROM generated_content WHERE id = ?", (content_id,)).fetchone()
    if not row:
        return None
    content = dict(row)
    content.pop("content_embedding", None)
    return _json_ready(content)


def _fetch_queue_by_id(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    queue_id: int | None,
) -> dict[str, Any] | None:
    if queue_id is None:
        return None
    if "publish_queue" not in schema:
        raise ValueError("publish_queue table does not exist")
    row = conn.execute("SELECT * FROM publish_queue WHERE id = ?", (queue_id,)).fetchone()
    return _json_ready(dict(row)) if row else None


def _fetch_latest_queue_for_content(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_id: int,
) -> dict[str, Any] | None:
    columns = schema.get("publish_queue")
    if not columns or "content_id" not in columns:
        return None
    order = _order_by(columns, ("created_at", "scheduled_at", "id"), descending=True)
    row = conn.execute(
        f"SELECT * FROM publish_queue WHERE content_id = ? {order}",
        (content_id,),
    ).fetchone()
    return _json_ready(dict(row)) if row else None


def _fetch_attempts(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    content_id: int,
    queue_id: int | None,
) -> list[dict[str, Any]]:
    columns = schema.get("publication_attempts")
    if not columns:
        return []
    filters = ["content_id = ?"]
    params: list[Any] = [content_id]
    if queue_id is not None and "queue_id" in columns:
        filters.append("queue_id = ?")
        params.append(queue_id)
    order = _order_by(columns, ("attempted_at", "id"), descending=True)
    rows = conn.execute(
        f"SELECT * FROM publication_attempts WHERE {' AND '.join(filters)} {order}",
        params,
    ).fetchall()
    return [_coerce_booleans(_json_ready(dict(row))) for row in rows]


def _fetch_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    table: str,
    content_id: int,
) -> list[dict[str, Any]]:
    columns = schema.get(table)
    if not columns or "content_id" not in columns:
        return []
    order = _order_by(columns, ("updated_at", "created_at", "id"), descending=True)
    rows = conn.execute(
        f"SELECT * FROM {table} WHERE content_id = ? {order}",
        (content_id,),
    ).fetchall()
    return [_coerce_booleans(_json_ready(dict(row))) for row in rows]


def _fetch_knowledge_summary(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_id: int,
) -> dict[str, Any]:
    columns = schema.get("content_knowledge_links")
    if not columns or "content_id" not in columns:
        return {"link_count": 0, "knowledge_ids": [], "avg_relevance_score": None, "links": []}
    join_knowledge = "knowledge" in schema and "knowledge_id" in columns
    if join_knowledge:
        rows = conn.execute(
            """SELECT ckl.id, ckl.content_id, ckl.knowledge_id, ckl.relevance_score,
                      ckl.created_at, k.source_type, k.source_id, k.source_url,
                      k.author, k.license, k.approved, k.published_at
               FROM content_knowledge_links ckl
               LEFT JOIN knowledge k ON k.id = ckl.knowledge_id
               WHERE ckl.content_id = ?
               ORDER BY ckl.relevance_score DESC, ckl.id ASC""",
            (content_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT *
               FROM content_knowledge_links
               WHERE content_id = ?
               ORDER BY relevance_score DESC, id ASC""",
            (content_id,),
        ).fetchall()
    links = [_json_ready(dict(row)) for row in rows]
    scores = [
        float(link["relevance_score"])
        for link in links
        if link.get("relevance_score") is not None
    ]
    return {
        "link_count": len(links),
        "knowledge_ids": [link.get("knowledge_id") for link in links],
        "avg_relevance_score": round(sum(scores) / len(scores), 4) if scores else None,
        "links": links,
    }


def _fetch_recent_engagement(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_id: int,
    *,
    limit: int,
) -> list[dict[str, Any]]:
    columns = schema.get("post_engagement")
    if not columns or "content_id" not in columns:
        return []
    order = _order_by(columns, ("fetched_at", "id"), descending=True)
    rows = conn.execute(
        f"SELECT * FROM post_engagement WHERE content_id = ? {order} LIMIT ?",
        (content_id, limit),
    ).fetchall()
    return [_json_ready(dict(row)) for row in rows]


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    }
    return {
        table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        for table in tables
        if table
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _order_by(
    columns: set[str],
    preferred: tuple[str, ...],
    *,
    descending: bool = False,
) -> str:
    available = [column for column in preferred if column in columns]
    if not available:
        return ""
    direction = " DESC" if descending else ""
    return "ORDER BY " + ", ".join(f"{column}{direction}" for column in available)


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _json_ready(_parse_json_column(str(key), item)) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, bytes):
        return f"<{len(value)} bytes>"
    return value


def _parse_json_column(key: str, value: Any) -> Any:
    if key not in JSON_COLUMNS or not isinstance(value, str) or not value:
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _coerce_booleans(row: dict[str, Any]) -> dict[str, Any]:
    for key in ("success", "selected"):
        if key in row and row[key] is not None:
            row[key] = bool(row[key])
    return row


def _redact_value(value: Any, *, key_path: tuple[str, ...]) -> Any:
    if isinstance(value, dict):
        return {
            str(key): _redact_value(item, key_path=(*key_path, str(key)))
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact_value(item, key_path=key_path) for item in value]
    if isinstance(value, str) and _is_free_text_key(key_path[-1] if key_path else ""):
        return _redact_text(value)
    return value


def _is_free_text_key(key: str) -> bool:
    normalized = key.lower()
    return normalized in FREE_TEXT_KEYS or normalized.endswith("_text")


def _redact_text(value: str) -> str:
    return HANDLE_RE.sub(REDACTED, URL_RE.sub(REDACTED, value))


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
