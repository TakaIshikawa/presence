"""Local author history summaries for reply drafting and review."""

from __future__ import annotations

import json
import sqlite3
from collections import Counter
from typing import Any

DEFAULT_LIMIT = 5
DEFAULT_TEXT_LIMIT = 160


def truncate_text(text: str | None, max_len: int = DEFAULT_TEXT_LIMIT) -> str:
    """Truncate user-authored text deterministically for compact review context."""
    if not text:
        return ""
    if max_len < 4:
        raise ValueError("max_len must be at least 4")
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def build_reply_author_history(
    db_or_conn: Any,
    *,
    handle: str | None = None,
    author_id: str | None = None,
    limit: int = DEFAULT_LIMIT,
    text_limit: int = DEFAULT_TEXT_LIMIT,
) -> dict[str, Any]:
    """Summarize prior reply_queue interactions for a handle or author ID.

    Args:
        db_or_conn: storage.db.Database, sqlite3.Connection, or compatible object
            with a ``conn`` attribute.
        handle: Inbound author handle, with or without leading ``@``.
        author_id: Platform author ID/DID.
        limit: Maximum recent interaction rows and draft snippets to include.
        text_limit: Maximum characters for inbound and draft text snippets.

    Returns:
        A deterministic, JSON-serializable history context dictionary.
    """
    normalized_handle = _normalize_handle(handle) if handle else None
    normalized_author_id = author_id.strip() if author_id else None
    if not normalized_handle and not normalized_author_id:
        raise ValueError("handle or author_id is required")
    if limit <= 0:
        raise ValueError("limit must be positive")

    rows = _fetch_rows(
        _connection(db_or_conn),
        handle=normalized_handle,
        author_id=normalized_author_id,
    )
    status_counts = Counter((row.get("status") or "unknown") for row in rows)
    recent_rows = rows[:limit]

    return {
        "query": {
            "handle": normalized_handle,
            "author_id": normalized_author_id,
        },
        "matched_count": len(rows),
        "status_counts": dict(sorted(status_counts.items())),
        "last_interaction_timestamp": _last_interaction_timestamp(rows),
        "recent_interactions": [
            _interaction_item(row, text_limit=text_limit) for row in recent_rows
        ],
        "prior_draft_snippets": [
            {
                "id": row["id"],
                "status": row.get("status"),
                "detected_at": row.get("detected_at"),
                "draft_text": truncate_text(row.get("draft_text"), text_limit),
            }
            for row in recent_rows
            if row.get("draft_text")
        ],
        "relationship_highlights": _relationship_highlights(rows),
    }


def format_reply_author_history_text(history: dict[str, Any]) -> str:
    """Render an author history dictionary as compact review text."""
    query = history.get("query", {})
    identity = (
        f"@{query['handle']}"
        if query.get("handle")
        else f"author_id={query.get('author_id')}"
    )
    lines = [
        f"Reply history for {identity}",
        f"Matched interactions: {history.get('matched_count', 0)}",
        f"Status counts: {history.get('status_counts', {})}",
    ]
    last_seen = history.get("last_interaction_timestamp")
    if last_seen:
        lines.append(f"Last interaction: {last_seen}")

    highlights = history.get("relationship_highlights") or []
    if highlights:
        lines.append("Relationship highlights:")
        lines.extend(f"  - {highlight}" for highlight in highlights)

    interactions = history.get("recent_interactions") or []
    if interactions:
        lines.append("Recent interactions:")
        for item in interactions:
            author = item.get("author_handle") or "unknown"
            lines.append(
                f"  - [{item.get('detected_at')}] {item.get('status')} @{author}: "
                f"\"{item.get('inbound_text', '')}\""
            )
            draft = item.get("draft_text")
            if draft:
                lines.append(f"    Draft: \"{draft}\"")
    else:
        lines.append("No prior interactions found.")

    return "\n".join(lines)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    if isinstance(db_or_conn, sqlite3.Connection):
        return db_or_conn
    conn = getattr(db_or_conn, "conn", None)
    if conn is None:
        raise TypeError("db_or_conn must be a sqlite3 connection or Database-like object")
    return conn


def _fetch_rows(
    conn: sqlite3.Connection,
    *,
    handle: str | None,
    author_id: str | None,
) -> list[dict[str, Any]]:
    filters = []
    params: list[Any] = []
    if handle:
        filters.append("LOWER(LTRIM(inbound_author_handle, '@')) = ?")
        params.append(handle)
    if author_id:
        filters.append("inbound_author_id = ?")
        params.append(author_id)

    cursor = conn.execute(
        f"""SELECT id, inbound_tweet_id, platform, inbound_author_handle,
                  inbound_author_id, inbound_text, our_tweet_id, our_post_text,
                  draft_text, relationship_context, status, detected_at,
                  reviewed_at, posted_at
             FROM reply_queue
             WHERE {' OR '.join(filters)}
             ORDER BY COALESCE(posted_at, reviewed_at, detected_at, '') DESC,
                      id DESC""",
        params,
    )
    return [dict(row) for row in cursor.fetchall()]


def _normalize_handle(handle: str | None) -> str | None:
    if handle is None:
        return None
    return handle.strip().lstrip("@").lower() or None


def _interaction_item(row: dict[str, Any], *, text_limit: int) -> dict[str, Any]:
    return {
        "id": row["id"],
        "status": row.get("status"),
        "platform": row.get("platform") or "x",
        "author_handle": _normalize_handle(row.get("inbound_author_handle")),
        "author_id": row.get("inbound_author_id"),
        "inbound_tweet_id": row.get("inbound_tweet_id"),
        "detected_at": row.get("detected_at"),
        "reviewed_at": row.get("reviewed_at"),
        "posted_at": row.get("posted_at"),
        "inbound_text": truncate_text(row.get("inbound_text"), text_limit),
        "draft_text": truncate_text(row.get("draft_text"), text_limit),
    }


def _last_interaction_timestamp(rows: list[dict[str, Any]]) -> str | None:
    timestamps = [
        timestamp
        for row in rows
        for timestamp in (row.get("posted_at"), row.get("reviewed_at"), row.get("detected_at"))
        if timestamp
    ]
    return max(timestamps) if timestamps else None


def _relationship_highlights(rows: list[dict[str, Any]]) -> list[str]:
    highlights: list[str] = []
    seen = set()
    for row in rows:
        highlight = _relationship_highlight(row.get("relationship_context"))
        if highlight and highlight not in seen:
            seen.add(highlight)
            highlights.append(highlight)
    return highlights


def _relationship_highlight(relationship_context_json: str | None) -> str | None:
    if not relationship_context_json:
        return None
    try:
        context = json.loads(relationship_context_json)
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(context, dict):
        return None

    parts = []
    if context.get("engagement_stage") is not None:
        parts.append(
            f"{context.get('stage_name', '?')} (stage {context['engagement_stage']})"
        )
    if context.get("dunbar_tier") is not None:
        parts.append(f"{context.get('tier_name', '?')} (tier {context['dunbar_tier']})")
    if context.get("relationship_strength") is not None:
        try:
            parts.append(f"strength: {float(context['relationship_strength']):.2f}")
        except (TypeError, ValueError):
            pass
    return " | ".join(parts) if parts else None
