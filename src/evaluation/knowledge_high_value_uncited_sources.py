"""Surface high-value knowledge rows that have not been cited."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 50
DEFAULT_LIKES_THRESHOLD = 50.0
DEFAULT_REPOSTS_THRESHOLD = 10.0
DEFAULT_BOOKMARKS_THRESHOLD = 20.0
DEFAULT_CLICKS_THRESHOLD = 100.0
DEFAULT_RECENT_CURATED_DAYS = 30
CURATED_RECENT_TYPES = {"curated_article", "curated_newsletter"}
METRIC_KEYS = {
    "likes": ("likes", "like_count", "favorite_count", "favorites"),
    "reposts": ("reposts", "repost_count", "retweets", "shares"),
    "bookmarks": ("bookmarks", "bookmark_count", "saves"),
    "clicks": ("clicks", "click_count", "url_clicks"),
}


def build_knowledge_high_value_uncited_sources_report(
    knowledge_rows: list[dict[str, Any]],
    cited_knowledge_ids: set[int] | None = None,
    *,
    likes_threshold: float = DEFAULT_LIKES_THRESHOLD,
    reposts_threshold: float = DEFAULT_REPOSTS_THRESHOLD,
    bookmarks_threshold: float = DEFAULT_BOOKMARKS_THRESHOLD,
    clicks_threshold: float = DEFAULT_CLICKS_THRESHOLD,
    recent_curated_days: int = DEFAULT_RECENT_CURATED_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    schema_gaps: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the uncited high-value knowledge report from raw rows."""
    if min(likes_threshold, reposts_threshold, bookmarks_threshold, clicks_threshold) < 0:
        raise ValueError("metric thresholds must be non-negative")
    if recent_curated_days <= 0:
        raise ValueError("recent_curated_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    cited = cited_knowledge_ids or set()
    rows = []
    for raw in knowledge_rows:
        row = _normalize_knowledge(raw)
        if row["knowledge_id"] in cited:
            continue
        reason = _high_value_reason(
            row,
            generated_at=generated_at,
            likes_threshold=likes_threshold,
            reposts_threshold=reposts_threshold,
            bookmarks_threshold=bookmarks_threshold,
            clicks_threshold=clicks_threshold,
            recent_curated_days=recent_curated_days,
        )
        if not reason:
            continue
        rows.append(
            {
                "knowledge_id": row["knowledge_id"],
                "source_type": row["source_type"],
                "author": row["author"],
                "title_or_excerpt": row["title_or_excerpt"],
                "published_at": row["published_at"],
                "high_value_reason": reason,
                "recommendation": _recommendation(row["source_type"]),
            }
        )

    rows.sort(key=lambda item: (item["source_type"], item["knowledge_id"]))
    shown = rows[:limit]
    return {
        "artifact_type": "knowledge_high_value_uncited_sources",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "likes_threshold": likes_threshold,
            "reposts_threshold": reposts_threshold,
            "bookmarks_threshold": bookmarks_threshold,
            "clicks_threshold": clicks_threshold,
            "recent_curated_days": recent_curated_days,
            "limit": limit,
        },
        "totals": {
            "knowledge_rows": len(knowledge_rows),
            "cited_knowledge_count": len(cited),
            "uncited_high_value_count": len(rows),
            "shown_count": len(shown),
        },
        "sources": shown,
        "schema_gaps": schema_gaps or {"missing_tables": [], "missing_columns": {}},
        "empty_state": {
            "is_empty": not rows,
            "message": "No high-value uncited knowledge sources found." if not rows else None,
        },
    }


def build_knowledge_high_value_uncited_sources_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    gaps = _schema_gaps(schema)
    rows = _load_knowledge(conn, schema) if "knowledge" in schema and not gaps["missing_columns"].get("knowledge") else []
    cited = _load_cited_ids(conn, schema)
    return build_knowledge_high_value_uncited_sources_report(rows, cited, schema_gaps=gaps, **kwargs)


def format_knowledge_high_value_uncited_sources_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_knowledge_high_value_uncited_sources_text(report: dict[str, Any]) -> str:
    lines = [
        "Knowledge High Value Uncited Sources",
        f"Generated: {report['generated_at']}",
        (
            "Totals: "
            f"knowledge_rows={report['totals']['knowledge_rows']} "
            f"uncited_high_value={report['totals']['uncited_high_value_count']}"
        ),
    ]
    gaps = report.get("schema_gaps", {})
    if gaps.get("missing_tables"):
        lines.append(f"Missing tables: {', '.join(gaps['missing_tables'])}")
    if gaps.get("missing_columns"):
        lines.append(
            "Missing columns: "
            + "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(gaps["missing_columns"].items()))
        )
    if not report["sources"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)

    lines.extend(["", "knowledge_id | source_type | published_at | reason | recommendation | title_or_excerpt"])
    for row in report["sources"]:
        lines.append(
            f"{row['knowledge_id']} | {row['source_type']} | {row['published_at'] or '-'} | "
            f"{row['high_value_reason']} | {row['recommendation']} | {row['title_or_excerpt']}"
        )
    return "\n".join(lines)


format_knowledge_high_value_uncited_sources_table = format_knowledge_high_value_uncited_sources_text


def _normalize_knowledge(row: dict[str, Any]) -> dict[str, Any]:
    metadata = _json_object(row.get("metadata"))
    content = _text(row.get("content"))
    title = _text(row.get("title") or metadata.get("title") or metadata.get("headline"))
    return {
        "knowledge_id": _int(row.get("knowledge_id") or row.get("id")) or 0,
        "source_type": _text(row.get("source_type")) or "unknown",
        "author": _text(row.get("author") or metadata.get("author")),
        "title_or_excerpt": title or _excerpt(content),
        "published_at": _text(row.get("published_at")),
        "metadata": metadata,
    }


def _high_value_reason(
    row: dict[str, Any],
    *,
    generated_at: datetime,
    likes_threshold: float,
    reposts_threshold: float,
    bookmarks_threshold: float,
    clicks_threshold: float,
    recent_curated_days: int,
) -> str | None:
    thresholds = {
        "likes": likes_threshold,
        "reposts": reposts_threshold,
        "bookmarks": bookmarks_threshold,
        "clicks": clicks_threshold,
    }
    for metric, threshold in thresholds.items():
        value = _metric(row["metadata"], METRIC_KEYS[metric])
        if value >= threshold:
            return f"{metric}>={_format_number(threshold)}"
    published_at = _parse_ts(row["published_at"])
    if row["source_type"] in CURATED_RECENT_TYPES and published_at:
        if generated_at - published_at <= timedelta(days=recent_curated_days):
            return f"recent_{row['source_type']}"
    return None


def _recommendation(source_type: str) -> str:
    if source_type in CURATED_RECENT_TYPES:
        return "Review for near-term citation in upcoming content."
    return "Review and link from the next relevant generated content."


def _load_knowledge(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    columns = schema["knowledge"]
    select = [
        _select(columns, ("id",), "id"),
        _select(columns, ("source_type",), "source_type"),
        _select(columns, ("author",), "author"),
        _select(columns, ("title",), "title"),
        _select(columns, ("content", "insight"), "content"),
        _select(columns, ("published_at", "created_at", "ingested_at"), "published_at"),
        _select(columns, ("metadata",), "metadata"),
    ]
    rows = conn.execute(f"SELECT {', '.join(select)} FROM knowledge ORDER BY id ASC").fetchall()
    return [dict(row) for row in rows]


def _load_cited_ids(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> set[int]:
    if "content_knowledge_links" not in schema or "knowledge_id" not in schema["content_knowledge_links"]:
        return set()
    rows = conn.execute("SELECT DISTINCT knowledge_id FROM content_knowledge_links WHERE knowledge_id IS NOT NULL").fetchall()
    return {_int(row[0]) for row in rows if _int(row[0]) is not None}


def _schema_gaps(schema: dict[str, set[str]]) -> dict[str, Any]:
    required = {"knowledge": {"id", "source_type", "content"}}
    missing_tables = [table for table in sorted(required) if table not in schema]
    missing_columns = {
        table: sorted(columns - schema.get(table, set()))
        for table, columns in required.items()
        if table in schema and columns - schema.get(table, set())
    }
    if "content_knowledge_links" in schema and "knowledge_id" not in schema["content_knowledge_links"]:
        missing_columns["content_knowledge_links"] = ["knowledge_id"]
    return {"missing_tables": missing_tables, "missing_columns": missing_columns}


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()}
    return {table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")} for table in tables}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _select(columns: set[str], candidates: tuple[str, ...], alias: str) -> str:
    for column in candidates:
        if column in columns:
            return f"{column} AS {alias}"
    return f"NULL AS {alias}"


def _metric(metadata: Any, keys: tuple[str, ...]) -> float:
    if not isinstance(metadata, dict):
        return 0.0
    for key in keys:
        value = _float(metadata.get(key))
        if value:
            return value
    for value in metadata.values():
        if isinstance(value, dict):
            nested = _metric(value, keys)
            if nested:
                return nested
    return 0.0


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_ts(value: Any) -> datetime | None:
    text = _text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _excerpt(value: str, limit: int = 100) -> str:
    text = " ".join(value.split())
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _format_number(value: float) -> str:
    return str(int(value)) if value.is_integer() else str(value)
