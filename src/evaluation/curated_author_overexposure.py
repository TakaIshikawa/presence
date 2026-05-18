"""Detect overexposure to the same curated author/account."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any
from urllib.parse import urlparse


DEFAULT_DAYS = 30
DEFAULT_SHARE_THRESHOLD = 0.5
DEFAULT_MIN_ITEMS = 3
DEFAULT_LIMIT = 50
UNKNOWN_AUTHOR = "(unknown author)"
AUTHOR_KEYS = (
    "author",
    "account",
    "handle",
    "author_handle",
    "username",
    "source_author",
    "curated_author",
)
URL_KEYS = ("source_url", "url", "canonical_url", "published_url", "author_url")
TIMESTAMP_KEYS = ("effective_at", "published_at", "sent_at", "created_at", "ingested_at")
URL_RE = re.compile(r"https?://[^\s<>)\"']+")
HANDLE_RE = re.compile(r"(?<![A-Za-z0-9_])@([A-Za-z0-9_][A-Za-z0-9_.-]{1,30})")
UNKNOWN_VALUES = {"", "unknown", "n/a", "none", "null", "-"}


def build_curated_author_overexposure_report(
    rows: list[dict[str, Any]],
    *,
    days: int = DEFAULT_DAYS,
    share_threshold: float = DEFAULT_SHARE_THRESHOLD,
    min_items: int = DEFAULT_MIN_ITEMS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build an overexposure report from in-memory content/citation rows."""
    if days <= 0:
        raise ValueError("days must be positive")
    if not 0 <= share_threshold <= 1:
        raise ValueError("share_threshold must be between 0 and 1")
    if min_items <= 0:
        raise ValueError("min_items must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    items = [_item(row) for row in rows]
    items = [item for item in items if _in_window(item["effective_at"], cutoff)]
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        grouped[item["author"]].append(item)

    total = len(items)
    authors = []
    for author, author_items in grouped.items():
        share = round(len(author_items) / total, 3) if total else 0.0
        authors.append(
            {
                "author": author,
                "display_author": _display_author(author, author_items),
                "item_count": len(author_items),
                "exposure_share": share,
                "source_types": sorted({item["source_type"] for item in author_items}),
                "affected_content_ids": _affected_content_ids(author_items),
                "first_seen_at": _first_timestamp(author_items),
                "last_seen_at": _last_timestamp(author_items),
            }
        )
    authors.sort(key=lambda row: (-row["exposure_share"], -row["item_count"], row["author"]))

    flagged = [
        {
            **row,
            "threshold": share_threshold,
            "suggested_alternates": _suggested_alternates(row["author"], authors),
        }
        for row in authors
        if row["author"] != UNKNOWN_AUTHOR
        and row["item_count"] >= min_items
        and row["exposure_share"] >= share_threshold
    ][:limit]

    return {
        "artifact_type": "curated_author_overexposure",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "days": days,
            "cutoff": cutoff.isoformat(),
            "share_threshold": share_threshold,
            "min_items": min_items,
            "limit": limit,
        },
        "totals": {
            "row_count": len(items),
            "author_count": len(authors),
            "overexposed_author_count": len(flagged),
            "unknown_author_count": len(grouped.get(UNKNOWN_AUTHOR, [])),
        },
        "overexposed_authors": flagged,
        "author_summary": authors[:limit],
        "empty_state": {
            "is_empty": not flagged,
            "message": "No curated author overexposure found." if not flagged else None,
        },
    }


def build_curated_author_overexposure_report_from_db(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    share_threshold: float = DEFAULT_SHARE_THRESHOLD,
    min_items: int = DEFAULT_MIN_ITEMS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Load curated author exposure rows from SQLite and build the report."""
    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    rows = _load_db_rows(conn, schema, cutoff)
    report = build_curated_author_overexposure_report(
        rows,
        days=days,
        share_threshold=share_threshold,
        min_items=min_items,
        limit=limit,
        now=generated_at,
    )
    report["missing_tables"] = [
        table
        for table in ("knowledge", "generated_content", "newsletter_sends")
        if table not in schema
    ]
    return report


def format_curated_author_overexposure_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_curated_author_overexposure_text(report: dict[str, Any]) -> str:
    filters = report["filters"]
    totals = report["totals"]
    lines = [
        "Curated Author Overexposure",
        f"Generated: {report['generated_at']}",
        (
            f"Window: {filters['days']}d share_threshold={filters['share_threshold']} "
            f"min_items={filters['min_items']}"
        ),
        (
            f"Totals: rows={totals['row_count']} authors={totals['author_count']} "
            f"overexposed={totals['overexposed_author_count']} unknown={totals['unknown_author_count']}"
        ),
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["overexposed_authors"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.append("Overexposed authors:")
    for row in report["overexposed_authors"]:
        lines.append(
            f"- {row['author']}: share={row['exposure_share']:.3f} count={row['item_count']} "
            f"affected={_format_ids(row['affected_content_ids'])} "
            f"alternates={_format_ids(row['suggested_alternates'])}"
        )
    return "\n".join(lines)


def normalize_curated_author(row: dict[str, Any]) -> str:
    """Normalize author/account identifiers from fields, metadata, URLs, and handles."""
    metadata = _json_object(_first(row, "metadata", "raw_metadata", "source_metadata"))
    for value in _candidate_values(row, metadata):
        normalized = _normalize_identifier(value)
        if normalized:
            return normalized
    text = " ".join(_text(_first(row, "content", "body", "text", "subject", "title")).split())
    handle = HANDLE_RE.search(text)
    if handle:
        normalized = _normalize_identifier(handle.group(1))
        if normalized:
            return normalized
    for url in URL_RE.findall(text):
        normalized = _normalize_identifier(url)
        if normalized:
            return normalized
    return UNKNOWN_AUTHOR


def _load_db_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    cutoff: datetime,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if "knowledge" in schema:
        rows.extend(_load_knowledge_rows(conn, schema["knowledge"], cutoff))
    if "generated_content" in schema:
        rows.extend(_load_generated_content_rows(conn, schema["generated_content"], cutoff))
    if "newsletter_sends" in schema:
        rows.extend(_load_newsletter_rows(conn, schema["newsletter_sends"], cutoff))
    return rows


def _load_knowledge_rows(conn: sqlite3.Connection, columns: set[str], cutoff: datetime) -> list[dict[str, Any]]:
    timestamp = _timestamp_expr(columns)
    selected = [
        _col(columns, "id", "content_id"),
        _col(columns, "source_type", "source_type", "'knowledge'"),
        _col(columns, "author", "author"),
        _col(columns, "source_url", "source_url"),
        _col(columns, "content", "content"),
        _col(columns, "metadata", "metadata"),
        f"{timestamp} AS effective_at",
    ]
    where = []
    params: list[Any] = []
    if timestamp != "NULL":
        where.append(f"{timestamp} >= ?")
        params.append(cutoff.isoformat())
    if "source_type" in columns:
        where.append("LOWER(COALESCE(source_type, '')) LIKE 'curated%'")
    if "approved" in columns:
        where.append("COALESCE(approved, 0) = 1")
    rows = conn.execute(
        f"SELECT {', '.join(selected)} FROM knowledge {_where(where)} ORDER BY effective_at DESC, content_id ASC",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _load_generated_content_rows(conn: sqlite3.Connection, columns: set[str], cutoff: datetime) -> list[dict[str, Any]]:
    timestamp = _timestamp_expr(columns)
    selected = [
        _col(columns, "id", "content_id"),
        _col(columns, "content_type", "source_type", "'generated_content'"),
        "NULL AS author",
        _col(columns, "published_url", "source_url"),
        _col(columns, "content", "content"),
        "NULL AS metadata",
        f"{timestamp} AS effective_at",
    ]
    where = []
    params: list[Any] = []
    if timestamp != "NULL":
        where.append(f"{timestamp} >= ?")
        params.append(cutoff.isoformat())
    rows = conn.execute(
        f"SELECT {', '.join(selected)} FROM generated_content {_where(where)} ORDER BY effective_at DESC, content_id ASC",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _load_newsletter_rows(conn: sqlite3.Connection, columns: set[str], cutoff: datetime) -> list[dict[str, Any]]:
    timestamp = _timestamp_expr(columns, "sent_at", "created_at")
    selected = [
        _col(columns, "id", "content_id"),
        "'newsletter' AS source_type",
        "NULL AS author",
        "NULL AS source_url",
        _col(columns, "subject", "content"),
        _col(columns, "metadata", "metadata"),
        _col(columns, "source_content_ids", "source_content_ids"),
        f"{timestamp} AS effective_at",
    ]
    where = []
    params: list[Any] = []
    if timestamp != "NULL":
        where.append(f"{timestamp} >= ?")
        params.append(cutoff.isoformat())
    if "status" in columns:
        where.append("LOWER(COALESCE(status, 'sent')) IN ('sent', 'published')")
    rows = conn.execute(
        f"SELECT {', '.join(selected)} FROM newsletter_sends {_where(where)} ORDER BY effective_at DESC, content_id ASC",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _item(row: dict[str, Any]) -> dict[str, Any]:
    author = normalize_curated_author(row)
    effective_at = _parse_timestamp(_first(row, *TIMESTAMP_KEYS))
    return {
        "author": author,
        "display_author": _display_value(row, author),
        "content_id": _text(_first(row, "content_id", "id", "knowledge_id", "newsletter_send_id")) or "unknown",
        "source_type": _text(_first(row, "source_type", "content_type", "type")) or "unknown",
        "effective_at": effective_at.isoformat() if effective_at else None,
    }


def _candidate_values(row: dict[str, Any], metadata: dict[str, Any]) -> list[Any]:
    values = [_first(row, *AUTHOR_KEYS)]
    values.extend(metadata.get(key) for key in AUTHOR_KEYS)
    values.extend(_items(metadata.get(key)) for key in ("authors", "accounts", "handles"))
    values.extend(_first(row, *URL_KEYS) for _ in [None])
    values.extend(metadata.get(key) for key in URL_KEYS)
    flat: list[Any] = []
    for value in values:
        flat.extend(value if isinstance(value, list) else [value])
    return flat


def _normalize_identifier(value: Any) -> str | None:
    text = _text(value)
    if not text:
        return None
    if text.startswith("{") or text.startswith("["):
        parsed = _json_object(text)
        for key in AUTHOR_KEYS + URL_KEYS:
            normalized = _normalize_identifier(parsed.get(key))
            if normalized:
                return normalized
    if text.startswith("@"):
        text = text[1:]
    parsed_url = urlparse(text)
    if parsed_url.scheme and parsed_url.netloc:
        host = parsed_url.netloc.lower().removeprefix("www.")
        parts = [part for part in parsed_url.path.split("/") if part]
        if host in {"x.com", "twitter.com", "bsky.app"} and parts:
            if host == "bsky.app" and parts[0] == "profile" and len(parts) > 1:
                text = parts[1]
            else:
                text = parts[0]
        else:
            text = host
    text = text.split("?", 1)[0].split("#", 1)[0].strip().strip("/")
    normalized = " ".join(text.casefold().split())
    if normalized.startswith("@"):
        normalized = normalized[1:]
    return None if normalized in UNKNOWN_VALUES else normalized


def _display_value(row: dict[str, Any], author: str) -> str:
    value = _first(row, *AUTHOR_KEYS)
    return _text(value) or author


def _display_author(author: str, items: list[dict[str, Any]]) -> str:
    values = sorted(
        {
            item["display_author"]
            for item in items
            if item.get("display_author") and item["display_author"] != author
        },
        key=lambda value: (len(value), value.casefold()),
    )
    return values[0] if values else author


def _suggested_alternates(author: str, authors: list[dict[str, Any]]) -> list[str]:
    alternates = [
        row["author"]
        for row in authors
        if row["author"] not in {author, UNKNOWN_AUTHOR}
    ]
    return sorted(alternates, key=lambda value: next(row["exposure_share"] for row in authors if row["author"] == value))[:3]


def _affected_content_ids(items: list[dict[str, Any]]) -> list[str]:
    return sorted({item["content_id"] for item in items}, key=lambda value: (not value.isdigit(), value))


def _first_timestamp(items: list[dict[str, Any]]) -> str | None:
    values = sorted(item["effective_at"] for item in items if item["effective_at"])
    return values[0] if values else None


def _last_timestamp(items: list[dict[str, Any]]) -> str | None:
    values = sorted(item["effective_at"] for item in items if item["effective_at"])
    return values[-1] if values else None


def _in_window(value: str | None, cutoff: datetime) -> bool:
    parsed = _parse_timestamp(value)
    return True if parsed is None else parsed >= cutoff


def _timestamp_expr(columns: set[str], *preferred: str) -> str:
    names = preferred or TIMESTAMP_KEYS
    existing = [name for name in names if name in columns]
    if not existing:
        existing = [name for name in TIMESTAMP_KEYS if name in columns]
    if not existing:
        return "NULL"
    return existing[0] if len(existing) == 1 else f"COALESCE({', '.join(existing)})"


def _col(columns: set[str], column: str, output: str, fallback: str = "NULL") -> str:
    return f"{column} AS {output}" if column in columns else f"{fallback} AS {output}"


def _where(clauses: list[str]) -> str:
    return f"WHERE {' AND '.join(clauses)}" if clauses else ""


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {
        row["name"]: {info["name"] for info in conn.execute(f"PRAGMA table_info({row['name']})")}
        for row in rows
    }


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(str(value or "{}"))
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _items(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return [value]
    return parsed if isinstance(parsed, list) else [parsed]


def _parse_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _utc(value)
    if not _text(value):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _first(row: dict[str, Any], *keys: str) -> Any:
    return next((row[key] for key in keys if key in row and row[key] not in (None, "")), None)


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _format_ids(values: list[str]) -> str:
    return ",".join(str(value) for value in values) if values else "-"
