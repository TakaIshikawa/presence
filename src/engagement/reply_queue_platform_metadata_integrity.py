"""Audit reply queue platform metadata integrity."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
REQUIRED_COLUMNS = {"id", "platform", "platform_metadata"}
X_IDENTIFIER_KEYS = ("tweet_id", "tweet", "id", "post_id", "reply_to_tweet_id")
BLUESKY_IDENTIFIER_KEYS = ("uri", "cid", "at_uri")
PLATFORM_KEYS = ("platform", "network", "service")
FALLBACK_KEYS = ("tweet_id", "post_id", "platform_post_id", "reply_to_id", "uri", "cid", "at_uri")


def build_reply_queue_platform_metadata_integrity_report(
    db_or_conn: Any,
    *,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a reply queue platform metadata integrity report."""
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    columns = schema.get("reply_queue")
    if columns is None:
        return _report(generated_at, limit, [], 0, ["reply_queue"], {})
    missing = sorted(REQUIRED_COLUMNS - columns)
    if missing:
        return _report(generated_at, limit, [], 0, [], {"reply_queue": missing})

    rows = _load_rows(conn, columns)
    items: list[dict[str, Any]] = []
    for row in rows:
        items.extend(_row_gaps(row))
    items.sort(key=lambda item: (_gap_rank(item["gap_type"]), item["platform"], _int_or_text(item["reply_id"])))
    return _report(generated_at, limit, items, len(rows), [], {})


def format_reply_queue_platform_metadata_integrity_json(report: dict[str, Any]) -> str:
    """Render the report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_queue_platform_metadata_integrity_text(report: dict[str, Any]) -> str:
    """Render the report as readable terminal text."""
    summary = report["summary"]
    lines = [
        "Reply Queue Platform Metadata Integrity",
        f"Generated: {report['generated_at']}",
        f"Limit: {report['filters']['limit']}",
        f"Totals: replies={summary['reply_count']} gaps={summary['gap_count']} shown={summary['shown_count']}",
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report.get("missing_columns"):
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["items"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "reply_id | platform | gap_type"])
    for item in report["items"]:
        lines.append(f"{item['reply_id']} | {item['platform']} | {item['gap_type']}")
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    select_parts = ["id AS reply_id", "platform", "platform_metadata"]
    for column in FALLBACK_KEYS:
        select_parts.append(f"{column}" if column in columns else f"NULL AS {column}")
    rows = conn.execute(f"SELECT {', '.join(select_parts)} FROM reply_queue ORDER BY id ASC").fetchall()
    return [dict(row) for row in rows]


def _row_gaps(row: dict[str, Any]) -> list[dict[str, Any]]:
    platform = _normalize_platform(row.get("platform"))
    raw = row.get("platform_metadata")
    if raw is None or _clean(raw) == "":
        return [_item(row, platform, "missing_metadata")]
    metadata = _parse_metadata(raw)
    if metadata is None:
        return [_item(row, platform, "malformed_metadata")]

    gaps: list[dict[str, Any]] = []
    metadata_platform = _metadata_platform(metadata)
    if metadata_platform and metadata_platform != platform:
        gaps.append(_item(row, platform, "platform_mismatch", metadata_platform=metadata_platform))
    elif _shape_platform(metadata) and _shape_platform(metadata) != platform:
        gaps.append(_item(row, platform, "platform_mismatch", metadata_platform=_shape_platform(metadata)))
    if gaps:
        return gaps
    if platform in {"x", "twitter"} and not _has_identifier(row, metadata, X_IDENTIFIER_KEYS, ("tweet_id", "post_id", "platform_post_id", "reply_to_id")):
        gaps.append(_item(row, platform, "missing_native_identifier"))
    if platform == "bluesky" and not _has_identifier(row, metadata, BLUESKY_IDENTIFIER_KEYS, ("uri", "cid", "at_uri")):
        gaps.append(_item(row, platform, "missing_native_identifier"))
    return gaps


def _parse_metadata(raw: Any) -> dict[str, Any] | None:
    if isinstance(raw, dict):
        return raw
    try:
        parsed = json.loads(str(raw))
    except (TypeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _metadata_platform(metadata: dict[str, Any]) -> str:
    for key in PLATFORM_KEYS:
        value = _normalize_platform(metadata.get(key))
        if value:
            return value
    return ""


def _shape_platform(metadata: dict[str, Any]) -> str:
    has_x = any(_has_metadata_value(metadata, key) for key in X_IDENTIFIER_KEYS)
    has_bluesky = any(_has_metadata_value(metadata, key) for key in BLUESKY_IDENTIFIER_KEYS)
    if has_x and not has_bluesky:
        return "x"
    if has_bluesky and not has_x:
        return "bluesky"
    return ""


def _has_identifier(row: dict[str, Any], metadata: dict[str, Any], metadata_keys: tuple[str, ...], fallback_keys: tuple[str, ...]) -> bool:
    return any(_has_metadata_value(metadata, key) for key in metadata_keys) or any(_clean(row.get(key)) for key in fallback_keys)


def _has_metadata_value(metadata: dict[str, Any], key: str) -> bool:
    value = metadata.get(key)
    if isinstance(value, dict):
        return any(_clean(child) for child in value.values())
    return _clean(value) != ""


def _item(row: dict[str, Any], platform: str, gap_type: str, **extra: Any) -> dict[str, Any]:
    return {
        "reply_id": row.get("reply_id"),
        "platform": platform,
        "gap_type": gap_type,
        **extra,
    }


def _report(
    generated_at: datetime,
    limit: int,
    items: list[dict[str, Any]],
    reply_count: int,
    missing_tables: list[str],
    missing_columns: dict[str, list[str]],
) -> dict[str, Any]:
    shown = items[:limit]
    return {
        "artifact_type": "reply_queue_platform_metadata_integrity",
        "generated_at": generated_at.isoformat(),
        "filters": {"limit": limit},
        "summary": {
            "reply_count": reply_count,
            "gap_count": len(items),
            "shown_count": len(shown),
            "by_gap_type": _counts(items, "gap_type"),
        },
        "items": shown,
        "missing_tables": sorted(missing_tables),
        "missing_columns": {table: sorted(columns) for table, columns in sorted(missing_columns.items())},
        "empty_state": {
            "is_empty": not items,
            "message": "No reply queue platform metadata integrity gaps found." if not items else None,
        },
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {str(row[0]): {str(column[1]) for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _counts(items: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        value = _clean(item.get(key))
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))


def _gap_rank(gap_type: str) -> int:
    return {
        "missing_metadata": 0,
        "malformed_metadata": 1,
        "platform_mismatch": 2,
        "missing_native_identifier": 3,
    }.get(gap_type, 99)


def _normalize_platform(value: Any) -> str:
    platform = _clean(value).lower()
    return "x" if platform in {"twitter", "tweet"} else platform


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _int_or_text(value: Any) -> tuple[int, Any]:
    try:
        return (0, int(value))
    except (TypeError, ValueError):
        return (1, "" if value is None else str(value))


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
