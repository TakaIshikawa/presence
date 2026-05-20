"""Report publication attempts with missing or incomplete response metadata."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
DEFAULT_PLATFORM = "all"
REQUIRED_COLUMNS = {"id", "content_id", "platform", "response_metadata"}
PLATFORM_IDENTIFIER_KEYS = {
    "x": ("tweet_id", "tweet", "id", "post_id", "platform_post_id"),
    "twitter": ("tweet_id", "tweet", "id", "post_id", "platform_post_id"),
    "bluesky": ("uri", "cid", "at_uri", "id", "post_id", "platform_post_id"),
}
GENERIC_IDENTIFIER_KEYS = ("id", "post_id", "platform_post_id", "url", "uri", "cid")


def build_publication_attempt_response_metadata_gaps_report(
    attempt_rows: list[dict[str, Any]],
    *,
    platform: str = DEFAULT_PLATFORM,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a response metadata gaps report from publication attempt rows."""
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    normalized_platform = _clean(platform).lower() or DEFAULT_PLATFORM
    items: list[dict[str, Any]] = []
    scanned = 0
    for row in attempt_rows:
        row_platform = _clean(row.get("platform")).lower() or "unknown"
        if normalized_platform != DEFAULT_PLATFORM and row_platform != normalized_platform:
            continue
        scanned += 1
        gap_type = _metadata_gap_type(row.get("response_metadata"), row_platform)
        if gap_type:
            items.append(_item(row, row_platform, gap_type))

    items.sort(key=lambda item: (_gap_rank(item["gap_type"]), item["platform"], _clean(item["attempted_at"]), _int_or_text(item["attempt_id"])))
    shown = items[:limit]
    return {
        "artifact_type": "publication_attempt_response_metadata_gaps",
        "generated_at": generated_at.isoformat(),
        "filters": {"platform": normalized_platform, "limit": limit},
        "summary": {
            "attempt_count": scanned,
            "gap_count": len(items),
            "shown_count": len(shown),
            "by_gap_type": _counts(items, "gap_type"),
            "by_platform": _counts(items, "platform"),
        },
        "items": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(columns) for table, columns in sorted((missing_columns or {}).items())},
        "empty_state": {
            "is_empty": not items,
            "message": "No publication attempt response metadata gaps found." if not items else None,
        },
    }


def build_publication_attempt_response_metadata_gaps_report_from_db(
    db_or_conn: Any,
    **kwargs: Any,
) -> dict[str, Any]:
    """Load publication attempts from SQLite and build the report."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    columns = schema.get("publication_attempts")
    if columns is None:
        return build_publication_attempt_response_metadata_gaps_report(
            [],
            missing_tables=["publication_attempts"],
            **kwargs,
        )
    missing = sorted(REQUIRED_COLUMNS - columns)
    if missing:
        return build_publication_attempt_response_metadata_gaps_report(
            [],
            missing_columns={"publication_attempts": missing},
            **kwargs,
        )
    return build_publication_attempt_response_metadata_gaps_report(_load_attempts(conn, columns), **kwargs)


def format_publication_attempt_response_metadata_gaps_json(report: dict[str, Any]) -> str:
    """Render the report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_publication_attempt_response_metadata_gaps_text(report: dict[str, Any]) -> str:
    """Render the report as readable terminal text."""
    summary = report["summary"]
    lines = [
        "Publication Attempt Response Metadata Gaps",
        f"Generated: {report['generated_at']}",
        f"Platform: {report['filters']['platform']}",
        f"Limit: {report['filters']['limit']}",
        f"Totals: attempts={summary['attempt_count']} gaps={summary['gap_count']} shown={summary['shown_count']}",
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report.get("missing_columns"):
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["items"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "attempt_id | content_id | platform | success | attempted_at | gap_type"])
    for item in report["items"]:
        lines.append(
            f"{item['attempt_id']} | {item['content_id']} | {item['platform']} | "
            f"{item['success']} | {item['attempted_at'] or '-'} | {item['gap_type']}"
        )
    return "\n".join(lines)


def _load_attempts(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    attempted_at = _column_expr(columns, "attempted_at", "created_at", "published_at", fallback="NULL")
    success_expr = _column_expr(columns, "success", fallback="NULL")
    status_expr = _column_expr(columns, "status", "outcome", "result", fallback="NULL")
    rows = conn.execute(
        f"""SELECT id AS attempt_id,
                  content_id,
                  platform,
                  {success_expr} AS success,
                  {attempted_at} AS attempted_at,
                  {status_expr} AS status,
                  response_metadata
           FROM publication_attempts
           ORDER BY {attempted_at} ASC, id ASC"""
    ).fetchall()
    return [dict(row) for row in rows]


def _metadata_gap_type(raw: Any, platform: str) -> str | None:
    if raw is None or _clean(raw) == "":
        return "missing_metadata"
    metadata = _parse_metadata(raw)
    if metadata is None:
        return "malformed_metadata"
    if not metadata:
        return "empty_metadata"
    keys = PLATFORM_IDENTIFIER_KEYS.get(platform, GENERIC_IDENTIFIER_KEYS)
    if not any(_has_value(metadata, key) for key in keys):
        return "missing_platform_identifier"
    return None


def _parse_metadata(raw: Any) -> dict[str, Any] | None:
    if isinstance(raw, dict):
        return raw
    try:
        parsed = json.loads(str(raw))
    except (TypeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _has_value(metadata: dict[str, Any], key: str) -> bool:
    value = metadata.get(key)
    if isinstance(value, dict):
        return any(_clean(child) for child in value.values())
    return _clean(value) != ""


def _item(row: dict[str, Any], platform: str, gap_type: str) -> dict[str, Any]:
    return {
        "attempt_id": row.get("attempt_id", row.get("id")),
        "content_id": row.get("content_id"),
        "platform": platform,
        "success": row.get("success"),
        "attempted_at": row.get("attempted_at"),
        "gap_type": gap_type,
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


def _column_expr(columns: set[str], *columns_to_try: str, fallback: str) -> str:
    for column in columns_to_try:
        if column in columns:
            return column
    return fallback


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
        "empty_metadata": 2,
        "missing_platform_identifier": 3,
    }.get(gap_type, 99)


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _int_or_text(value: Any) -> tuple[int, Any]:
    try:
        return (0, int(value))
    except (TypeError, ValueError):
        return (1, "" if value is None else str(value))


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
