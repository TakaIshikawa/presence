"""Report publications whose retry budget is nearly or fully exhausted."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any, Iterable, Mapping

from output.publish_errors import classify_publish_error, normalize_error_category
from storage.db import MAX_RETRIES


DEFAULT_RETRY_LIMIT = MAX_RETRIES
DEFAULT_NEARLY_EXHAUSTED_RETRIES = 1


def build_publication_retry_exhaustion_report(
    rows: Iterable[Mapping[str, Any]],
    *,
    retry_limit: int = DEFAULT_RETRY_LIMIT,
    nearly_exhausted_retries: int = DEFAULT_NEARLY_EXHAUSTED_RETRIES,
    now: datetime | None = None,
    schema_gaps: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return publication rows with retry budget exhaustion status."""
    if retry_limit <= 0:
        raise ValueError("retry_limit must be positive")
    if nearly_exhausted_retries < 0:
        raise ValueError("nearly_exhausted_retries must be non-negative")
    generated_at = _utc(now or datetime.now(timezone.utc))
    report_rows = [
        _publication_row(row, retry_limit=retry_limit, nearly_exhausted_retries=nearly_exhausted_retries)
        for row in rows
    ]
    report_rows.sort(key=_sort_key)
    return {
        "artifact_type": "publication_retry_exhaustion",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "retry_limit": retry_limit,
            "nearly_exhausted_retries": nearly_exhausted_retries,
            "statuses": ["failed", "queued", "pending", "unpublished"],
        },
        "summary": {
            "row_count": len(report_rows),
            "retryable_count": sum(1 for row in report_rows if row["exhaustion_status"] == "retryable"),
            "nearly_exhausted_count": sum(1 for row in report_rows if row["exhaustion_status"] == "nearly_exhausted"),
            "exhausted_count": sum(1 for row in report_rows if row["exhaustion_status"] == "exhausted"),
        },
        "groups": _groups(report_rows),
        "rows": report_rows,
        "schema_gaps": schema_gaps or {"missing_tables": [], "missing_columns": {}},
    }


def build_publication_retry_exhaustion_report_from_db(
    db_or_conn: Any,
    *,
    retry_limit: int = DEFAULT_RETRY_LIMIT,
    nearly_exhausted_retries: int = DEFAULT_NEARLY_EXHAUSTED_RETRIES,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Load unpublished or failed publication rows from SQLite."""
    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    schema = _schema(conn)
    gaps = _schema_gaps(schema)
    rows = _load_rows(conn, schema) if not gaps["missing_tables"] else []
    return build_publication_retry_exhaustion_report(
        rows,
        retry_limit=retry_limit,
        nearly_exhausted_retries=nearly_exhausted_retries,
        now=now,
        schema_gaps=gaps,
    )


def format_publication_retry_exhaustion_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_publication_retry_exhaustion_table(report: dict[str, Any]) -> str:
    lines = [
        "Publication Retry Exhaustion",
        f"Generated: {report['generated_at']}",
        (
            "Totals: "
            f"rows={report['summary']['row_count']} "
            f"retryable={report['summary']['retryable_count']} "
            f"near={report['summary']['nearly_exhausted_count']} "
            f"exhausted={report['summary']['exhausted_count']}"
        ),
    ]
    if report["groups"]:
        lines.append("Groups: " + "; ".join(
            f"{group['channel']}/{group['last_error_category']}={group['count']}"
            for group in report["groups"]
        ))
    if not report["rows"]:
        lines.extend(["", "No unpublished or failed publication retry rows found."])
        return "\n".join(lines)
    lines.extend(["", "publication  content  channel   attempts  limit  remaining  category       status"])
    for row in report["rows"]:
        lines.append(
            f"{row['publication_id']:<12} "
            f"{row['content_id']:<8} "
            f"{row['channel'][:8]:<8} "
            f"{row['attempts']:<8} "
            f"{row['retry_limit']:<6} "
            f"{row['remaining_retries']:<10} "
            f"{row['last_error_category'][:14]:<14} "
            f"{row['exhaustion_status']}"
        )
    return "\n".join(lines)


def _publication_row(
    row: Mapping[str, Any],
    *,
    retry_limit: int,
    nearly_exhausted_retries: int,
) -> dict[str, Any]:
    data = _row_dict(row)
    attempts = max(_int(_first(data, "attempts", "attempt_count", "retry_count")) or 0, 0)
    remaining = max(retry_limit - attempts, 0)
    raw_category = _text(_first(data, "last_error_category", "error_category"))
    category = normalize_error_category(raw_category)
    if category == "unknown":
        category = classify_publish_error(_text(data.get("error")), platform=_text(_first(data, "channel", "platform")) or "")
    return {
        "publication_id": _int_or_text(_first(data, "publication_id", "id")),
        "content_id": _int_or_text(_first(data, "content_id", "generated_content_id")),
        "channel": (_text(_first(data, "channel", "platform")) or "unknown").lower(),
        "publication_status": (_text(data.get("status")) or "unknown").lower(),
        "attempts": attempts,
        "retry_limit": retry_limit,
        "remaining_retries": remaining,
        "last_error_category": category,
        "last_error_at": _text(_first(data, "last_error_at", "updated_at")),
        "exhaustion_status": _exhaustion_status(remaining, nearly_exhausted_retries),
        "action": _action(remaining, nearly_exhausted_retries),
    }


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    cp = schema.get("content_publications", set())
    if not {"content_id", "platform", "status"}.issubset(cp):
        return []
    selected = [
        _expr(cp, "id", "publication_id"),
        "content_id",
        "platform AS channel",
        "status",
        _expr(cp, "attempt_count", "attempts"),
        _expr(cp, "error_category", "last_error_category"),
        _expr(cp, "error", "error"),
        _expr(cp, "last_error_at", "last_error_at"),
        _expr(cp, "updated_at", "updated_at"),
    ]
    terminal = "'published','sent','succeeded','success','posted'"
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT {', '.join(selected)}
                FROM content_publications
                WHERE LOWER(COALESCE(status, '')) NOT IN ({terminal})
                ORDER BY platform ASC, content_id ASC"""
        ).fetchall()
    ]


def _groups(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter((row["channel"], row["last_error_category"], row["exhaustion_status"]) for row in rows)
    groups = [
        {
            "channel": channel,
            "last_error_category": category,
            "exhaustion_status": status,
            "count": count,
        }
        for (channel, category, status), count in counts.items()
    ]
    return sorted(groups, key=lambda row: (-row["count"], row["channel"], row["last_error_category"], row["exhaustion_status"]))


def _exhaustion_status(remaining: int, nearly_exhausted_retries: int) -> str:
    if remaining <= 0:
        return "exhausted"
    if remaining <= nearly_exhausted_retries:
        return "nearly_exhausted"
    return "retryable"


def _action(remaining: int, nearly_exhausted_retries: int) -> str:
    if remaining <= 0:
        return "escalate_or_repair_before_retry"
    if remaining <= nearly_exhausted_retries:
        return "inspect_before_next_retry"
    return "retry_available"


def _schema_gaps(schema: dict[str, set[str]]) -> dict[str, Any]:
    if "content_publications" not in schema:
        return {"missing_tables": ["content_publications"], "missing_columns": {}}
    required = {"content_id", "platform", "status"}
    missing = sorted(required - schema["content_publications"])
    return {"missing_tables": [], "missing_columns": {"content_publications": missing} if missing else {}}


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {
        str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")}
        for row in rows
    }


def _row_dict(row: Mapping[str, Any]) -> dict[str, Any]:
    if hasattr(row, "keys"):
        return {str(key): row[key] for key in row.keys()}
    return dict(row)


def _expr(columns: set[str], column: str, output: str) -> str:
    return f"{column} AS {output}" if column in columns else f"NULL AS {output}"


def _first(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _int_or_text(value: Any) -> int | str | None:
    parsed = _int(value)
    return parsed if parsed is not None else _text(value)


def _sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    rank = {"exhausted": 0, "nearly_exhausted": 1, "retryable": 2}
    return (
        rank.get(row["exhaustion_status"], 9),
        row["remaining_retries"],
        row["channel"],
        row["last_error_category"],
        row["publication_id"] or 0,
    )
