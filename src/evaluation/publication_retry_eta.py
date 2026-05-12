"""Report retry ETA state for queued and failed content publications."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
DEFAULT_MAX_ATTEMPTS = 3
TARGET_STATUSES = {"queued", "failed"}


def build_publication_retry_eta_report(
    db_or_conn: Any,
    *,
    limit: int = DEFAULT_LIMIT,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Summarize queued and failed content_publications by retry ETA bucket."""
    if limit <= 0:
        raise ValueError("limit must be positive")
    if max_attempts <= 0:
        raise ValueError("max_attempts must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    rows = _load_rows(conn, schema)
    items = [_item(row, generated_at, max_attempts) for row in rows]
    items.sort(key=_sort_key)
    limited = items[:limit]
    return {
        "artifact_type": "publication_retry_eta",
        "generated_at": generated_at.isoformat(),
        "filters": {"limit": limit, "max_attempts": max_attempts},
        "totals": {
            "rows_scanned": len(rows),
            "returned": len(limited),
            "bucket_counts": dict(sorted(Counter(item["retry_bucket"] for item in items).items())),
            "platform_counts": dict(sorted(Counter(item["platform"] for item in items).items())),
            "status_counts": dict(sorted(Counter(item["status"] for item in items).items())),
            "error_category_counts": dict(sorted(Counter(item["error_category"] for item in items).items())),
        },
        "rows": limited,
        "groups": _groups(items),
        "missing_tables": [table for table in ("content_publications", "generated_content") if table not in schema],
        "missing_columns": _missing_columns(schema),
    }


def format_publication_retry_eta_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_publication_retry_eta_text(report: dict[str, Any]) -> str:
    lines = [
        "Publication Retry ETA",
        f"Generated: {report['generated_at']}",
        f"Limit: {report['filters']['limit']} max_attempts={report['filters']['max_attempts']}",
        "Totals: "
        + ", ".join(f"{bucket}={count}" for bucket, count in report["totals"]["bucket_counts"].items()),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["rows"]:
        lines.extend(["", "No queued or failed publication retry rows found."])
        return "\n".join(lines)
    lines.extend(["", "Retry rows:"])
    for item in report["rows"]:
        lines.append(
            f"  - publication_id={item['publication_id']} content_id={item['content_id']} "
            f"platform={item['platform']} status={item['status']} bucket={item['retry_bucket']} "
            f"next_retry_at={item['next_retry_at'] or '-'} error_category={item['error_category']}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    cp = schema.get("content_publications")
    if cp is None or not {"content_id", "platform", "status"}.issubset(cp):
        return []
    gc = schema.get("generated_content", set())
    join = (
        "LEFT JOIN generated_content gc ON gc.id = cp.content_id"
        if "generated_content" in schema and "id" in gc
        else "LEFT JOIN (SELECT NULL AS id) gc ON 0"
    )
    rows = conn.execute(
        f"""SELECT
               {_expr(cp, "id", "cp", "publication_id")},
               cp.content_id AS content_id,
               cp.platform AS platform,
               cp.status AS status,
               {_expr(cp, "next_retry_at", "cp", "next_retry_at")},
               {_expr(cp, "last_error_at", "cp", "last_error_at")},
               {_expr(cp, "error_category", "cp", "error_category")},
               {_expr(cp, "error", "cp", "error")},
               {_expr(cp, "attempt_count", "cp", "attempt_count")},
               {_expr(gc, "published", "gc", "content_published")},
               {_expr(gc, "retry_count", "gc", "content_retry_count")}
            FROM content_publications cp
            {join}
            WHERE LOWER(COALESCE(cp.status, '')) IN ('queued', 'failed')
            ORDER BY cp.content_id ASC, cp.platform ASC"""
    ).fetchall()
    return [dict(row) for row in rows]


def _item(row: dict[str, Any], now: datetime, max_attempts: int) -> dict[str, Any]:
    status = _clean(row.get("status")).lower() or "unknown"
    attempt_count = _int(row.get("attempt_count")) or 0
    next_retry = _parse_dt(row.get("next_retry_at"))
    content_state = _int(row.get("content_published"))
    if content_state in {1, -1}:
        bucket = "blocked_by_terminal_content_state"
    elif attempt_count >= max_attempts - 1:
        bucket = "retry_exhaustion_risk"
    elif next_retry is None and status == "failed":
        bucket = "retry_missing_eta"
    elif next_retry is not None and next_retry <= now:
        bucket = "retry_due_now"
    elif next_retry is not None:
        bucket = "retry_scheduled"
    else:
        bucket = "retry_missing_eta"
    return {
        "publication_id": row.get("publication_id"),
        "content_id": row.get("content_id"),
        "platform": _clean(row.get("platform")).lower() or "unknown",
        "status": status,
        "retry_bucket": bucket,
        "next_retry_at": next_retry.isoformat() if next_retry else None,
        "last_error_at": (_parse_dt(row.get("last_error_at")) or None).isoformat()
        if _parse_dt(row.get("last_error_at"))
        else None,
        "error_category": _clean(row.get("error_category")).lower() or "none",
        "error": row.get("error"),
        "attempt_count": attempt_count,
        "content_published": content_state,
    }


def _groups(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for item in items:
        key = (item["retry_bucket"], item["platform"], item["status"], item["error_category"])
        group = grouped.setdefault(
            key,
            {
                "retry_bucket": key[0],
                "platform": key[1],
                "status": key[2],
                "error_category": key[3],
                "count": 0,
                "representative_publication_ids": [],
            },
        )
        group["count"] += 1
        if len(group["representative_publication_ids"]) < 5:
            group["representative_publication_ids"].append(item["publication_id"])
    return sorted(grouped.values(), key=lambda row: (-row["count"], row["retry_bucket"], row["platform"], row["status"]))


def _sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (
        item["next_retry_at"] is None,
        item["next_retry_at"] or "",
        item["last_error_at"] is None,
        item["last_error_at"] or "",
        item["publication_id"] or 0,
        item["content_id"] or 0,
    )


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    return {table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")} for table in tables}


def _missing_columns(schema: dict[str, set[str]]) -> dict[str, list[str]]:
    required = {
        "content_publications": {"content_id", "platform", "status", "next_retry_at", "attempt_count"},
        "generated_content": {"id", "published"},
    }
    missing: dict[str, list[str]] = {}
    for table, columns in required.items():
        if table in schema:
            gaps = sorted(columns - schema[table])
            if gaps:
                missing[table] = gaps
    return missing


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _expr(columns: set[str], column: str, alias: str, output: str) -> str:
    return f"{alias}.{column} AS {output}" if column in columns else f"NULL AS {output}"


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))
