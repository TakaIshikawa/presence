"""Report selected generated content variants that have not published."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 7
DEFAULT_LIMIT = 100
SELECTED_STATUSES = {"selected", "approved", "queued", "ready"}
PUBLISHED_STATUSES = {"published", "posted", "sent", "complete", "completed"}
TIME_COLUMNS = ("selected_at", "approved_at", "updated_at", "created_at")


def build_content_variant_selection_staleness_report(
    rows: list[dict[str, Any]],
    *,
    days: int = DEFAULT_DAYS,
    platform: str | None = None,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    platform_filter = _norm(platform)
    findings = []
    for row in rows:
        selected_at = _parse_timestamp(row.get("selected_at"))
        row_platform = _norm(row.get("platform")) or "unknown"
        if platform_filter and row_platform != platform_filter:
            continue
        if selected_at and selected_at <= cutoff and _is_stale(row):
            findings.append(_finding(row, selected_at, generated_at))
    findings.sort(key=lambda f: (-f["selected_age_days"], f["platform"], str(f["variant_id"])))
    shown = findings[:limit]
    return {
        "artifact_type": "content_variant_selection_staleness",
        "generated_at": generated_at.isoformat(),
        "filters": {"days": days, "platform": platform, "limit": limit},
        "summary": {
            "row_count": len(rows),
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_platform_content_type_status": [
                {"platform": p, "content_type": c, "selection_status": s, "count": n}
                for (p, c, s), n in sorted(Counter((f["platform"], f["content_type"], f["selection_status"]) for f in findings).items())
            ],
        },
        "findings": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {t: sorted(c) for t, c in sorted((missing_columns or {}).items()) if c},
        "empty_state": {"is_empty": not findings, "message": "No stale selected variants found." if not findings else None},
    }


def build_content_variant_selection_staleness_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    table = next((name for name in ("content_variants", "generated_content_variants", "generated_content") if name in schema), None)
    if table is None:
        return build_content_variant_selection_staleness_report([], missing_tables=["content_variants|generated_content_variants|generated_content"], **kwargs)
    columns = schema[table]
    missing = []
    if "id" not in columns and "variant_id" not in columns:
        missing.append("id|variant_id")
    if not set(TIME_COLUMNS) & columns:
        missing.append("|".join(TIME_COLUMNS))
    if missing:
        return build_content_variant_selection_staleness_report([], missing_columns={table: missing}, **kwargs)
    return build_content_variant_selection_staleness_report(_load_rows(conn, table, columns, schema), **kwargs)


def format_content_variant_selection_staleness_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_content_variant_selection_staleness_text(report: dict[str, Any]) -> str:
    s = report["summary"]
    lines = [
        "Content Variant Selection Staleness",
        f"Generated: {report['generated_at']}",
        f"Filters: days={report['filters']['days']} platform={report['filters']['platform'] or '*'} limit={report['filters']['limit']}",
        f"Totals: rows={s['row_count']} findings={s['finding_count']} shown={s['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + ", ".join(f"{t}.{c}" for t, cols in report["missing_columns"].items() for c in cols))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.append("Findings:")
    for f in report["findings"]:
        lines.append(f"  - variant={f['variant_id']} platform={f['platform']} age_days={f['selected_age_days']} publish_status={f['publish_status']} queue_id={f['queue_id'] or '-'}")
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, table: str, columns: set[str], schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    aliases = {
        "variant_id": ("variant_id", "id"),
        "content_id": ("content_id", "generated_content_id"),
        "platform": ("platform",),
        "content_type": ("content_type", "type", "format"),
        "selection_status": ("selection_status", "status"),
        "publish_status": ("publish_status", "publication_status", "status"),
        "selected_at": TIME_COLUMNS,
        "queue_id": ("queue_id", "publish_queue_id"),
    }
    select = ", ".join(f"v.{_coalesce(columns, names)} AS {alias}" if _coalesce(columns, names) != "NULL" else f"NULL AS {alias}" for alias, names in aliases.items())
    join = ""
    queue_select = ""
    if "publish_queue" in schema and "content_id" in schema["publish_queue"] and ("content_id" in columns or "generated_content_id" in columns):
        pq = schema["publish_queue"]
        join = " LEFT JOIN publish_queue pq ON pq.content_id = v.content_id"
        queue_select = f", {_qexpr(pq, 'id')} AS linked_queue_id, {_qexpr(pq, 'status')} AS linked_queue_status"
    rows = conn.execute(f"SELECT {select}{queue_select} FROM {table} v{join} ORDER BY datetime(v.{_coalesce(columns, TIME_COLUMNS)}) ASC").fetchall()
    out = []
    for row in rows:
        item = dict(row)
        item["queue_id"] = item.get("queue_id") or item.get("linked_queue_id")
        item["publish_status"] = item.get("linked_queue_status") or item.get("publish_status")
        out.append(item)
    return out


def _is_stale(row: dict[str, Any]) -> bool:
    status = _norm(row.get("selection_status")) or "selected"
    publish = _norm(row.get("publish_status"))
    return status in SELECTED_STATUSES and publish not in PUBLISHED_STATUSES


def _finding(row: dict[str, Any], selected_at: datetime, now: datetime) -> dict[str, Any]:
    publish_status = _norm(row.get("publish_status")) or "unpublished"
    return {
        "variant_id": row.get("variant_id"),
        "content_id": row.get("content_id"),
        "platform": _norm(row.get("platform")) or "unknown",
        "content_type": _norm(row.get("content_type")) or "unknown",
        "selection_status": _norm(row.get("selection_status")) or "selected",
        "selected_at": selected_at.isoformat(),
        "selected_age_days": round((now - selected_at).total_seconds() / 86400, 2),
        "publish_status": publish_status,
        "queue_id": row.get("queue_id"),
        "staleness_reason": "selected_variant_not_published",
    }


def _qexpr(columns: set[str], column: str) -> str:
    return f"pq.{column}" if column in columns else "NULL"


def _coalesce(columns: set[str], names: tuple[str, ...]) -> str:
    available = [name for name in names if name in columns]
    return "COALESCE(" + ", ".join(available) + ")" if len(available) > 1 else (available[0] if available else "NULL")


def _norm(value: Any) -> str | None:
    text = "" if value is None else str(value).strip().lower()
    return text or None


def _parse_timestamp(value: Any) -> datetime | None:
    text = "" if value is None else str(value).strip()
    if not text:
        return None
    try:
        return _utc(datetime.fromisoformat(text.replace("Z", "+00:00")))
    except ValueError:
        return None


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
