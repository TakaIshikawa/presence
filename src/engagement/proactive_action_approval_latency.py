"""Report proactive action approval and posting latency."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_WINDOW_DAYS = 14
DEFAULT_PENDING_SLA_HOURS = 24.0
DEFAULT_REVIEW_SLA_HOURS = 48.0
DEFAULT_LIMIT = 25


def build_proactive_action_approval_latency_report(
    rows: list[dict[str, Any]],
    *,
    window_days: int = DEFAULT_WINDOW_DAYS,
    pending_sla_hours: float = DEFAULT_PENDING_SLA_HOURS,
    review_sla_hours: float = DEFAULT_REVIEW_SLA_HOURS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: tuple[str, ...] = (),
) -> dict[str, Any]:
    if window_days <= 0:
        raise ValueError("window_days must be positive")
    if pending_sla_hours <= 0 or review_sla_hours <= 0:
        raise ValueError("SLA hours must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    window_start = generated_at - timedelta(days=window_days)
    normalized = [_normalize_row(row, generated_at) for row in rows]
    overdue_pending = [
        _item(row, row["pending_age_hours"], "pending_review")
        for row in normalized
        if row["status"] == "pending" and row["pending_age_hours"] is not None and row["pending_age_hours"] > pending_sla_hours
    ]
    late_reviewed = [
        _item(row, row["review_latency_hours"], "reviewed")
        for row in normalized
        if row["status"] in {"approved", "posted", "dismissed", "rejected"}
        and row["review_latency_hours"] is not None
        and row["review_latency_hours"] > review_sla_hours
    ]
    late_posted = [
        _item(row, row["post_latency_hours"], "posted")
        for row in normalized
        if row["status"] == "posted"
        and row["post_latency_hours"] is not None
        and row["post_latency_hours"] > review_sla_hours
    ]
    overdue_pending.sort(key=lambda item: (-item["latency_hours"], item["action_id"]))
    late_reviewed.sort(key=lambda item: (-item["latency_hours"], item["action_id"]))
    late_posted.sort(key=lambda item: (-item["latency_hours"], item["action_id"]))

    return {
        "artifact_type": "proactive_action_approval_latency",
        "generated_at": generated_at.isoformat(),
        "thresholds": {
            "window_days": window_days,
            "window_start": window_start.isoformat(),
            "window_end": generated_at.isoformat(),
            "pending_sla_hours": pending_sla_hours,
            "review_sla_hours": review_sla_hours,
            "limit": limit,
        },
        "missing_tables": list(missing_tables),
        "summary": {
            "rows_scanned": len(rows),
            "pending_count": sum(1 for row in normalized if row["status"] == "pending"),
            "reviewed_count": sum(1 for row in normalized if row["reviewed_at"]),
            "posted_count": sum(1 for row in normalized if row["posted_at"]),
            "overdue_pending_count": len(overdue_pending),
            "late_reviewed_count": len(late_reviewed),
            "late_posted_count": len(late_posted),
        },
        "overdue_pending_actions": overdue_pending[:limit],
        "late_reviewed_actions": late_reviewed[:limit],
        "late_posted_actions": late_posted[:limit],
        "grouped_summaries": _grouped_summaries(overdue_pending + late_reviewed + late_posted),
    }


def build_proactive_action_approval_latency_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = tuple(table for table in ("proactive_actions",) if table not in schema)
    if missing_tables:
        return build_proactive_action_approval_latency_report([], missing_tables=missing_tables, **kwargs)
    now = _utc(kwargs.get("now") or datetime.now(timezone.utc))
    window_days = kwargs.get("window_days", DEFAULT_WINDOW_DAYS)
    cutoff = now - timedelta(days=window_days)
    rows = _load_rows(conn, schema["proactive_actions"], cutoff=cutoff, window_end=now)
    return build_proactive_action_approval_latency_report(
        rows,
        missing_tables=missing_tables,
        **{**kwargs, "now": now},
    )


def format_proactive_action_approval_latency_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_proactive_action_approval_latency_text(report: dict[str, Any]) -> str:
    thresholds = report["thresholds"]
    summary = report["summary"]
    lines = [
        "Proactive Action Approval Latency",
        f"Generated: {report['generated_at']}",
        f"Window: days={thresholds['window_days']} start={thresholds['window_start']} end={thresholds['window_end']}",
        f"SLAs: pending={thresholds['pending_sla_hours']:.1f}h review={thresholds['review_sla_hours']:.1f}h limit={thresholds['limit']}",
        (
            f"Totals: rows={summary['rows_scanned']} pending={summary['pending_count']} "
            f"reviewed={summary['reviewed_count']} posted={summary['posted_count']} "
            f"overdue_pending={summary['overdue_pending_count']} late_reviewed={summary['late_reviewed_count']} "
            f"late_posted={summary['late_posted_count']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    for label, key in (
        ("Overdue pending actions", "overdue_pending_actions"),
        ("Late reviewed actions", "late_reviewed_actions"),
        ("Late posted actions", "late_posted_actions"),
    ):
        if report[key]:
            lines.extend(["", f"{label}:"])
            for item in report[key]:
                lines.append(
                    f"- action_id={item['action_id']} type={item['action_type']} source={item['discovery_source']} "
                    f"status={item['status']} latency={item['latency_hours']:.1f}h"
                )
    if not any(report[key] for key in ("overdue_pending_actions", "late_reviewed_actions", "late_posted_actions")) and not report["missing_tables"]:
        lines.append("No proactive action approval latency breaches found.")
    return "\n".join(lines)


def _load_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    cutoff: datetime,
    window_end: datetime,
) -> list[dict[str, Any]]:
    selected = [
        _column_expr(columns, "id", default="rowid"),
        _column_expr(columns, "action_type", default="'unknown'"),
        _column_expr(columns, "discovery_source", default="'unknown'"),
        _column_expr(columns, "status", default="'pending'"),
        _column_expr(columns, "created_at", default="NULL"),
        _column_expr(columns, "reviewed_at", default="NULL"),
        _column_expr(columns, "posted_at", default="NULL"),
    ]
    filters = []
    params: list[Any] = []
    if "created_at" in columns:
        filters.append("datetime(created_at) >= datetime(?) AND datetime(created_at) <= datetime(?)")
        params.extend([_sqlite_ts(cutoff), _sqlite_ts(window_end)])
    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM proactive_actions {where} ORDER BY id", params)]


def _normalize_row(row: dict[str, Any], now: datetime) -> dict[str, Any]:
    created_at = _parse_dt(row.get("created_at"))
    reviewed_at = _parse_dt(row.get("reviewed_at"))
    posted_at = _parse_dt(row.get("posted_at"))
    status = _norm(row.get("status"))
    return {
        "action_id": _int(row.get("id")),
        "action_type": _norm(row.get("action_type")),
        "discovery_source": _norm(row.get("discovery_source")),
        "status": status,
        "created_at": created_at.isoformat() if created_at else None,
        "reviewed_at": reviewed_at.isoformat() if reviewed_at else None,
        "posted_at": posted_at.isoformat() if posted_at else None,
        "pending_age_hours": _hours(created_at, now) if created_at and status == "pending" else None,
        "review_latency_hours": _hours(created_at, reviewed_at) if created_at and reviewed_at else None,
        "post_latency_hours": _hours(reviewed_at or created_at, posted_at) if (reviewed_at or created_at) and posted_at else None,
    }


def _item(row: dict[str, Any], latency: float | None, breach_type: str) -> dict[str, Any]:
    return {
        "action_id": row["action_id"],
        "action_type": row["action_type"],
        "discovery_source": row["discovery_source"],
        "status": row["status"],
        "created_at": row["created_at"],
        "reviewed_at": row["reviewed_at"],
        "posted_at": row["posted_at"],
        "breach_type": breach_type,
        "latency_hours": round(float(latency or 0.0), 2),
    }


def _grouped_summaries(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        grouped[(item["action_type"], item["discovery_source"])].append(item)
    summaries = [
        {
            "action_type": action_type,
            "discovery_source": source,
            "count": len(bucket),
            "max_latency_hours": max(item["latency_hours"] for item in bucket),
            "breach_types": dict(sorted(__import__("collections").Counter(item["breach_type"] for item in bucket).items())),
        }
        for (action_type, source), bucket in grouped.items()
    ]
    summaries.sort(key=lambda item: (-item["count"], -item["max_latency_hours"], item["action_type"], item["discovery_source"]))
    return summaries


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _column_expr(columns: set[str], name: str, *, default: str) -> str:
    return name if name in columns else f"{default} AS {name}"


def _parse_dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _hours(start: datetime, end: datetime) -> float:
    return (end - start).total_seconds() / 3600


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _sqlite_ts(value: datetime) -> str:
    return _utc(value).strftime("%Y-%m-%d %H:%M:%S")


def _norm(value: Any) -> str:
    text = "" if value is None else str(value).strip().lower()
    return text or "unknown"


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0
