"""Summarize proactive action conversion by discovery source and action type."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import json
import sqlite3
from statistics import median
from typing import Any


def build_proactive_action_conversion_by_source_report(
    rows: list[dict[str, Any]],
    *,
    missing_tables: tuple[str, ...] = (),
) -> dict[str, Any]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    normalized = [_normalize_row(row) for row in rows]
    for row in normalized:
        grouped[(row["discovery_source"], row["action_type"])].append(row)
    summaries = [_summary(source, action_type, bucket) for (source, action_type), bucket in grouped.items()]
    summaries.sort(key=lambda item: (-item["total_count"], item["discovery_source"], item["action_type"]))
    return {
        "artifact_type": "proactive_action_conversion_by_source",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "missing_tables": list(missing_tables),
        "summary": {
            "rows_scanned": len(rows),
            "group_count": len(summaries),
            "pending_count": sum(1 for row in normalized if row["status"] == "pending"),
            "approved_count": sum(1 for row in normalized if row["approved"]),
            "posted_count": sum(1 for row in normalized if row["posted"]),
            "dismissed_count": sum(1 for row in normalized if row["dismissed"]),
        },
        "grouped_summaries": summaries,
    }


def build_proactive_action_conversion_by_source_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = tuple(table for table in ("proactive_actions",) if table not in schema)
    rows = [] if missing_tables else _load_rows(conn, schema["proactive_actions"])
    return build_proactive_action_conversion_by_source_report(rows, missing_tables=missing_tables, **kwargs)


def format_proactive_action_conversion_by_source_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_proactive_action_conversion_by_source_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Proactive Action Conversion By Source",
        f"Generated: {report['generated_at']}",
        (
            f"Totals: rows={summary['rows_scanned']} pending={summary['pending_count']} "
            f"approved={summary['approved_count']} posted={summary['posted_count']} dismissed={summary['dismissed_count']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["grouped_summaries"]:
        lines.extend(["", "source | action_type | total | pending | approved | posted | dismissed | approval_rate | post_rate | median_review_h | median_post_h"])
        for row in report["grouped_summaries"]:
            lines.append(
                f"{row['discovery_source']} | {row['action_type']} | {row['total_count']} | {row['pending_count']} | "
                f"{row['approved_count']} | {row['posted_count']} | {row['dismissed_count']} | {row['approval_rate']} | "
                f"{row['post_rate']} | {row['median_review_hours']} | {row['median_post_hours']}"
            )
    elif not report["missing_tables"]:
        lines.append("No proactive actions found.")
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    select = [
        _expr(columns, "id", "id", "rowid"),
        _expr(columns, "discovery_source", "discovery_source", "'unknown'"),
        _expr(columns, "action_type", "action_type", "'unknown'"),
        _expr(columns, "status", "status", "'pending'"),
        _expr(columns, "created_at", "created_at", "NULL"),
        _expr(columns, "reviewed_at", "reviewed_at", "NULL"),
        _expr(columns, "posted_at", "posted_at", "NULL"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM proactive_actions ORDER BY rowid")]


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    status = _norm(row.get("status"), "pending")
    created_at = _parse_dt(row.get("created_at"))
    reviewed_at = _parse_dt(row.get("reviewed_at"))
    posted_at = _parse_dt(row.get("posted_at"))
    approved = status in {"approved", "posted"} or (reviewed_at is not None and status not in {"dismissed", "rejected"})
    posted = status == "posted" or posted_at is not None
    dismissed = status in {"dismissed", "rejected"}
    return {
        "id": row.get("id"),
        "discovery_source": _norm(row.get("discovery_source"), "unknown"),
        "action_type": _norm(row.get("action_type"), "unknown"),
        "status": status,
        "approved": approved,
        "posted": posted,
        "dismissed": dismissed,
        "review_hours": _hours(created_at, reviewed_at) if created_at and reviewed_at else None,
        "post_hours": _hours(created_at, posted_at) if created_at and posted_at else None,
    }


def _summary(source: str, action_type: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    approved = sum(1 for row in rows if row["approved"])
    posted = sum(1 for row in rows if row["posted"])
    dismissed = sum(1 for row in rows if row["dismissed"])
    review_hours = [row["review_hours"] for row in rows if row["review_hours"] is not None]
    post_hours = [row["post_hours"] for row in rows if row["post_hours"] is not None]
    return {
        "discovery_source": source,
        "action_type": action_type,
        "total_count": total,
        "pending_count": sum(1 for row in rows if row["status"] == "pending"),
        "approved_count": approved,
        "posted_count": posted,
        "dismissed_count": dismissed,
        "approval_rate": _rate(approved, total),
        "post_rate": _rate(posted, total),
        "median_review_hours": round(float(median(review_hours)), 2) if review_hours else None,
        "median_post_hours": round(float(median(post_hours)), 2) if post_hours else None,
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {row[0]: {col[1] for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def _expr(columns: set[str], column: str, output: str, default: str) -> str:
    return f"{column} AS {output}" if column in columns else f"{default} AS {output}"


def _parse_dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def _hours(start: datetime, end: datetime) -> float:
    return round((end - start).total_seconds() / 3600, 2)


def _rate(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 4) if denominator else 0.0


def _norm(value: Any, default: str) -> str:
    text = "" if value is None else str(value).strip().lower()
    return text or default
