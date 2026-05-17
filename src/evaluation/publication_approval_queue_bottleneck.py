"""Summarize publication items waiting too long in approval states."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_WARNING_DAYS = 3
DEFAULT_CRITICAL_DAYS = 7
PENDING_STATES = ("review", "approval", "pending_review", "pending_approval")


def build_publication_approval_queue_bottleneck_report(
    db_or_conn: Any,
    *,
    warning_days: int = DEFAULT_WARNING_DAYS,
    critical_days: int = DEFAULT_CRITICAL_DAYS,
    now: datetime | None = None,
) -> dict[str, Any]:
    if warning_days <= 0:
        raise ValueError("warning_days must be positive")
    if critical_days < warning_days:
        raise ValueError("critical_days must be greater than or equal to warning_days")
    generated_at = _utc(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    filters = {"warning_days": warning_days, "critical_days": critical_days}
    if "publication_queue" not in schema:
        return _report(generated_at, filters, [], missing_tables=["publication_queue"])
    rows = [_row_item(dict(row), generated_at, warning_days, critical_days) for row in _load_rows(conn, schema)]
    groups: dict[tuple[str, str, str], dict[str, Any]] = {}
    for item in rows:
        key = (item["review_state"], item["age_bucket"], item["content_type"])
        group = groups.setdefault(
            key,
            {
                "review_state": key[0],
                "age_bucket": key[1],
                "content_type": key[2],
                "item_count": 0,
                "oldest_item": None,
                "severity": "healthy",
            },
        )
        group["item_count"] += 1
        if group["oldest_item"] is None or item["age_days"] > group["oldest_item"]["age_days"]:
            group["oldest_item"] = item
        group["severity"] = _max_severity(group["severity"], item["severity"])
    summaries = sorted(groups.values(), key=lambda g: (-_severity_rank(g["severity"]), -g["oldest_item"]["age_days"], g["review_state"], g["content_type"]))
    return _report(generated_at, filters, summaries)


def format_publication_approval_queue_bottleneck_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_publication_approval_queue_bottleneck_text(report: dict[str, Any]) -> str:
    lines = [
        "Publication Approval Queue Bottleneck",
        f"Generated: {report['generated_at']}",
        f"Thresholds: warning={report['filters']['warning_days']}d critical={report['filters']['critical_days']}d",
        f"Totals: groups={report['totals']['group_count']} items={report['totals']['item_count']} critical={report['totals']['critical_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["groups"]:
        lines.append("No pending publication approvals found.")
        return "\n".join(lines)
    for group in report["groups"]:
        oldest = group["oldest_item"]
        lines.append(
            f"  - state={group['review_state']} type={group['content_type']} bucket={group['age_bucket']} "
            f"count={group['item_count']} severity={group['severity']} oldest={oldest['content_id']}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[sqlite3.Row]:
    cols = schema["publication_queue"]
    state_expr = _expr(cols, "review_state", fallback="state")
    created_expr = _expr(cols, "entered_state_at", fallback="updated_at")
    return conn.execute(
        f"""SELECT {_expr(cols, 'content_id', fallback='id')}, {_expr(cols, 'content_type')},
                  {state_expr}, {created_expr}, {_expr(cols, 'title')}
            FROM publication_queue
            WHERE LOWER(COALESCE({state_expr}, '')) IN ({','.join('?' for _ in PENDING_STATES)})""",
        [state.lower() for state in PENDING_STATES],
    ).fetchall()


def _row_item(row: dict[str, Any], now: datetime, warning_days: int, critical_days: int) -> dict[str, Any]:
    entered = _parse_dt(row.get("entered_state_at") or row.get("updated_at")) or now
    age_days = int((now - entered).total_seconds() // 86400)
    return {
        "content_id": _clean(row.get("content_id") or row.get("id")),
        "content_type": _clean(row.get("content_type")) or "unknown",
        "review_state": _clean(row.get("review_state") or row.get("state")) or "unknown",
        "title": _clean(row.get("title")),
        "entered_state_at": entered.isoformat(),
        "age_days": age_days,
        "age_bucket": _bucket(age_days),
        "severity": "critical" if age_days >= critical_days else "warning" if age_days >= warning_days else "healthy",
    }


def _report(generated_at: datetime, filters: dict[str, Any], groups: list[dict[str, Any]], *, missing_tables: list[str] | None = None) -> dict[str, Any]:
    return {
        "artifact_type": "publication_approval_queue_bottleneck",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "group_count": len(groups),
            "item_count": sum(group["item_count"] for group in groups),
            "warning_count": sum(1 for group in groups if group["severity"] == "warning"),
            "critical_count": sum(1 for group in groups if group["severity"] == "critical"),
        },
        "groups": groups,
        "missing_tables": missing_tables or [],
    }


def _expr(cols: set[str], name: str, *, fallback: str | None = None) -> str:
    if name in cols:
        return name
    if fallback and fallback in cols:
        return f"{fallback} AS {name}"
    return f"NULL AS {name}"


def _bucket(age_days: int) -> str:
    if age_days >= 14:
        return "14d_plus"
    if age_days >= 7:
        return "7_13d"
    if age_days >= 3:
        return "3_6d"
    return "0_2d"


def _max_severity(a: str, b: str) -> str:
    return a if _severity_rank(a) >= _severity_rank(b) else b


def _severity_rank(value: str) -> int:
    return {"healthy": 0, "warning": 1, "critical": 2}.get(value, 0)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    return {table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")} for table in tables}


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
