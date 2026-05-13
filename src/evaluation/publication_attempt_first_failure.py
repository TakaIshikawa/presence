"""Find first failed publication attempts and recovery status."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_LIMIT = 50
RECOVERY_STATUSES = ("recovered", "unrecovered")
REQUIRED_COLUMNS = {
    "id",
    "content_id",
    "platform",
    "attempted_at",
    "success",
    "error_category",
}


def build_publication_attempt_first_failure_report(
    db_or_conn: Any,
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return first failed publication attempts per content/platform pair."""

    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=lookback_days)
    filters = {
        "lookback_days": lookback_days,
        "lookback_start": cutoff.isoformat(),
        "lookback_end": generated_at.isoformat(),
        "limit": limit,
    }
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "publication_attempts" not in schema:
        return _empty_report(generated_at, filters, missing_tables=["publication_attempts"])
    missing = sorted(REQUIRED_COLUMNS - schema["publication_attempts"])
    if missing:
        return _empty_report(generated_at, filters, missing_columns={"publication_attempts": missing})

    groups = _attempt_groups(conn, cutoff, generated_at)
    items = [_failure_item(attempts) for attempts in groups.values()]
    items = [item for item in items if item is not None]
    items.sort(key=_sort_key)
    return {
        "artifact_type": "publication_attempt_first_failure",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": _totals(items),
        "items": items[:limit],
        "missing_tables": [],
        "missing_columns": {},
    }


def format_publication_attempt_first_failure_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_publication_attempt_first_failure_text(report: dict[str, Any]) -> str:
    filters = report["filters"]
    totals = report["totals"]
    lines = [
        "Publication Attempt First Failure",
        f"Generated: {report['generated_at']}",
        f"Filters: lookback_days={filters['lookback_days']} limit={filters['limit']}",
        (
            f"First failures: total={totals['total']} "
            f"recovered={totals['by_recovery_status']['recovered']} "
            f"unrecovered={totals['by_recovery_status']['unrecovered']}"
        ),
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report.get("missing_columns"):
        lines.append(
            "Missing columns: "
            + "; ".join(
                f"{table}({', '.join(columns)})"
                for table, columns in sorted(report["missing_columns"].items())
            )
        )
    if not report["items"]:
        lines.append("No first publication failures matched.")
        return "\n".join(lines)
    lines.append("")
    lines.append("Items:")
    for item in report["items"]:
        recovered_at = item["recovered_at"] or "-"
        lines.append(
            f"- content={item['content_id']} platform={item['platform']} "
            f"first_failed={item['first_failed_at']} error={item['error_category'] or '-'} "
            f"recovery={item['recovery_status']} attempts={item['attempts_until_recovery']} "
            f"recovered_at={recovered_at} latest={item['latest_status']}"
        )
    return "\n".join(lines)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    try:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    except sqlite3.Error:
        return {}
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    *,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    return {
        "artifact_type": "publication_attempt_first_failure",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "total": 0,
            "by_platform": {},
            "by_error_category": {},
            "by_recovery_status": {name: 0 for name in RECOVERY_STATUSES},
        },
        "items": [],
        "missing_tables": missing_tables or [],
        "missing_columns": missing_columns or {},
    }


def _attempt_groups(
    conn: sqlite3.Connection,
    cutoff: datetime,
    generated_at: datetime,
) -> dict[tuple[int, str], list[dict[str, Any]]]:
    failure_keys = conn.execute(
        """SELECT DISTINCT content_id, platform
           FROM publication_attempts
           WHERE success = 0
             AND datetime(attempted_at) >= datetime(?)
             AND datetime(attempted_at) <= datetime(?)
           ORDER BY content_id ASC, platform ASC""",
        (cutoff.isoformat(), generated_at.isoformat()),
    ).fetchall()
    groups: dict[tuple[int, str], list[dict[str, Any]]] = {}
    for row in failure_keys:
        key = (int(row["content_id"]), str(row["platform"]))
        attempts = conn.execute(
            """SELECT id, content_id, platform, attempted_at, success, error_category
               FROM publication_attempts
               WHERE content_id = ? AND platform = ?
               ORDER BY datetime(attempted_at) ASC, id ASC""",
            key,
        ).fetchall()
        groups[key] = [dict(attempt) for attempt in attempts]
    return groups


def _failure_item(attempts: list[dict[str, Any]]) -> dict[str, Any] | None:
    first_failure_index = None
    for index, attempt in enumerate(attempts):
        if int(attempt.get("success") or 0) == 0:
            first_failure_index = index
            break
    if first_failure_index is None:
        return None
    failure = attempts[first_failure_index]
    later = attempts[first_failure_index + 1 :]
    recovered_at = None
    attempts_until_recovery = None
    for offset, attempt in enumerate(later, start=1):
        if int(attempt.get("success") or 0) == 1:
            recovered_at = attempt.get("attempted_at")
            attempts_until_recovery = offset
            break
    latest = attempts[-1]
    recovery_status = "recovered" if recovered_at else "unrecovered"
    return {
        "content_id": int(failure["content_id"]),
        "platform": failure.get("platform"),
        "first_failure_attempt_id": int(failure["id"]),
        "first_failed_at": failure.get("attempted_at"),
        "error_category": failure.get("error_category") or "unknown",
        "attempts_until_recovery": attempts_until_recovery,
        "recovered_at": recovered_at,
        "recovery_status": recovery_status,
        "latest_status": "succeeded" if int(latest.get("success") or 0) == 1 else "failed",
        "latest_attempted_at": latest.get("attempted_at"),
        "attempt_count": len(attempts),
    }


def _totals(items: list[dict[str, Any]]) -> dict[str, Any]:
    recovery = Counter(item["recovery_status"] for item in items)
    return {
        "total": len(items),
        "by_platform": _counter(items, "platform"),
        "by_error_category": _counter(items, "error_category"),
        "by_recovery_status": {name: recovery.get(name, 0) for name in RECOVERY_STATUSES},
    }


def _counter(items: list[dict[str, Any]], field: str) -> dict[str, int]:
    return dict(sorted(Counter(str(item.get(field) or "(none)") for item in items).items()))


def _sort_key(item: dict[str, Any]) -> tuple[int, str, int]:
    rank = {"unrecovered": 0, "recovered": 1}
    return (rank[item["recovery_status"]], item["first_failed_at"] or "", item["content_id"])


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
