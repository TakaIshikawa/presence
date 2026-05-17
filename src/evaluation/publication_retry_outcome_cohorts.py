"""Group publication retry attempts into outcome cohorts."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from statistics import median
from typing import Any


DEFAULT_LOOKBACK_DAYS = 30
COHORTS = ("recovered", "still_failing", "abandoned", "flaky")
SUCCESS_STATUSES = {"success", "succeeded", "published", "sent"}
FAIL_STATUSES = {"failed", "error", "timeout", "rejected"}
ABANDONED_STATUSES = {"abandoned", "cancelled", "canceled"}


def build_publication_retry_outcome_cohorts_report(
    rows: list[dict[str, Any]],
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    now: datetime | None = None,
) -> dict[str, Any]:
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=lookback_days)
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        attempted_at = _parse_datetime(_first(row, "attempted_at", "created_at", "updated_at"))
        if attempted_at is not None and attempted_at < cutoff:
            continue
        grouped[_text(_first(row, "content_id", "publication_id", "item_id", "id")) or "unknown"].append(
            {**row, "attempted_at": attempted_at.isoformat() if attempted_at else None}
        )
    cohorts = {name: {"count": 0, "representative_items": []} for name in COHORTS}
    attempts_to_recovery: list[int] = []
    for item_id, attempts in grouped.items():
        attempts.sort(key=lambda attempt: attempt.get("attempted_at") or "")
        cohort = _classify(attempts)
        status_list = [_status(attempt) for attempt in attempts]
        representative = {
            "content_id": item_id,
            "attempt_count": len(attempts),
            "first_status": status_list[0] if status_list else "unknown",
            "latest_status": status_list[-1] if status_list else "unknown",
            "cohort": cohort,
        }
        cohorts[cohort]["count"] += 1
        cohorts[cohort]["representative_items"].append(representative)
        if cohort in {"recovered", "flaky"}:
            attempts_to_recovery.append(_first_success_index(attempts))
    for cohort in cohorts.values():
        cohort["representative_items"] = cohort["representative_items"][:5]
    item_count = len(grouped)
    recovered_count = cohorts["recovered"]["count"] + cohorts["flaky"]["count"]
    return {
        "artifact_type": "publication_retry_outcome_cohorts",
        "generated_at": generated_at.isoformat(),
        "filters": {"lookback_days": lookback_days, "cutoff": cutoff.isoformat()},
        "totals": {
            "rows_scanned": len(rows),
            "item_count": item_count,
            "recovery_rate": round(recovered_count / item_count, 4) if item_count else 0,
            "median_attempts_to_recovery": median(attempts_to_recovery) if attempts_to_recovery else None,
        },
        "cohorts": cohorts,
        "empty_state": {"is_empty": item_count == 0, "message": "No publication retry attempts found." if item_count == 0 else None},
    }


def build_publication_retry_outcome_cohorts_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    return build_publication_retry_outcome_cohorts_report(_load_rows(conn, schema), **kwargs)


def format_publication_retry_outcome_cohorts_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_publication_retry_outcome_cohorts_text(report: dict[str, Any]) -> str:
    lines = [
        "Publication Retry Outcome Cohorts",
        f"Generated: {report['generated_at']}",
        f"Lookback days: {report['filters']['lookback_days']}",
        f"Totals: items={report['totals']['item_count']} recovery_rate={report['totals']['recovery_rate']}",
    ]
    if report["empty_state"]["is_empty"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    for name, cohort in report["cohorts"].items():
        lines.append(f"- {name}: {cohort['count']}")
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    table = "publication_attempts" if "publication_attempts" in schema else "pipeline_runs" if "pipeline_runs" in schema else None
    if table is None:
        return []
    columns = schema[table]
    selected = [
        _col(columns, "content_id", "publication_id", "item_id", "id") + " AS content_id",
        _col(columns, "status", "outcome", default="'unknown'") + " AS status",
        _col(columns, "attempt_number", "retry_count", default="NULL") + " AS attempt_number",
        _col(columns, "created_at", "attempted_at", "updated_at", default="NULL") + " AS attempted_at",
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM {table}").fetchall()]


def _classify(attempts: list[dict[str, Any]]) -> str:
    statuses = [_status(attempt) for attempt in attempts]
    if any(status in ABANDONED_STATUSES for status in statuses):
        return "abandoned"
    success_indexes = [index for index, status in enumerate(statuses) if status in SUCCESS_STATUSES]
    failure_indexes = [index for index, status in enumerate(statuses) if status in FAIL_STATUSES]
    if success_indexes and failure_indexes and max(failure_indexes) > min(success_indexes):
        return "flaky"
    if success_indexes and failure_indexes:
        return "recovered"
    if statuses and statuses[-1] in FAIL_STATUSES:
        return "still_failing"
    return "recovered" if success_indexes else "still_failing"


def _first_success_index(attempts: list[dict[str, Any]]) -> int:
    for index, attempt in enumerate(attempts, start=1):
        if _status(attempt) in SUCCESS_STATUSES:
            return index
    return len(attempts)


def _status(row: dict[str, Any]) -> str:
    return _text(_first(row, "status", "outcome")).lower()


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _col(columns: set[str], *names: str, default: str = "NULL") -> str:
    for name in names:
        if name in columns:
            return name
    return default


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _parse_datetime(value: Any) -> datetime | None:
    text = _text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
