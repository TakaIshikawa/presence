"""Measure lag from failed publication attempts to later successful publication."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


def build_publication_recovery_lag_report(
    db_or_conn: Any,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Match failed and subsequent successful publication attempts by content/channel."""
    generated_at = _utc(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    attempts = _load_attempts(conn, schema)
    by_key: dict[tuple[int, str], list[dict[str, Any]]] = {}
    for attempt in attempts:
        by_key.setdefault((int(attempt["content_id"]), attempt["channel"]), []).append(attempt)

    rows = []
    for (content_id, channel), group in sorted(by_key.items()):
        group.sort(key=lambda item: (item["attempted_at_dt"], item["attempt_id"]))
        open_failure: dict[str, Any] | None = None
        for attempt in group:
            if attempt["success"]:
                if open_failure is not None and attempt["attempted_at_dt"] > open_failure["attempted_at_dt"]:
                    lag = _lag_hours(open_failure["attempted_at_dt"], attempt["attempted_at_dt"])
                    rows.append(
                        {
                            "content_id": content_id,
                            "channel": channel,
                            "first_failure_at": open_failure["attempted_at_dt"].isoformat(),
                            "recovery_at": attempt["attempted_at_dt"].isoformat(),
                            "lag_hours": lag,
                            "unrecovered": False,
                        }
                    )
                    open_failure = None
                continue
            if open_failure is None:
                open_failure = attempt
        if open_failure is not None:
            rows.append(
                {
                    "content_id": content_id,
                    "channel": channel,
                    "first_failure_at": open_failure["attempted_at_dt"].isoformat(),
                    "recovery_at": None,
                    "lag_hours": None,
                    "unrecovered": True,
                }
            )

    rows.sort(key=lambda row: (row["unrecovered"] is False, row["channel"], row["content_id"], row["first_failure_at"]))
    recovered_lags = [row["lag_hours"] for row in rows if row["lag_hours"] is not None]
    return {
        "artifact_type": "publication_recovery_lag",
        "generated_at": generated_at.isoformat(),
        "rows": rows,
        "schema_gaps": {"missing_tables": ["publication_attempts"] if "publication_attempts" not in schema else []},
        "summary": {
            "recovered_count": sum(1 for row in rows if not row["unrecovered"]),
            "unrecovered_count": sum(1 for row in rows if row["unrecovered"]),
            "average_recovery_lag_hours": round(sum(recovered_lags) / len(recovered_lags), 2) if recovered_lags else None,
        },
    }


def format_publication_recovery_lag_json(report: dict[str, Any]) -> str:
    """Render deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_publication_recovery_lag_text(report: dict[str, Any]) -> str:
    """Render a terminal table."""
    summary = report["summary"]
    avg = summary["average_recovery_lag_hours"]
    lines = [
        "Publication Recovery Lag",
        f"Generated: {report['generated_at']}",
        (
            f"Summary: recovered={summary['recovered_count']} "
            f"unrecovered={summary['unrecovered_count']} "
            f"average_lag_hours={avg if avg is not None else '-'}"
        ),
    ]
    if not report["rows"]:
        lines.extend(["", "No failed publication attempts found."])
        return "\n".join(lines)
    lines.extend(["", "Rows:", "content  channel       first_failure_at             recovery_at                  lag_hours  unrecovered"])
    for row in report["rows"]:
        lines.append(
            f"{row['content_id']:<8} {row['channel']:<13} {row['first_failure_at']:<28} "
            f"{row['recovery_at'] or '-':<28} {row['lag_hours'] if row['lag_hours'] is not None else '-':<10} {int(row['unrecovered'])}"
        )
    return "\n".join(lines)


def _load_attempts(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    columns = schema.get("publication_attempts")
    if not columns:
        return []
    content_col = _first(columns, ("content_id", "generated_content_id", "source_content_id"))
    channel_col = _first(columns, ("platform", "channel", "publication_type"))
    attempted_col = _first(columns, ("attempted_at", "created_at", "published_at"))
    success_col = _first(columns, ("success", "succeeded"))
    status_col = _first(columns, ("status", "state"))
    if not content_col or not channel_col or not attempted_col or "id" not in columns:
        return []
    rows = conn.execute(
        f"""SELECT id AS attempt_id,
                   {content_col} AS content_id,
                   {channel_col} AS channel,
                   {attempted_col} AS attempted_at,
                   {success_col if success_col else "NULL"} AS success,
                   {status_col if status_col else "NULL"} AS status
            FROM publication_attempts
            WHERE {content_col} IS NOT NULL
              AND {attempted_col} IS NOT NULL
            ORDER BY {content_col}, {channel_col}, {attempted_col}, id"""
    ).fetchall()
    attempts = []
    for row in rows:
        item = dict(row)
        attempted_at = _parse_ts(item.get("attempted_at"))
        if attempted_at is None:
            continue
        item["attempted_at_dt"] = attempted_at
        item["channel"] = str(item.get("channel") or "unknown")
        item["success"] = _success(item.get("success"), item.get("status"))
        attempts.append(item)
    return attempts


def _success(success: Any, status: Any) -> bool:
    if success is not None:
        return bool(success)
    return str(status or "").lower() in {"published", "success", "succeeded", "posted", "complete", "completed"}


def _lag_hours(start: datetime, end: datetime) -> float:
    return round((end - start).total_seconds() / 3600, 2)


def _parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _first(columns: set[str], names: tuple[str, ...]) -> str | None:
    return next((name for name in names if name in columns), None)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
