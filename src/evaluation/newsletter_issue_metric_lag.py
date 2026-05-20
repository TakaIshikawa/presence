"""Report lag between newsletter sends and engagement metric snapshots."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_FIRST_FETCH_SLA_HOURS = 6.0
DEFAULT_MAX_REFRESH_AGE_HOURS = 24.0
SENT_STATUSES = ("sent", "resonated", "low_resonance")


def build_newsletter_issue_metric_lag_report(
    db_or_conn: Any,
    *,
    first_fetch_sla_hours: float = DEFAULT_FIRST_FETCH_SLA_HOURS,
    max_refresh_age_hours: float = DEFAULT_MAX_REFRESH_AGE_HOURS,
    include_ok: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    if first_fetch_sla_hours <= 0:
        raise ValueError("first_fetch_sla_hours must be positive")
    if max_refresh_age_hours <= 0:
        raise ValueError("max_refresh_age_hours must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    filters = {
        "first_fetch_sla_hours": first_fetch_sla_hours,
        "max_refresh_age_hours": max_refresh_age_hours,
        "include_ok": include_ok,
        "sent_statuses": list(SENT_STATUSES),
    }
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or missing_columns:
        return _empty(generated_at, filters, missing_tables, missing_columns)

    rows = _load_rows(conn, schema)
    issues: list[dict[str, Any]] = []
    for row in rows:
        sent_at = _parse(row.get("sent_at"))
        first_fetched_at = _parse(row.get("first_fetched_at"))
        latest_fetched_at = _parse(row.get("latest_fetched_at"))
        first_fetch_lag_hours = (
            round((first_fetched_at - sent_at).total_seconds() / 3600, 2)
            if sent_at and first_fetched_at
            else None
        )
        refresh_age_hours = (
            round((generated_at - latest_fetched_at).total_seconds() / 3600, 2)
            if latest_fetched_at
            else None
        )
        gap_type = _gap_type(
            first_fetched_at=first_fetched_at,
            first_fetch_lag_hours=first_fetch_lag_hours,
            refresh_age_hours=refresh_age_hours,
            first_fetch_sla_hours=first_fetch_sla_hours,
            max_refresh_age_hours=max_refresh_age_hours,
        )
        if gap_type == "ok" and not include_ok:
            continue
        issues.append(
            {
                "issue_id": row.get("issue_id"),
                "newsletter_send_id": _int_or_none(row.get("newsletter_send_id")),
                "subject": row.get("subject"),
                "sent_at": sent_at.isoformat() if sent_at else None,
                "first_fetched_at": first_fetched_at.isoformat() if first_fetched_at else None,
                "latest_fetched_at": latest_fetched_at.isoformat() if latest_fetched_at else None,
                "first_fetch_lag_hours": first_fetch_lag_hours,
                "refresh_age_hours": refresh_age_hours,
                "gap_type": gap_type,
            }
        )

    issues.sort(key=_sort_key)
    by_gap_type = Counter(item["gap_type"] for item in issues)
    return {
        "artifact_type": "newsletter_issue_metric_lag",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "summary": {
            "sent_issue_count": len(rows),
            "reported_issue_count": len(issues),
            "ok_issue_count": sum(1 for row in rows if _row_gap_type(row, generated_at, first_fetch_sla_hours, max_refresh_age_hours) == "ok"),
            "by_gap_type": dict(sorted(by_gap_type.items())),
        },
        "issues": issues,
        "missing_tables": [],
        "missing_columns": {},
    }


def format_newsletter_issue_metric_lag_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_issue_metric_lag_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    filters = report["filters"]
    lines = [
        "Newsletter Issue Metric Lag",
        f"Generated: {report['generated_at']}",
        (
            f"Thresholds: first_fetch_sla_hours={filters['first_fetch_sla_hours']} "
            f"max_refresh_age_hours={filters['max_refresh_age_hours']}"
        ),
        (
            f"Totals: sent={summary['sent_issue_count']} reported={summary['reported_issue_count']} "
            f"ok={summary['ok_issue_count']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["issues"]:
        lines.append("No newsletter issue metric lag gaps found.")
        return "\n".join(lines)
    lines.extend(["", "issue_id | send_id | gap_type | sent_at | first_lag_h | refresh_age_h"])
    for item in report["issues"]:
        lines.append(
            f"{item['issue_id'] or '-'} | {item['newsletter_send_id'] or '-'} | "
            f"{item['gap_type']} | {item['sent_at'] or '-'} | "
            f"{item['first_fetch_lag_hours'] if item['first_fetch_lag_hours'] is not None else '-'} | "
            f"{item['refresh_age_hours'] if item['refresh_age_hours'] is not None else '-'}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    send_cols = schema["newsletter_sends"]
    engagement_cols = schema["newsletter_engagement"]
    status_filter = (
        f"LOWER(COALESCE(ns.status, 'sent')) IN ({', '.join('?' for _ in SENT_STATUSES)})"
        if "status" in send_cols
        else "1 = 1"
    )
    subject_expr = "ns.subject" if "subject" in send_cols else "NULL"
    issue_expr = "ns.issue_id" if "issue_id" in send_cols else "NULL"
    group_issue = "ns.issue_id" if "issue_id" in send_cols else "NULL"
    group_subject = "ns.subject" if "subject" in send_cols else "NULL"
    join_parts = []
    if "newsletter_send_id" in engagement_cols:
        join_parts.append("ne.newsletter_send_id = ns.id")
    if "issue_id" in engagement_cols and "issue_id" in send_cols:
        if "newsletter_send_id" in engagement_cols:
            join_parts.append("(ne.newsletter_send_id IS NULL AND ne.issue_id = ns.issue_id)")
        else:
            join_parts.append("ne.issue_id = ns.issue_id")
    join_condition = " OR ".join(join_parts)
    params: tuple[str, ...] = SENT_STATUSES if "status" in send_cols else ()
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT ns.id AS newsletter_send_id,
                      {issue_expr} AS issue_id,
                      {subject_expr} AS subject,
                      ns.sent_at AS sent_at,
                      MIN(ne.fetched_at) AS first_fetched_at,
                      MAX(ne.fetched_at) AS latest_fetched_at
               FROM newsletter_sends ns
               LEFT JOIN newsletter_engagement ne ON {join_condition}
               WHERE ns.sent_at IS NOT NULL
                 AND {status_filter}
               GROUP BY ns.id, {group_issue}, {group_subject}, ns.sent_at
               ORDER BY datetime(ns.sent_at) DESC, ns.id ASC""",
            params,
        ).fetchall()
    ]


def _schema_gaps(schema: dict[str, set[str]]) -> tuple[list[str], dict[str, list[str]]]:
    required = {
        "newsletter_sends": {"id", "sent_at"},
        "newsletter_engagement": {"fetched_at"},
    }
    missing_tables = [table for table in required if table not in schema]
    missing_columns = {
        table: sorted(columns - schema[table])
        for table, columns in required.items()
        if table in schema and columns - schema[table]
    }
    if "newsletter_engagement" in schema and not (
        {"newsletter_send_id", "issue_id"} & schema["newsletter_engagement"]
    ):
        missing_columns["newsletter_engagement"] = sorted(
            set(missing_columns.get("newsletter_engagement", [])) | {"newsletter_send_id_or_issue_id"}
        )
    return missing_tables, missing_columns


def _empty(
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: list[str],
    missing_columns: dict[str, list[str]],
) -> dict[str, Any]:
    return {
        "artifact_type": "newsletter_issue_metric_lag",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "summary": {
            "sent_issue_count": 0,
            "reported_issue_count": 0,
            "ok_issue_count": 0,
            "by_gap_type": {},
        },
        "issues": [],
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
    }


def _row_gap_type(
    row: dict[str, Any],
    now: datetime,
    first_fetch_sla_hours: float,
    max_refresh_age_hours: float,
) -> str:
    sent_at = _parse(row.get("sent_at"))
    first_fetched_at = _parse(row.get("first_fetched_at"))
    latest_fetched_at = _parse(row.get("latest_fetched_at"))
    lag = (
        round((first_fetched_at - sent_at).total_seconds() / 3600, 2)
        if sent_at and first_fetched_at
        else None
    )
    age = round((now - latest_fetched_at).total_seconds() / 3600, 2) if latest_fetched_at else None
    return _gap_type(
        first_fetched_at=first_fetched_at,
        first_fetch_lag_hours=lag,
        refresh_age_hours=age,
        first_fetch_sla_hours=first_fetch_sla_hours,
        max_refresh_age_hours=max_refresh_age_hours,
    )


def _gap_type(
    *,
    first_fetched_at: datetime | None,
    first_fetch_lag_hours: float | None,
    refresh_age_hours: float | None,
    first_fetch_sla_hours: float,
    max_refresh_age_hours: float,
) -> str:
    if first_fetched_at is None:
        return "no_metrics"
    if first_fetch_lag_hours is not None and first_fetch_lag_hours > first_fetch_sla_hours:
        return "first_fetch_sla_breach"
    if refresh_age_hours is not None and refresh_age_hours > max_refresh_age_hours:
        return "stale_refresh"
    return "ok"


def _sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    severity_rank = {"no_metrics": 0, "first_fetch_sla_breach": 1, "stale_refresh": 2, "ok": 3}
    return (
        severity_rank.get(item["gap_type"], 99),
        item["sent_at"] or "",
        item["newsletter_send_id"] or 0,
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {
        str(row[0]): {
            str(column[1])
            for column in conn.execute(f"PRAGMA table_info({row[0]})").fetchall()
        }
        for row in rows
    }


def _parse(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))
