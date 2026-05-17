"""Report stale or missing newsletter engagement metric snapshots."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_STALE_HOURS = 24.0


def build_newsletter_engagement_metric_freshness_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    stale_hours: float = DEFAULT_STALE_HOURS,
    now: datetime | None = None,
) -> dict[str, Any]:
    if days <= 0:
        raise ValueError("days must be positive")
    if stale_hours <= 0:
        raise ValueError("stale_hours must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {"days": days, "stale_hours": stale_hours, "window_start": cutoff.isoformat(), "window_end": generated_at.isoformat()}
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or missing_columns:
        return _empty(generated_at, filters, missing_tables, missing_columns)

    rows = conn.execute(
        """SELECT ns.id AS newsletter_send_id,
                  ns.issue_id AS issue_id,
                  ns.subject AS subject,
                  ns.sent_at AS sent_at,
                  MAX(ne.fetched_at) AS latest_fetched_at
           FROM newsletter_sends ns
           LEFT JOIN newsletter_engagement ne ON ne.newsletter_send_id = ns.id
           WHERE datetime(ns.sent_at) >= datetime(?)
             AND datetime(ns.sent_at) <= datetime(?)
             AND LOWER(COALESCE(ns.status, 'sent')) = 'sent'
           GROUP BY ns.id, ns.issue_id, ns.subject, ns.sent_at
           ORDER BY datetime(ns.sent_at) DESC, ns.id ASC""",
        (cutoff.isoformat(), generated_at.isoformat()),
    ).fetchall()
    stale_issues = []
    missing_metric_issues = []
    fresh_count = 0
    for row in rows:
        latest = _parse(row["latest_fetched_at"])
        base = {
            "newsletter_send_id": int(row["newsletter_send_id"]),
            "issue_id": row["issue_id"],
            "subject": row["subject"],
            "sent_at": _iso(row["sent_at"]),
            "latest_fetched_at": latest.isoformat() if latest else None,
            "age_hours": round((generated_at - latest).total_seconds() / 3600, 2) if latest else None,
        }
        if latest is None:
            missing_metric_issues.append(base)
        elif (generated_at - latest).total_seconds() / 3600 > stale_hours:
            stale_issues.append(base)
        else:
            fresh_count += 1
    stale_issues.sort(key=lambda item: (-(item["age_hours"] or 0), item["newsletter_send_id"]))
    missing_metric_issues.sort(key=lambda item: (item["sent_at"] or "", item["newsletter_send_id"]), reverse=True)
    return {
        "artifact_type": "newsletter_engagement_metric_freshness",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "sent_issue_count": len(rows),
            "fresh_metric_count": fresh_count,
            "stale_issue_count": len(stale_issues),
            "missing_metric_count": len(missing_metric_issues),
        },
        "stale_issues": stale_issues,
        "missing_metric_issues": missing_metric_issues,
        "missing_tables": [],
        "missing_columns": {},
    }


def format_newsletter_engagement_metric_freshness_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_engagement_metric_freshness_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    lines = [
        "Newsletter Engagement Metric Freshness",
        f"Generated: {report['generated_at']}",
        f"Filters: days={report['filters']['days']} stale_hours={report['filters']['stale_hours']}",
        f"Totals: sent={totals['sent_issue_count']} fresh={totals['fresh_metric_count']} stale={totals['stale_issue_count']} missing={totals['missing_metric_count']}",
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report.get("missing_columns"):
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    for heading, key in (("Stale issues:", "stale_issues"), ("Missing metric issues:", "missing_metric_issues")):
        if report[key]:
            lines.extend(["", heading])
            for item in report[key]:
                lines.append(
                    f"- send={item['newsletter_send_id']} issue={item['issue_id'] or '-'} "
                    f"latest={item['latest_fetched_at'] or '-'} age_h={item['age_hours']}"
                )
    if not report["stale_issues"] and not report["missing_metric_issues"]:
        lines.append("No stale or missing newsletter engagement metrics found.")
    return "\n".join(lines)


format_newsletter_engagement_metric_freshness_table = format_newsletter_engagement_metric_freshness_text


def _schema_gaps(schema: dict[str, set[str]]) -> tuple[list[str], dict[str, list[str]]]:
    required = {
        "newsletter_sends": {"id", "issue_id", "subject", "sent_at"},
        "newsletter_engagement": {"newsletter_send_id", "fetched_at"},
    }
    missing_tables = [table for table in required if table not in schema]
    missing_columns = {
        table: sorted(columns - schema[table])
        for table, columns in required.items()
        if table in schema and columns - schema[table]
    }
    return missing_tables, missing_columns


def _empty(generated_at: datetime, filters: dict[str, Any], missing_tables: list[str], missing_columns: dict[str, list[str]]) -> dict[str, Any]:
    return {
        "artifact_type": "newsletter_engagement_metric_freshness",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {"sent_issue_count": 0, "fresh_metric_count": 0, "stale_issue_count": 0, "missing_metric_count": 0},
        "stale_issues": [],
        "missing_metric_issues": [],
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _parse(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _iso(value: Any) -> str | None:
    parsed = _parse(value)
    return parsed.isoformat() if parsed else None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))
