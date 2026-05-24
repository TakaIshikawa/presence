"""Export newsletter engagement cohorts by signup month and source."""

from __future__ import annotations

import sqlite3
from typing import Any

from evaluation._batch_report_utils import connection, csv_rows, dump_json, first_table, pick, schema, text

FIELDS = ["cohort_month", "signup_source", "subscriber_count", "active_count", "open_count", "click_count", "unsubscribe_count", "engagement_rate"]
DEFAULT_LIMIT = 100


def build_newsletter_engagement_cohort_export_from_db(db_or_conn: Any, *, since: str | None = None, until: str | None = None, limit: int = DEFAULT_LIMIT) -> dict[str, Any]:
    if limit <= 0:
        raise ValueError("limit must be positive")
    conn = connection(db_or_conn)
    sch = schema(conn)
    table = first_table(sch, ("newsletter_subscribers", "subscribers"))
    missing_tables = [] if table else ["newsletter_subscribers|subscribers"]
    diagnostics = []
    rows: list[dict[str, Any]] = []
    if table:
        cols = sch[table]
        if not {"created_at", "signup_at", "subscribed_at"} & cols:
            diagnostics.append("missing subscriber signup timestamp")
        else:
            rows = _subscriber_rows(conn, table, cols, since, until)
    events_table = first_table(sch, ("newsletter_engagement_events", "newsletter_send_events"))
    if not events_table:
        diagnostics.append("missing optional engagement events table")
    event_counts = _event_counts(conn, events_table, sch.get(events_table, set())) if events_table else {}
    groups: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        key = (text(row["cohort_month"]), text(row["signup_source"]) or "unknown")
        g = groups.setdefault(key, {"cohort_month": key[0], "signup_source": key[1], "subscriber_count": 0, "active_count": 0, "open_count": 0, "click_count": 0, "unsubscribe_count": 0})
        g["subscriber_count"] += 1
        if text(row.get("status")).lower() not in {"unsubscribed", "inactive", "disabled"}:
            g["active_count"] += 1
        counts = event_counts.get(str(row["subscriber_id"]), {})
        g["open_count"] += counts.get("open", 0)
        g["click_count"] += counts.get("click", 0)
        g["unsubscribe_count"] += counts.get("unsubscribe", 0)
    cohort_rows = sorted(groups.values(), key=lambda r: (r["cohort_month"], r["signup_source"]))[:limit]
    for row in cohort_rows:
        row["engagement_rate"] = round((row["open_count"] + row["click_count"]) / row["subscriber_count"], 3) if row["subscriber_count"] else 0
    return {"artifact_type": "newsletter_engagement_cohort_export", "filters": {"since": since, "until": until, "limit": limit}, "rows": cohort_rows, "diagnostics": diagnostics, "missing_tables": missing_tables, "empty_state": {"is_empty": not cohort_rows, "message": "No newsletter subscriber cohorts found." if not cohort_rows and not missing_tables else None}}


def build_newsletter_engagement_cohort_export(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return build_newsletter_engagement_cohort_export_from_db(*args, **kwargs)


def format_newsletter_engagement_cohort_export_json(export: dict[str, Any]) -> str:
    return dump_json(export)


def format_newsletter_engagement_cohort_export_csv(export: dict[str, Any]) -> str:
    return csv_rows(export["rows"], FIELDS)


def _subscriber_rows(conn: sqlite3.Connection, table: str, cols: set[str], since: str | None, until: str | None) -> list[dict[str, Any]]:
    ts = pick(cols, "created_at", "signup_at", "subscribed_at")
    source_expr = pick(cols, "signup_source", "source", default="'unknown'")
    status_expr = pick(cols, "status", default="'active'")
    select = [f"{pick(cols, 'id', 'subscriber_id', default='rowid')} AS subscriber_id", f"substr({ts}, 1, 7) AS cohort_month", f"{source_expr} AS signup_source", f"{status_expr} AS status"]
    where = []
    args = []
    if since:
        where.append(f"date({ts}) >= date(?)")
        args.append(since)
    if until:
        where.append(f"date({ts}) <= date(?)")
        args.append(until)
    sql = f"SELECT {', '.join(select)} FROM {table}" + (" WHERE " + " AND ".join(where) if where else "") + " ORDER BY cohort_month, signup_source, subscriber_id"
    return [dict(row) for row in conn.execute(sql, args)]


def _event_counts(conn: sqlite3.Connection, table: str, cols: set[str]) -> dict[str, dict[str, int]]:
    if not {"subscriber_id", "event_type"} <= cols:
        return {}
    counts: dict[str, dict[str, int]] = {}
    for row in conn.execute(f"SELECT subscriber_id, event_type FROM {table}"):
        bucket = counts.setdefault(str(row["subscriber_id"]), {})
        kind = text(row["event_type"]).lower()
        if kind in {"open", "opened"}:
            bucket["open"] = bucket.get("open", 0) + 1
        elif kind in {"click", "clicked"}:
            bucket["click"] = bucket.get("click", 0) + 1
        elif kind in {"unsubscribe", "unsubscribed"}:
            bucket["unsubscribe"] = bucket.get("unsubscribe", 0) + 1
    return counts
