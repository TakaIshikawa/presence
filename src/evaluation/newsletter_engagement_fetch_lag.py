"""Report missing or stale newsletter engagement fetches."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_GRACE_HOURS = 24.0
DEFAULT_STALE_AFTER_HOURS = 48.0
DEFAULT_LIMIT = 25
DEFAULT_STATUSES = ("sent",)

_ISSUE_RANK = {
    "missing_engagement_metrics": 0,
    "stale_engagement_metrics": 1,
    "engagement_without_send": 2,
}


def build_newsletter_engagement_fetch_lag_report(
    rows: list[dict[str, Any]],
    *,
    grace_hours: float = DEFAULT_GRACE_HOURS,
    stale_after_hours: float = DEFAULT_STALE_AFTER_HOURS,
    limit: int = DEFAULT_LIMIT,
    status_filters: tuple[str, ...] = DEFAULT_STATUSES,
    now: datetime | None = None,
    missing_tables: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Build a deterministic newsletter engagement fetch-lag report."""
    if grace_hours < 0:
        raise ValueError("grace_hours must be non-negative")
    if stale_after_hours <= 0:
        raise ValueError("stale_after_hours must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    statuses = tuple(sorted({status.lower() for status in status_filters if status}))
    send_rows = [_normalize_send(row) for row in rows if row.get("row_type", "send") == "send"]
    orphan_rows = [
        _normalize_orphan(row)
        for row in rows
        if row.get("row_type") == "engagement_without_send"
    ]
    eligible_sends = [row for row in send_rows if row["status"] in statuses]
    issue_items = _send_issues(
        eligible_sends,
        generated_at=generated_at,
        grace_hours=grace_hours,
        stale_after_hours=stale_after_hours,
    )
    issue_items.extend(_orphan_issue(row) for row in orphan_rows)
    issue_items.sort(key=_issue_sort_key)

    counts = {issue_type: 0 for issue_type in _ISSUE_RANK}
    for item in issue_items:
        counts[item["issue_type"]] += 1

    return {
        "artifact_type": "newsletter_engagement_fetch_lag",
        "generated_at": generated_at.isoformat(),
        "missing_tables": list(missing_tables),
        "thresholds": {
            "grace_hours": grace_hours,
            "stale_after_hours": stale_after_hours,
            "limit": limit,
            "status_filters": list(statuses),
        },
        "summary": {
            "send_rows_scanned": len(send_rows),
            "eligible_send_count": len(eligible_sends),
            "orphan_engagement_count": len(orphan_rows),
            "issue_count": len(issue_items),
            "missing_engagement_metrics_count": counts["missing_engagement_metrics"],
            "stale_engagement_metrics_count": counts["stale_engagement_metrics"],
            "engagement_without_send_count": counts["engagement_without_send"],
        },
        "issue_items": issue_items[:limit],
    }


def build_newsletter_engagement_fetch_lag_report_from_db(
    db_or_conn: Any,
    **kwargs: Any,
) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    required = ("newsletter_sends", "newsletter_engagement")
    missing_tables = tuple(table for table in required if table not in schema)
    if missing_tables:
        return build_newsletter_engagement_fetch_lag_report(
            [],
            missing_tables=missing_tables,
            **kwargs,
        )
    rows = _load_rows(conn, schema, status_filters=kwargs.get("status_filters", DEFAULT_STATUSES))
    return build_newsletter_engagement_fetch_lag_report(
        rows,
        missing_tables=missing_tables,
        **kwargs,
    )


def format_newsletter_engagement_fetch_lag_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_engagement_fetch_lag_text(report: dict[str, Any]) -> str:
    thresholds = report["thresholds"]
    summary = report["summary"]
    lines = [
        "Newsletter Engagement Fetch Lag",
        f"Generated: {report['generated_at']}",
        (
            f"Thresholds: grace_hours={thresholds['grace_hours']} "
            f"stale_after_hours={thresholds['stale_after_hours']} "
            f"limit={thresholds['limit']} statuses={','.join(thresholds['status_filters'])}"
        ),
        (
            f"Totals: sends={summary['send_rows_scanned']} eligible={summary['eligible_send_count']} "
            f"orphan_engagement={summary['orphan_engagement_count']} issues={summary['issue_count']} "
            f"missing={summary['missing_engagement_metrics_count']} "
            f"stale={summary['stale_engagement_metrics_count']} "
            f"orphan={summary['engagement_without_send_count']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["issue_items"]:
        lines.extend(["", "Issue items:"])
        for item in report["issue_items"]:
            lines.append(
                f"- type={item['issue_type']} send={item['newsletter_send_id'] or '-'} "
                f"issue={item['issue_id'] or '-'} status={item['status'] or '-'} "
                f"sent_at={item['sent_at'] or '-'} latest_fetched_at={item['latest_fetched_at'] or '-'}"
            )
    elif not report["missing_tables"]:
        lines.append("No newsletter engagement fetch lag issues found.")
    return "\n".join(lines)


def _load_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    status_filters: tuple[str, ...],
) -> list[dict[str, Any]]:
    send_rows = _load_send_rows(conn, schema, status_filters=status_filters)
    orphan_rows = _load_orphan_rows(conn, schema)
    return send_rows + orphan_rows


def _load_send_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    status_filters: tuple[str, ...],
) -> list[dict[str, Any]]:
    ns = schema["newsletter_sends"]
    ne = schema["newsletter_engagement"]
    selected = [
        "'send' AS row_type",
        _column_expr(ns, "id", "ns", "newsletter_send_id", default="ns.rowid"),
        _column_expr(ns, "issue_id", "ns", "issue_id", default="NULL"),
        _column_expr(ns, "subject", "ns", "subject", default="NULL"),
        _column_expr(ns, "status", "ns", "status", default="'sent'"),
        _column_expr(ns, "sent_at", "ns", "sent_at", default="NULL"),
        "MAX(ne.fetched_at) AS latest_fetched_at" if "fetched_at" in ne else "NULL AS latest_fetched_at",
        "COUNT(ne.rowid) AS engagement_row_count",
    ]
    join_parts = []
    if "newsletter_send_id" in ne and "id" in ns:
        join_parts.append("ne.newsletter_send_id = ns.id")
    if "issue_id" in ne and "issue_id" in ns:
        join_parts.append("(ne.newsletter_send_id IS NULL AND ne.issue_id = ns.issue_id)")
    join = " OR ".join(join_parts) if join_parts else "0"
    where = ""
    params: list[Any] = []
    if "status" in ns:
        statuses = tuple(status.lower() for status in status_filters)
        placeholders = ", ".join("?" for _ in statuses)
        where = f"WHERE LOWER(COALESCE(ns.status, 'sent')) IN ({placeholders})"
        params.extend(statuses)
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT {', '.join(selected)}
                FROM newsletter_sends ns
                LEFT JOIN newsletter_engagement ne ON {join}
                {where}
                GROUP BY ns.rowid
                ORDER BY datetime({_column_ref(ns, 'sent_at', 'ns', default='NULL')}) ASC, ns.rowid ASC""",
            params,
        ).fetchall()
    ]


def _load_orphan_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    ns = schema["newsletter_sends"]
    ne = schema["newsletter_engagement"]
    selected = [
        "'engagement_without_send' AS row_type",
        _column_expr(ne, "id", "ne", "engagement_id", default="ne.rowid"),
        _column_expr(ne, "newsletter_send_id", "ne", "newsletter_send_id", default="NULL"),
        _column_expr(ne, "issue_id", "ne", "issue_id", default="NULL"),
        _column_expr(ne, "fetched_at", "ne", "latest_fetched_at", default="NULL"),
    ]
    match_parts = []
    if "newsletter_send_id" in ne and "id" in ns:
        match_parts.append("ne.newsletter_send_id = ns.id")
    if "issue_id" in ne and "issue_id" in ns:
        match_parts.append("(ne.newsletter_send_id IS NULL AND ne.issue_id = ns.issue_id)")
    match = " OR ".join(match_parts) if match_parts else "0"
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT {', '.join(selected)}
                FROM newsletter_engagement ne
                LEFT JOIN newsletter_sends ns ON {match}
                WHERE ns.rowid IS NULL
                ORDER BY datetime({_column_ref(ne, 'fetched_at', 'ne', default='NULL')}) ASC, ne.rowid ASC"""
        ).fetchall()
    ]


def _send_issues(
    rows: list[dict[str, Any]],
    *,
    generated_at: datetime,
    grace_hours: float,
    stale_after_hours: float,
) -> list[dict[str, Any]]:
    issues = []
    for row in rows:
        sent_at = _parse_datetime(row.get("sent_at"))
        if sent_at is None or generated_at - sent_at < timedelta(hours=grace_hours):
            continue
        latest = _parse_datetime(row.get("latest_fetched_at"))
        if latest is None:
            issues.append(_send_issue(row, "missing_engagement_metrics", generated_at=generated_at))
        elif generated_at - latest > timedelta(hours=stale_after_hours):
            issues.append(_send_issue(row, "stale_engagement_metrics", generated_at=generated_at))
    return issues


def _send_issue(row: dict[str, Any], issue_type: str, *, generated_at: datetime) -> dict[str, Any]:
    sent_at = _parse_datetime(row.get("sent_at"))
    latest = _parse_datetime(row.get("latest_fetched_at"))
    return {
        "issue_type": issue_type,
        "newsletter_send_id": row["newsletter_send_id"],
        "engagement_id": None,
        "issue_id": row["issue_id"],
        "subject": row["subject"],
        "status": row["status"],
        "sent_at": sent_at.isoformat() if sent_at else None,
        "latest_fetched_at": latest.isoformat() if latest else None,
        "lag_hours": round((generated_at - sent_at).total_seconds() / 3600, 2) if sent_at else None,
        "metric_age_hours": round((generated_at - latest).total_seconds() / 3600, 2) if latest else None,
    }


def _orphan_issue(row: dict[str, Any]) -> dict[str, Any]:
    latest = _parse_datetime(row.get("latest_fetched_at"))
    return {
        "issue_type": "engagement_without_send",
        "newsletter_send_id": row["newsletter_send_id"],
        "engagement_id": row["engagement_id"],
        "issue_id": row["issue_id"],
        "subject": None,
        "status": None,
        "sent_at": None,
        "latest_fetched_at": latest.isoformat() if latest else None,
        "lag_hours": None,
        "metric_age_hours": None,
    }


def _normalize_send(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "newsletter_send_id": _maybe_int(row.get("newsletter_send_id")),
        "issue_id": str(row.get("issue_id") or ""),
        "subject": str(row.get("subject") or ""),
        "status": str(row.get("status") or "sent").strip().lower(),
        "sent_at": row.get("sent_at"),
        "latest_fetched_at": row.get("latest_fetched_at"),
        "engagement_row_count": _int(row.get("engagement_row_count")),
    }


def _normalize_orphan(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "engagement_id": _maybe_int(row.get("engagement_id")),
        "newsletter_send_id": _maybe_int(row.get("newsletter_send_id")),
        "issue_id": str(row.get("issue_id") or ""),
        "latest_fetched_at": row.get("latest_fetched_at"),
    }


def _issue_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    sent = _parse_datetime(item.get("sent_at"))
    fetched = _parse_datetime(item.get("latest_fetched_at"))
    primary_ts = sent or fetched
    timestamp = primary_ts.timestamp() if primary_ts else float("-inf")
    return (
        _ISSUE_RANK.get(item["issue_type"], 99),
        timestamp,
        item.get("newsletter_send_id") or 0,
        item.get("engagement_id") or 0,
        item.get("issue_id") or "",
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _column_expr(columns: set[str], name: str, alias: str, out: str, *, default: str) -> str:
    return f"{alias}.{name} AS {out}" if name in columns else f"{default} AS {out}"


def _column_ref(columns: set[str], name: str, alias: str, *, default: str) -> str:
    return f"{alias}.{name}" if name in columns else default


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _maybe_int(value: Any) -> int | None:
    if value is None:
        return None
    return _int(value)
