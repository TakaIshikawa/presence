"""Report selected newsletter subjects whose engagement outcomes are late."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any


DEFAULT_MIN_AGE_HOURS = 24
DEFAULT_STALE_AFTER_HOURS = 24
DEFAULT_LIMIT = 50


def build_newsletter_subject_outcome_lag_report(
    db_or_conn: Any,
    *,
    min_age_hours: int = DEFAULT_MIN_AGE_HOURS,
    stale_after_hours: int = DEFAULT_STALE_AFTER_HOURS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a read-only report for sent newsletters missing fresh metrics."""
    if min_age_hours <= 0:
        raise ValueError("min_age_hours must be positive")
    if stale_after_hours <= 0:
        raise ValueError("stale_after_hours must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _aware(now or datetime.now(timezone.utc))
    rows = _candidate_rows(conn, schema)
    items = []
    for row in rows:
        sent_at = _parse_timestamp(row.get("sent_at"))
        if sent_at is None:
            continue
        age_hours = (generated_at - sent_at).total_seconds() / 3600
        if age_hours < min_age_hours:
            continue
        latest_metrics_at = _parse_timestamp(row.get("latest_metrics_at"))
        if latest_metrics_at is None:
            lag_status = "absent"
        elif (generated_at - latest_metrics_at).total_seconds() / 3600 > stale_after_hours:
            lag_status = "stale"
        else:
            continue
        items.append(
            {
                "candidate_id": row["candidate_id"],
                "newsletter_send_id": row["newsletter_send_id"],
                "issue_id": row["issue_id"],
                "subject": row["subject"],
                "candidate_score": row["candidate_score"],
                "source": row["source"],
                "rank": row["rank"],
                "sent_at": sent_at.isoformat(),
                "age_hours": round(age_hours, 2),
                "lag_status": lag_status,
                "latest_metrics_at": latest_metrics_at.isoformat() if latest_metrics_at else None,
            }
        )
    items.sort(key=lambda item: (item["age_hours"], item["newsletter_send_id"] or 0), reverse=True)
    items = items[:limit]
    return {
        "artifact_type": "newsletter_subject_outcome_lag",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "min_age_hours": min_age_hours,
            "stale_after_hours": stale_after_hours,
            "limit": limit,
        },
        "totals": {
            "lagged_subject_count": len(items),
            "absent_count": sum(1 for item in items if item["lag_status"] == "absent"),
            "stale_count": sum(1 for item in items if item["lag_status"] == "stale"),
        },
        "items": items,
        "empty_state": {
            "is_empty": not items,
            "schema_present": all(table in schema for table in ("newsletter_subject_candidates", "newsletter_sends")),
            "message": "No newsletter subject outcome lag found." if not items else None,
        },
    }


def format_newsletter_subject_outcome_lag_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_subject_outcome_lag_text(report: dict[str, Any]) -> str:
    lines = [
        "Newsletter Subject Outcome Lag",
        f"Generated: {report['generated_at']}",
        (
            f"Min age: {report['filters']['min_age_hours']}h "
            f"stale after: {report['filters']['stale_after_hours']}h "
            f"limit={report['filters']['limit']}"
        ),
        (
            "Totals: "
            f"lagged={report['totals']['lagged_subject_count']} "
            f"absent={report['totals']['absent_count']} "
            f"stale={report['totals']['stale_count']}"
        ),
    ]
    if not report["items"]:
        lines.extend(["", report["empty_state"]["message"]])
        return "\n".join(lines)
    lines.extend(["", "Lagged subjects:"])
    for item in report["items"]:
        lines.append(
            f"- send={item['newsletter_send_id']} issue={item['issue_id'] or '-'} "
            f"candidate={item['candidate_id']} status={item['lag_status']} "
            f"age_h={item['age_hours']} metrics={item['latest_metrics_at'] or '-'} "
            f"score={item['candidate_score']} source={item['source']}: {item['subject']}"
        )
    return "\n".join(lines)


def _candidate_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    required = {"newsletter_subject_candidates", "newsletter_sends"}
    if not required.issubset(schema):
        return []
    if not {"id", "newsletter_send_id", "subject"}.issubset(schema["newsletter_subject_candidates"]):
        return []
    if not {"id", "sent_at"}.issubset(schema["newsletter_sends"]):
        return []
    has_engagement = "newsletter_engagement" in schema
    latest_join = ""
    latest_select = "NULL AS latest_metrics_at"
    if has_engagement and {"newsletter_send_id", "fetched_at"}.issubset(schema["newsletter_engagement"]):
        latest_join = """
            LEFT JOIN (
                SELECT newsletter_send_id, MAX(fetched_at) AS latest_metrics_at
                FROM newsletter_engagement
                GROUP BY newsletter_send_id
            ) ne ON ne.newsletter_send_id = ns.id
        """
        latest_select = "ne.latest_metrics_at AS latest_metrics_at"
    selected_filter = "COALESCE(nsc.selected, 0) = 1" if "selected" in schema["newsletter_subject_candidates"] else "1 = 1"
    status_filter = "LOWER(COALESCE(ns.status, 'sent')) = 'sent'" if "status" in schema["newsletter_sends"] else "1 = 1"
    rows = conn.execute(
        f"""SELECT nsc.id AS candidate_id,
                  nsc.newsletter_send_id AS newsletter_send_id,
                  COALESCE(nsc.issue_id, ns.issue_id) AS issue_id,
                  nsc.subject AS subject,
                  nsc.score AS candidate_score,
                  COALESCE(nsc.source, 'heuristic') AS source,
                  nsc.rank AS rank,
                  ns.sent_at AS sent_at,
                  {latest_select}
           FROM newsletter_subject_candidates nsc
           JOIN newsletter_sends ns ON ns.id = nsc.newsletter_send_id
           {latest_join}
           WHERE {selected_filter} AND {status_filter}
           ORDER BY ns.sent_at ASC, nsc.id ASC"""
    ).fetchall()
    return [dict(row) for row in rows]


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {
        row["name"]: {column["name"] for column in conn.execute(f"PRAGMA table_info({row['name']})")}
        for row in rows
    }


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _aware(parsed)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
