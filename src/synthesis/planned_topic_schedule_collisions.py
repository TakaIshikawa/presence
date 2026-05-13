"""Find schedule collisions and stale states in planned topics."""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
ACTIVE_CAMPAIGN_STATUSES = {"active", "planned"}


def build_planned_topic_schedule_collisions_report(
    db_or_conn: Any,
    *,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a read-only planned topic schedule collision report."""
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _as_utc(now or datetime.now(timezone.utc))
    today = generated_at.date()
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    filters = {"limit": limit, "today": today.isoformat()}
    missing_tables = [] if "planned_topics" in schema else ["planned_topics"]
    missing_columns = _missing_columns(schema)
    if missing_tables or missing_columns.get("planned_topics"):
        return _empty_report(generated_at, filters, missing_tables, missing_columns)

    rows = [_normalize_row(dict(row)) for row in _load_rows(conn, schema)]
    findings: list[dict[str, Any]] = []
    date_groups = _date_groups(rows)
    campaign_groups = _campaign_groups(rows)
    for group in date_groups:
        if group["count"] > 1:
            findings.append({
                "finding_type": "same_day_collision",
                "target_date": group["target_date"],
                "campaign_id": group["campaign_id"],
                "campaign_name": group["campaign_name"],
                "topic": group["topic"],
                "planned_topic_ids": group["planned_topic_ids"],
            })
    for row in rows:
        target = _parse_date(row["target_date"])
        active_campaign = row["campaign_status"] in ACTIVE_CAMPAIGN_STATUSES or row["campaign_status"] is None
        if row["status"] == "planned" and target is not None and target < today:
            findings.append(_finding("overdue_planned_topic", row))
        if row["status"] == "generated" and row["content_id"] is None:
            findings.append(_finding("generated_missing_content", row))
        if row["status"] == "skipped" and active_campaign:
            findings.append(_finding("skipped_active_campaign_topic", row))

    findings.sort(key=lambda item: (item["finding_type"], item.get("target_date") or "", item.get("planned_topic_id") or 0))
    finding_counts = dict(sorted(Counter(item["finding_type"] for item in findings).items()))
    return {
        "artifact_type": "planned_topic_schedule_collisions",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "planned_topic_count": len(rows),
            "finding_count": len(findings),
            "date_group_count": len(date_groups),
            "campaign_group_count": len(campaign_groups),
            "finding_counts": finding_counts,
        },
        "findings": findings[:limit],
        "date_groups": date_groups,
        "campaign_groups": campaign_groups,
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
    }


def format_planned_topic_schedule_collisions_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_planned_topic_schedule_collisions_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    lines = [
        "Planned Topic Schedule Collisions",
        f"Generated: {report['generated_at']}",
        f"Totals: topics={totals['planned_topic_count']} findings={totals['finding_count']}",
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[sqlite3.Row]:
    has_campaigns = "content_campaigns" in schema and {"id", "name", "status", "start_date", "end_date"}.issubset(schema["content_campaigns"])
    if has_campaigns and "campaign_id" in schema["planned_topics"]:
        return conn.execute(
            """SELECT pt.id AS planned_topic_id, pt.campaign_id, pt.topic, pt.angle,
                      pt.target_date, pt.status, pt.content_id, pt.created_at,
                      cc.name AS campaign_name, cc.status AS campaign_status,
                      cc.start_date AS campaign_start_date, cc.end_date AS campaign_end_date
               FROM planned_topics pt
               LEFT JOIN content_campaigns cc ON cc.id = pt.campaign_id
               ORDER BY pt.target_date, pt.campaign_id, pt.topic, pt.id"""
        ).fetchall()
    return conn.execute(
        """SELECT pt.id AS planned_topic_id, pt.campaign_id, pt.topic, pt.angle,
                  pt.target_date, pt.status, pt.content_id, pt.created_at,
                  NULL AS campaign_name, NULL AS campaign_status,
                  NULL AS campaign_start_date, NULL AS campaign_end_date
           FROM planned_topics pt
           ORDER BY pt.target_date, pt.campaign_id, pt.topic, pt.id"""
    ).fetchall()


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "planned_topic_id": int(row["planned_topic_id"]),
        "campaign_id": row.get("campaign_id"),
        "campaign_name": row.get("campaign_name"),
        "campaign_status": (row.get("campaign_status") or "").lower() or None,
        "campaign_start_date": row.get("campaign_start_date"),
        "campaign_end_date": row.get("campaign_end_date"),
        "topic": row.get("topic") or "unknown",
        "angle": row.get("angle"),
        "target_date": row.get("target_date"),
        "status": (row.get("status") or "planned").lower(),
        "content_id": row.get("content_id"),
    }


def _date_groups(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[Any, str, str], dict[str, Any]] = {}
    for row in rows:
        if not row["target_date"]:
            continue
        key = (row["campaign_id"], row["topic"], row["target_date"])
        group = groups.setdefault(
            key,
            {
                "target_date": row["target_date"],
                "campaign_id": row["campaign_id"],
                "campaign_name": row["campaign_name"],
                "topic": row["topic"],
                "count": 0,
                "planned_topic_ids": [],
            },
        )
        group["count"] += 1
        group["planned_topic_ids"].append(row["planned_topic_id"])
    return sorted(groups.values(), key=lambda item: (item["target_date"], item["campaign_id"] or 0, item["topic"]))


def _campaign_groups(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[Any, dict[str, Any]] = {}
    for row in rows:
        key = row["campaign_id"]
        group = groups.setdefault(
            key,
            {
                "campaign_id": key,
                "campaign_name": row["campaign_name"],
                "campaign_status": row["campaign_status"],
                "count": 0,
                "status_counts": {},
                "representative_planned_topic_ids": [],
            },
        )
        group["count"] += 1
        group["status_counts"][row["status"]] = group["status_counts"].get(row["status"], 0) + 1
        if len(group["representative_planned_topic_ids"]) < 5:
            group["representative_planned_topic_ids"].append(row["planned_topic_id"])
    return sorted(groups.values(), key=lambda item: (-(item["count"]), item["campaign_id"] or 0))


def _finding(kind: str, row: dict[str, Any]) -> dict[str, Any]:
    return {
        "finding_type": kind,
        "planned_topic_id": row["planned_topic_id"],
        "campaign_id": row["campaign_id"],
        "campaign_name": row["campaign_name"],
        "campaign_status": row["campaign_status"],
        "topic": row["topic"],
        "angle": row["angle"],
        "target_date": row["target_date"],
        "status": row["status"],
        "content_id": row["content_id"],
    }


def _empty_report(generated_at: datetime, filters: dict[str, Any], missing_tables: list[str], missing_columns: dict[str, list[str]]) -> dict[str, Any]:
    return {
        "artifact_type": "planned_topic_schedule_collisions",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {"planned_topic_count": 0, "finding_count": 0, "date_group_count": 0, "campaign_group_count": 0, "finding_counts": {}},
        "findings": [],
        "date_groups": [],
        "campaign_groups": [],
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
    }


def _missing_columns(schema: dict[str, set[str]]) -> dict[str, list[str]]:
    expected = {"planned_topics": {"id", "campaign_id", "topic", "angle", "target_date", "status", "content_id", "created_at"}}
    return {
        table: sorted(columns - schema.get(table, set()))
        for table, columns in expected.items()
        if table in schema and columns - schema.get(table, set())
    }


def _parse_date(raw: Any) -> date | None:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return date.fromisoformat(str(raw)[:10])
        except ValueError:
            return None


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {
        str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")}
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
