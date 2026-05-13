"""Analyze content topic saturation and topic assignment quality."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 90
DEFAULT_STALE_AFTER_DAYS = 30
DEFAULT_OVERUSED_TOPIC_COUNT = 3
DEFAULT_LOW_CONFIDENCE_THRESHOLD = 0.6
DEFAULT_LIMIT = 100


def build_content_topic_saturation_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    stale_after_days: int = DEFAULT_STALE_AFTER_DAYS,
    overused_topic_count: int = DEFAULT_OVERUSED_TOPIC_COUNT,
    low_confidence_threshold: float = DEFAULT_LOW_CONFIDENCE_THRESHOLD,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a read-only content topic saturation report."""
    if days <= 0 or stale_after_days <= 0 or overused_topic_count <= 0 or limit <= 0:
        raise ValueError("days, stale_after_days, overused_topic_count, and limit must be positive")
    if low_confidence_threshold < 0:
        raise ValueError("low_confidence_threshold must be non-negative")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    stale_cutoff = generated_at - timedelta(days=stale_after_days)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    filters = {
        "days": days,
        "stale_after_days": stale_after_days,
        "overused_topic_count": overused_topic_count,
        "low_confidence_threshold": low_confidence_threshold,
        "limit": limit,
    }
    missing_tables = [table for table in ("content_topics",) if table not in schema]
    missing_columns = _missing_columns(schema)
    if missing_tables or missing_columns.get("content_topics"):
        return _empty_report(generated_at, filters, missing_tables, missing_columns)

    rows = [_row_dict(row) for row in _load_rows(conn, schema, cutoff)]
    topic_groups = _topic_groups(rows)
    findings: list[dict[str, Any]] = []
    for group in topic_groups:
        if group["content_count"] >= overused_topic_count:
            findings.append({
                "finding_type": "overused_topic",
                "topic": group["topic"],
                "subtopic": group["subtopic"],
                "count": group["content_count"],
                "representative_content_ids": group["representative_content_ids"],
            })
        latest = _parse_timestamp(group["latest_published_at"] or group["latest_created_at"])
        if latest is None or latest < stale_cutoff:
            findings.append({
                "finding_type": "stale_topic",
                "topic": group["topic"],
                "subtopic": group["subtopic"],
                "latest_published_at": group["latest_published_at"],
                "latest_created_at": group["latest_created_at"],
                "representative_content_ids": group["representative_content_ids"],
            })
    for row in rows:
        if row["confidence"] is not None and row["confidence"] < low_confidence_threshold:
            findings.append({
                "finding_type": "low_confidence_topic",
                "content_topic_id": row["content_topic_id"],
                "content_id": row["content_id"],
                "topic": row["topic"],
                "subtopic": row["subtopic"],
                "confidence": row["confidence"],
            })
        if row["generated_content_missing"]:
            findings.append({
                "finding_type": "orphan_topic_assignment",
                "content_topic_id": row["content_topic_id"],
                "content_id": row["content_id"],
                "topic": row["topic"],
                "subtopic": row["subtopic"],
            })

    findings.sort(key=lambda item: (item["finding_type"], item.get("topic") or "", item.get("content_topic_id") or 0))
    return {
        "artifact_type": "content_topic_saturation",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "topic_assignment_count": len(rows),
            "topic_group_count": len(topic_groups),
            "finding_count": len(findings),
            "orphan_topic_assignment_count": sum(1 for row in rows if row["generated_content_missing"]),
        },
        "topic_groups": topic_groups,
        "findings": findings[:limit],
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
    }


def format_content_topic_saturation_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_content_topic_saturation_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    lines = [
        "Content Topic Saturation",
        f"Generated: {report['generated_at']}",
        f"Totals: assignments={totals['topic_assignment_count']} groups={totals['topic_group_count']} findings={totals['finding_count']}",
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    return "\n".join(lines)


def _empty_report(generated_at: datetime, filters: dict[str, Any], missing_tables: list[str], missing_columns: dict[str, list[str]]) -> dict[str, Any]:
    return {
        "artifact_type": "content_topic_saturation",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {"topic_assignment_count": 0, "topic_group_count": 0, "finding_count": 0, "orphan_topic_assignment_count": 0},
        "topic_groups": [],
        "findings": [],
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
    }


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]], cutoff: datetime) -> list[sqlite3.Row]:
    has_content = "generated_content" in schema and {"id", "content_type", "published_at", "created_at"}.issubset(schema["generated_content"])
    if has_content:
        return conn.execute(
            """SELECT ct.id AS content_topic_id, ct.content_id, ct.topic, ct.subtopic, ct.confidence,
                      ct.created_at AS topic_created_at, gc.id AS generated_content_id,
                      gc.content_type, gc.published_at, gc.created_at AS content_created_at
               FROM content_topics ct
               LEFT JOIN generated_content gc ON gc.id = ct.content_id
               WHERE datetime(ct.created_at) >= datetime(?)
               ORDER BY ct.topic, ct.subtopic, ct.id""",
            (cutoff.isoformat(),),
        ).fetchall()
    return conn.execute(
        """SELECT ct.id AS content_topic_id, ct.content_id, ct.topic, ct.subtopic, ct.confidence,
                  ct.created_at AS topic_created_at, NULL AS generated_content_id,
                  NULL AS content_type, NULL AS published_at, NULL AS content_created_at
           FROM content_topics ct
           WHERE datetime(ct.created_at) >= datetime(?)
           ORDER BY ct.topic, ct.subtopic, ct.id""",
        (cutoff.isoformat(),),
    ).fetchall()


def _row_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = dict(row)
    return {
        "content_topic_id": int(data["content_topic_id"]),
        "content_id": data.get("content_id"),
        "topic": data.get("topic") or "unknown",
        "subtopic": data.get("subtopic") or "",
        "confidence": _float_or_none(data.get("confidence")),
        "content_type": data.get("content_type"),
        "published_at": data.get("published_at"),
        "content_created_at": data.get("content_created_at"),
        "generated_content_missing": data.get("generated_content_id") is None,
    }


def _topic_groups(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        key = (row["topic"], row["subtopic"])
        group = groups.setdefault(
            key,
            {
                "topic": row["topic"],
                "subtopic": row["subtopic"],
                "assignment_count": 0,
                "content_count": 0,
                "content_types": {},
                "average_confidence": 0.0,
                "latest_published_at": None,
                "latest_created_at": None,
                "representative_content_ids": [],
            },
        )
        group["assignment_count"] += 1
        if row["content_id"] is not None:
            group["content_count"] += 1
            if len(group["representative_content_ids"]) < 5:
                group["representative_content_ids"].append(row["content_id"])
        ctype = row.get("content_type") or "missing"
        group["content_types"][ctype] = group["content_types"].get(ctype, 0) + 1
        group["_confidence_sum"] = group.get("_confidence_sum", 0.0) + (row["confidence"] or 0.0)
        group["_confidence_count"] = group.get("_confidence_count", 0) + (1 if row["confidence"] is not None else 0)
        group["latest_published_at"] = _latest(group["latest_published_at"], row.get("published_at"))
        group["latest_created_at"] = _latest(group["latest_created_at"], row.get("content_created_at"))
    output = []
    for group in groups.values():
        count = group.pop("_confidence_count", 0)
        total = group.pop("_confidence_sum", 0.0)
        group["average_confidence"] = round(total / count, 4) if count else None
        output.append(group)
    return sorted(output, key=lambda item: (-item["assignment_count"], item["topic"], item["subtopic"]))


def _latest(a: Any, b: Any) -> Any:
    if not a:
        return b
    if not b:
        return a
    return max(a, b)


def _missing_columns(schema: dict[str, set[str]]) -> dict[str, list[str]]:
    expected = {"content_topics": {"id", "content_id", "topic", "subtopic", "confidence", "created_at"}}
    return {
        table: sorted(columns - schema.get(table, set()))
        for table, columns in expected.items()
        if table in schema and columns - schema.get(table, set())
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {
        str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")}
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _as_utc(parsed)


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
