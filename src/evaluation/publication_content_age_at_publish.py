"""Report content age when publications go live."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_STALE_HOURS = 72.0


def build_publication_content_age_at_publish_report(
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
        """SELECT cp.id AS publication_id,
                  cp.content_id,
                  cp.platform,
                  cp.published_at,
                  gc.created_at,
                  COALESCE(gc.content_type, 'unknown') AS content_type,
                  COALESCE(gc.content_format, 'unknown') AS content_format
           FROM content_publications cp
           JOIN generated_content gc ON gc.id = cp.content_id
           WHERE LOWER(COALESCE(cp.status, '')) = 'published'
             AND cp.published_at IS NOT NULL
             AND gc.created_at IS NOT NULL
             AND datetime(cp.published_at) >= datetime(?)
             AND datetime(cp.published_at) <= datetime(?)
           ORDER BY datetime(cp.published_at) DESC, cp.id ASC""",
        (cutoff.isoformat(), generated_at.isoformat()),
    ).fetchall()
    stale = []
    groups: dict[tuple[str, str, str], list[float]] = defaultdict(list)
    included = 0
    for row in rows:
        created = _parse(row["created_at"])
        published = _parse(row["published_at"])
        if created is None or published is None:
            continue
        age_hours = (published - created).total_seconds() / 3600
        if age_hours < 0:
            continue
        included += 1
        key = (row["content_type"], row["platform"] or "unknown", row["content_format"])
        groups[key].append(age_hours)
        item = {
            "publication_id": int(row["publication_id"]),
            "content_id": int(row["content_id"]),
            "content_type": row["content_type"],
            "platform": row["platform"] or "unknown",
            "content_format": row["content_format"],
            "created_at": created.isoformat(),
            "published_at": published.isoformat(),
            "age_hours": round(age_hours, 2),
        }
        if age_hours > stale_hours:
            stale.append(item)
    stale.sort(key=lambda item: (-item["age_hours"], item["content_id"], item["platform"]))
    grouped_summaries = [
        {
            "content_type": key[0],
            "platform": key[1],
            "content_format": key[2],
            "publication_count": len(values),
            "average_age_hours": round(sum(values) / len(values), 2),
            "max_age_hours": round(max(values), 2),
            "stale_count": sum(1 for value in values if value > stale_hours),
        }
        for key, values in groups.items()
    ]
    grouped_summaries.sort(key=lambda item: (item["content_type"], item["platform"], item["content_format"]))
    return {
        "artifact_type": "publication_content_age_at_publish",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {"publication_count": included, "stale_publication_count": len(stale), "group_count": len(grouped_summaries)},
        "stale_publications": stale,
        "grouped_summaries": grouped_summaries,
        "missing_tables": [],
        "missing_columns": {},
    }


def format_publication_content_age_at_publish_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_publication_content_age_at_publish_text(report: dict[str, Any]) -> str:
    lines = [
        "Publication Content Age At Publish",
        f"Generated: {report['generated_at']}",
        f"Filters: days={report['filters']['days']} stale_hours={report['filters']['stale_hours']}",
        f"Totals: publications={report['totals']['publication_count']} stale={report['totals']['stale_publication_count']} groups={report['totals']['group_count']}",
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report.get("missing_columns"):
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if report["grouped_summaries"]:
        lines.extend(["", "Groups:"])
        for group in report["grouped_summaries"]:
            lines.append(
                f"- {group['content_type']}/{group['platform']}/{group['content_format']}: "
                f"count={group['publication_count']} avg_h={group['average_age_hours']} max_h={group['max_age_hours']} stale={group['stale_count']}"
            )
    if report["stale_publications"]:
        lines.extend(["", "Stale publications:"])
        for item in report["stale_publications"]:
            lines.append(f"- content={item['content_id']} platform={item['platform']} age_h={item['age_hours']} published={item['published_at']}")
    if not report["grouped_summaries"]:
        lines.append("No published content with usable timestamps found.")
    return "\n".join(lines)


format_publication_content_age_at_publish_table = format_publication_content_age_at_publish_text


def _schema_gaps(schema: dict[str, set[str]]) -> tuple[list[str], dict[str, list[str]]]:
    required = {
        "generated_content": {"id", "created_at"},
        "content_publications": {"id", "content_id", "platform", "status", "published_at"},
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
        "artifact_type": "publication_content_age_at_publish",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {"publication_count": 0, "stale_publication_count": 0, "group_count": 0},
        "stale_publications": [],
        "grouped_summaries": [],
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


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))
