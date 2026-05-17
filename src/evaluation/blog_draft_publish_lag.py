"""Measure lag between blog draft creation and publication."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import json
import sqlite3
from statistics import median
from typing import Any


DEFAULT_STALE_DAYS = 14


def build_blog_draft_publish_lag_report(
    rows: list[dict[str, Any]],
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
    now: datetime | None = None,
) -> dict[str, Any]:
    if stale_days < 0:
        raise ValueError("stale_days must be non-negative")
    generated_at = _utc(now or datetime.now(timezone.utc))
    draft_rows = []
    for row in rows:
        created_at = _parse_ts(_first(row, "draft_created_at", "created_at", "generated_at"))
        if created_at is None:
            continue
        published_at = _parse_ts(_first(row, "published_at", "publication_created_at"))
        draft_age = round((generated_at - created_at).total_seconds() / 86400, 2)
        publish_lag = round((published_at - created_at).total_seconds() / 86400, 2) if published_at else None
        status = "published" if published_at else "stale_unpublished" if draft_age >= stale_days else "draft"
        draft_rows.append(
            {
                "content_id": _text(_first(row, "content_id", "id")) or "unknown",
                "title": _text(_first(row, "title", "headline")) or None,
                "topic": _text(_first(row, "topic", "primary_topic")) or None,
                "source_type": _text(_first(row, "source_type")) or None,
                "draft_created_at": created_at.isoformat(),
                "published_at": published_at.isoformat() if published_at else None,
                "draft_age_days": draft_age,
                "publish_lag_days": publish_lag,
                "status": status,
            }
        )

    stale_rows = [row for row in draft_rows if row["status"] == "stale_unpublished"]
    published_lags = [row["publish_lag_days"] for row in draft_rows if row["publish_lag_days"] is not None]
    draft_rows.sort(key=lambda row: ({"stale_unpublished": 0, "draft": 1, "published": 2}[row["status"]], -(row["draft_age_days"] or 0), row["content_id"]))
    return {
        "artifact_type": "blog_draft_publish_lag",
        "generated_at": generated_at.isoformat(),
        "filters": {"stale_days": stale_days},
        "totals": {
            "rows_scanned": len(rows),
            "draft_count": len(draft_rows),
            "published_count": sum(1 for row in draft_rows if row["status"] == "published"),
            "stale_unpublished_count": len(stale_rows),
            "median_publish_lag_days": round(median(published_lags), 2) if published_lags else None,
            "max_publish_lag_days": max(published_lags) if published_lags else None,
        },
        "drafts": draft_rows,
        "stale_drafts": stale_rows,
        "summary_by_group": _summary_by_group(draft_rows, stale_days),
        "empty_state": {"is_empty": not stale_rows, "message": "No stale unpublished blog drafts found." if not stale_rows else None},
    }


def build_blog_draft_publish_lag_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    return build_blog_draft_publish_lag_report(_load_rows(conn, _schema(conn)), **kwargs)


def format_blog_draft_publish_lag_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_blog_draft_publish_lag_table(report: dict[str, Any]) -> str:
    lines = [
        "Blog Draft Publish Lag",
        f"Generated: {report['generated_at']}",
        f"Stale threshold: {report['filters']['stale_days']} days",
        (
            "Totals: "
            f"drafts={report['totals']['draft_count']} "
            f"published={report['totals']['published_count']} "
            f"stale={report['totals']['stale_unpublished_count']} "
            f"median_lag={report['totals']['median_publish_lag_days']}"
        ),
    ]
    if not report["drafts"]:
        lines.append("No blog draft rows found.")
        return "\n".join(lines)
    lines.extend(["", "content_id | status | draft_age_days | publish_lag_days | group"])
    for row in report["drafts"]:
        group = row["topic"] or row["source_type"] or "ungrouped"
        lines.append(f"{row['content_id']} | {row['status']} | {row['draft_age_days']} | {row['publish_lag_days']} | {group}")
    return "\n".join(lines)


format_blog_draft_publish_lag_text = format_blog_draft_publish_lag_table


def _summary_by_group(rows: list[dict[str, Any]], stale_days: int) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[row["topic"] or row["source_type"] or "ungrouped"].append(row)
    summary = []
    for group, group_rows in groups.items():
        lags = [row["publish_lag_days"] for row in group_rows if row["publish_lag_days"] is not None]
        summary.append(
            {
                "group": group,
                "count": len(group_rows),
                "median_lag_days": round(median(lags), 2) if lags else None,
                "max_lag_days": max(lags) if lags else None,
                "stale_threshold_count": sum(1 for row in group_rows if row["status"] == "stale_unpublished" and row["draft_age_days"] >= stale_days),
            }
        )
    return sorted(summary, key=lambda item: (-item["stale_threshold_count"], item["group"]))


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    if "generated_content" not in schema:
        return []
    cols = schema["generated_content"]
    if "id" not in cols:
        return []
    type_expr = _col(cols, "content_type", "type", "format", default="NULL")
    title_expr = _col(cols, "title", "headline", default="NULL")
    created_expr = _col(cols, "created_at", "generated_at", "updated_at", default="NULL")
    source_expr = _col(cols, "source_type", default="NULL")
    topic_select = "ct.topic" if "content_topics" in schema and {"content_id", "topic"} <= schema["content_topics"] else "NULL"
    topic_join = " LEFT JOIN content_topics ct ON ct.content_id = gc.id" if topic_select != "NULL" else ""
    pub = _publication_subquery(schema)
    pub_join = f" LEFT JOIN ({pub}) pub ON pub.content_id = gc.id" if pub else ""
    where = f"LOWER(COALESCE({type_expr}, '')) LIKE '%blog%'"
    sql = f"""SELECT gc.id AS content_id,
                     {title_expr} AS title,
                     {created_expr} AS draft_created_at,
                     pub.published_at AS published_at,
                     {topic_select} AS topic,
                     {source_expr} AS source_type
              FROM generated_content gc{topic_join}{pub_join}
              WHERE {where}
              GROUP BY gc.id"""
    return [dict(row) for row in conn.execute(sql).fetchall()]


def _publication_subquery(schema: dict[str, set[str]]) -> str | None:
    for table in ("content_publications", "blog_publications", "content_exports"):
        if table not in schema:
            continue
        cols = schema[table]
        content_expr = _col(cols, "content_id", "generated_content_id", "source_content_id", default="NULL")
        published_expr = _col(cols, "published_at", "created_at", "exported_at", default="NULL")
        channel_expr = _col(cols, "platform", "channel", "publication_type", default="NULL")
        if content_expr == "NULL":
            continue
        return (
            f"SELECT {content_expr} AS content_id, MIN({published_expr}) AS published_at "
            f"FROM {table} WHERE LOWER(COALESCE({channel_expr}, 'blog')) LIKE '%blog%' GROUP BY {content_expr}"
        )
    return None


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


def _parse_ts(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
