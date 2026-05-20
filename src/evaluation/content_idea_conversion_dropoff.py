"""Report content idea aging and conversion dropoff."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_STALE_DAYS = 14
DEFAULT_LOOKBACK_DAYS = 90
OPEN_STATUSES = {"open", "new", "pending", "backlog"}
PROMOTED_STATUSES = {"promoted", "approved", "planned"}
DISMISSED_STATUSES = {"dismissed", "rejected", "closed"}


def build_content_idea_conversion_dropoff_report(
    rows: list[dict[str, Any]],
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    priority: str | None = None,
    source: str | None = None,
    now: datetime | None = None,
    missing_tables: tuple[str, ...] = (),
) -> dict[str, Any]:
    if stale_days < 0:
        raise ValueError("stale_days must be non-negative")
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=lookback_days)
    priority_filter = _norm(priority, "") or None
    source_filter = _norm(source, "") or None
    normalized = [_normalize(row, generated_at) for row in rows]
    filtered = [
        row
        for row in normalized
        if (row["created_at_dt"] is None or row["created_at_dt"] >= cutoff)
        and (priority_filter is None or row["priority"] == priority_filter)
        and (source_filter is None or row["source"] == source_filter)
    ]
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in filtered:
        grouped[(row["topic"], row["source"], row["priority"])].append(row)
    summaries = [_summary(topic, source_name, priority_name, bucket, stale_days) for (topic, source_name, priority_name), bucket in grouped.items()]
    summaries.sort(key=lambda item: (-item["stale_open_count"], -item["open_count"], item["topic"], item["source"], item["priority"]))
    return {
        "artifact_type": "content_idea_conversion_dropoff",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "stale_days": stale_days,
            "lookback_days": lookback_days,
            "lookback_start": cutoff.isoformat(),
            "priority": priority_filter or "all",
            "source": source_filter or "all",
        },
        "summary": {
            "rows_scanned": len(rows),
            "eligible_rows": len(filtered),
            "group_count": len(summaries),
            "open_count": sum(1 for row in filtered if row["state"] == "open"),
            "promoted_count": sum(1 for row in filtered if row["state"] == "promoted"),
            "dismissed_count": sum(1 for row in filtered if row["state"] == "dismissed"),
        },
        "grouped_summaries": summaries,
        "missing_tables": list(missing_tables),
    }


def build_content_idea_conversion_dropoff_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing = tuple(table for table in ("content_ideas",) if table not in schema)
    rows = [] if missing else _load_rows(conn, schema["content_ideas"])
    return build_content_idea_conversion_dropoff_report(rows, missing_tables=missing, **kwargs)


def format_content_idea_conversion_dropoff_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_content_idea_conversion_dropoff_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Content Idea Conversion Dropoff",
        f"Generated: {report['generated_at']}",
        f"Filters: stale_days={report['filters']['stale_days']} lookback_days={report['filters']['lookback_days']} priority={report['filters']['priority']} source={report['filters']['source']}",
        f"Totals: rows={summary['eligible_rows']} open={summary['open_count']} promoted={summary['promoted_count']} dismissed={summary['dismissed_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["grouped_summaries"]:
        lines.extend(["", "topic | source | priority | open | stale_open | promoted | dismissed | promotion_rate | dismissal_rate | max_open_age_days"])
        for row in report["grouped_summaries"]:
            lines.append(
                f"{row['topic']} | {row['source']} | {row['priority']} | {row['open_count']} | {row['stale_open_count']} | "
                f"{row['promoted_count']} | {row['dismissed_count']} | {row['promotion_rate']} | {row['dismissal_rate']} | {row['max_open_age_days']}"
            )
    elif not report["missing_tables"]:
        lines.append("No content ideas found.")
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    select = [
        _expr(columns, "id", "id", "rowid"),
        _expr(columns, "topic", "topic", "'unknown'"),
        _expr(columns, "source", "source", _expr(columns, "discovery_source", "source", "'unknown'").split(" AS ")[0]),
        _expr(columns, "priority", "priority", "'unknown'"),
        _expr(columns, "status", "status", "'open'"),
        _expr(columns, "created_at", "created_at", "NULL"),
        _expr(columns, "promoted_at", "promoted_at", "NULL"),
        _expr(columns, "dismissed_at", "dismissed_at", "NULL"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM content_ideas ORDER BY rowid")]


def _normalize(row: dict[str, Any], now: datetime) -> dict[str, Any]:
    status = _norm(row.get("status"), "open")
    created_at = _parse_dt(row.get("created_at"))
    promoted_at = _parse_dt(row.get("promoted_at"))
    dismissed_at = _parse_dt(row.get("dismissed_at"))
    if status in PROMOTED_STATUSES or promoted_at is not None:
        state = "promoted"
    elif status in DISMISSED_STATUSES or dismissed_at is not None:
        state = "dismissed"
    else:
        state = "open"
    return {
        "id": row.get("id"),
        "topic": _norm(row.get("topic"), "unknown"),
        "source": _norm(row.get("source"), "unknown"),
        "priority": _norm(row.get("priority"), "unknown"),
        "state": state,
        "created_at_dt": created_at,
        "open_age_days": max(0, int((now - created_at).total_seconds() // 86400)) if created_at and state == "open" else None,
    }


def _summary(topic: str, source: str, priority: str, rows: list[dict[str, Any]], stale_days: int) -> dict[str, Any]:
    open_rows = [row for row in rows if row["state"] == "open"]
    promoted = sum(1 for row in rows if row["state"] == "promoted")
    dismissed = sum(1 for row in rows if row["state"] == "dismissed")
    total = len(rows)
    open_ages = [row["open_age_days"] for row in open_rows if row["open_age_days"] is not None]
    return {
        "topic": topic,
        "source": source,
        "priority": priority,
        "open_count": len(open_rows),
        "promoted_count": promoted,
        "dismissed_count": dismissed,
        "stale_open_count": sum(1 for age in open_ages if age >= stale_days),
        "promotion_rate": _rate(promoted, total),
        "dismissal_rate": _rate(dismissed, total),
        "max_open_age_days": max(open_ages) if open_ages else 0,
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {row[0]: {col[1] for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def _expr(columns: set[str], column: str, output: str, default: str) -> str:
    return f"{column} AS {output}" if column in columns else f"{default} AS {output}"


def _parse_dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _rate(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 4) if denominator else 0.0


def _norm(value: Any, default: str) -> str:
    text = "" if value is None else str(value).strip().lower()
    return text or default
