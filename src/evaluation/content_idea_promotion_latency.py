"""Measure content idea promotion and dismissal latency."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_STALE_DAYS = 14
DEFAULT_LIMIT = 25
IDEA_CLASSES = ("fresh_open", "stale_open", "promoted", "dismissed")
REQUIRED_COLUMNS = {"id", "note", "status", "priority", "topic", "created_at", "updated_at"}


def build_content_idea_promotion_latency_report(
    db_or_conn: Any,
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    stale_days: int = DEFAULT_STALE_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return content idea status and latency buckets."""

    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")
    if stale_days < 0:
        raise ValueError("stale_days must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=lookback_days)
    filters = {
        "lookback_days": lookback_days,
        "stale_days": stale_days,
        "limit": limit,
        "lookback_start": cutoff.isoformat(),
        "lookback_end": generated_at.isoformat(),
    }
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "content_ideas" not in schema:
        return _empty_report(generated_at, filters, missing_tables=["content_ideas"])
    missing = sorted(REQUIRED_COLUMNS - schema["content_ideas"])
    if missing:
        return _empty_report(generated_at, filters, missing_columns={"content_ideas": missing})

    rows = _rows(conn, cutoff, generated_at)
    items = [_item(row, stale_days=stale_days, now=generated_at) for row in rows]
    items.sort(key=_sort_key)
    return {
        "artifact_type": "content_idea_promotion_latency",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": _totals(items),
        "items": items[:limit],
        "missing_tables": [],
        "missing_columns": {},
    }


def format_content_idea_promotion_latency_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_content_idea_promotion_latency_text(report: dict[str, Any]) -> str:
    filters = report["filters"]
    totals = report["totals"]
    lines = [
        "Content Idea Promotion Latency",
        f"Generated: {report['generated_at']}",
        (
            f"Filters: lookback_days={filters['lookback_days']} "
            f"stale_days={filters['stale_days']} limit={filters['limit']}"
        ),
        (
            "Status counts: "
            + " ".join(f"{name}={totals['status_counts'][name]}" for name in IDEA_CLASSES)
            + f" stale_open={totals['stale_open_count']}"
        ),
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report.get("missing_columns"):
        lines.append(
            "Missing columns: "
            + "; ".join(
                f"{table}({', '.join(columns)})"
                for table, columns in sorted(report["missing_columns"].items())
            )
        )
    if not report["items"]:
        lines.append("No content ideas matched the lookback window.")
        return "\n".join(lines)
    lines.append("")
    lines.append("Items:")
    for item in report["items"]:
        lines.append(
            f"- #{item['id']} {item['classification']} priority={item['priority']} "
            f"topic={item['topic'] or '-'} latency={item['latency_days']}d "
            f"bucket={item['latency_bucket']}"
        )
    return "\n".join(lines)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    try:
        tables = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    except sqlite3.Error:
        return {}
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in tables}


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    *,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    return {
        "artifact_type": "content_idea_promotion_latency",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "total": 0,
            "status_counts": {name: 0 for name in IDEA_CLASSES},
            "priority_counts": {},
            "topic_counts": {},
            "latency_bucket_counts": {},
            "stale_open_count": 0,
        },
        "items": [],
        "missing_tables": missing_tables or [],
        "missing_columns": missing_columns or {},
    }


def _rows(conn: sqlite3.Connection, cutoff: datetime, generated_at: datetime) -> list[dict[str, Any]]:
    cursor = conn.execute(
        """SELECT id, note, status, priority, topic, created_at, updated_at
           FROM content_ideas
           WHERE datetime(created_at) >= datetime(?)
             AND datetime(created_at) <= datetime(?)
           ORDER BY datetime(created_at) DESC, id DESC""",
        (cutoff.isoformat(), generated_at.isoformat()),
    )
    return [dict(row) for row in cursor.fetchall()]


def _item(row: dict[str, Any], *, stale_days: int, now: datetime) -> dict[str, Any]:
    status = _clean(row.get("status")) or "open"
    created_at = _parse(row.get("created_at")) or now
    updated_at = _parse(row.get("updated_at")) or created_at
    if status == "promoted":
        classification = "promoted"
        latency_days = _days_between(created_at, updated_at)
    elif status == "dismissed":
        classification = "dismissed"
        latency_days = _days_between(created_at, updated_at)
    else:
        latency_days = _days_between(created_at, now)
        classification = "stale_open" if latency_days >= stale_days else "fresh_open"
    return {
        "id": int(row["id"]),
        "status": status,
        "classification": classification,
        "priority": _clean(row.get("priority")) or "normal",
        "topic": row.get("topic"),
        "latency_days": latency_days,
        "latency_bucket": _latency_bucket(latency_days),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
        "note_preview": (row.get("note") or "")[:96],
    }


def _totals(items: list[dict[str, Any]]) -> dict[str, Any]:
    status_counts = Counter(item["classification"] for item in items)
    return {
        "total": len(items),
        "status_counts": {name: status_counts.get(name, 0) for name in IDEA_CLASSES},
        "priority_counts": dict(sorted(Counter(item["priority"] for item in items).items())),
        "topic_counts": dict(sorted(Counter(item["topic"] or "(none)" for item in items).items())),
        "latency_bucket_counts": dict(sorted(Counter(item["latency_bucket"] for item in items).items())),
        "stale_open_count": status_counts.get("stale_open", 0),
    }


def _latency_bucket(days: int) -> str:
    if days <= 1:
        return "0-1d"
    if days <= 3:
        return "2-3d"
    if days <= 7:
        return "4-7d"
    if days <= 14:
        return "8-14d"
    return "15d+"


def _sort_key(item: dict[str, Any]) -> tuple[int, int, int]:
    rank = {"stale_open": 0, "fresh_open": 1, "promoted": 2, "dismissed": 3}
    return (rank[item["classification"]], -item["latency_days"], item["id"])


def _days_between(start: datetime, end: datetime) -> int:
    return max(0, int((end - start).total_seconds() // 86400))


def _parse(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        text = str(value).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return _as_utc(parsed)


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    return text or None


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
