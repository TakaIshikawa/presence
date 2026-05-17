"""Report stale knowledge linked to reply drafts."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_STALE_DAYS = 180


def build_reply_draft_knowledge_freshness_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    stale_days: int = DEFAULT_STALE_DAYS,
    now: datetime | None = None,
) -> dict[str, Any]:
    if days <= 0:
        raise ValueError("days must be positive")
    if stale_days <= 0:
        raise ValueError("stale_days must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {"days": days, "stale_days": stale_days, "window_start": cutoff.isoformat(), "window_end": generated_at.isoformat()}
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or missing_columns:
        return _empty(generated_at, filters, missing_tables, missing_columns)
    rows = conn.execute(
        """SELECT rq.id AS reply_queue_id,
                  rq.inbound_author AS inbound_author,
                  rq.detected_at AS detected_at,
                  k.id AS knowledge_id,
                  k.source_type AS source_type,
                  k.title AS title,
                  COALESCE(k.published_at, k.ingested_at) AS source_at
           FROM reply_queue rq
           JOIN reply_knowledge_links rkl ON rkl.reply_queue_id = rq.id
           JOIN knowledge k ON k.id = rkl.knowledge_id
           WHERE rq.detected_at IS NOT NULL
             AND datetime(rq.detected_at) >= datetime(?)
             AND datetime(rq.detected_at) <= datetime(?)
           ORDER BY datetime(rq.detected_at) DESC, rq.id ASC, k.id ASC""",
        (cutoff.isoformat(), generated_at.isoformat()),
    ).fetchall()
    stale = []
    author_counts: Counter[str] = Counter()
    source_counts: Counter[str] = Counter()
    for row in rows:
        detected = _parse(row["detected_at"])
        source_at = _parse(row["source_at"])
        if detected is None or source_at is None:
            continue
        age_days = (detected - source_at).total_seconds() / 86400
        if age_days <= stale_days:
            continue
        author = row["inbound_author"] or "unknown"
        source_type = row["source_type"] or "unknown"
        author_counts[author] += 1
        source_counts[source_type] += 1
        stale.append(
            {
                "reply_queue_id": int(row["reply_queue_id"]),
                "knowledge_id": int(row["knowledge_id"]),
                "inbound_author": author,
                "source_type": source_type,
                "title": row["title"],
                "detected_at": detected.isoformat(),
                "source_at": source_at.isoformat(),
                "source_age_days": round(age_days, 2),
            }
        )
    stale.sort(key=lambda item: (-item["source_age_days"], item["reply_queue_id"], item["knowledge_id"]))
    return {
        "artifact_type": "reply_draft_knowledge_freshness",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {"linked_source_count": len(rows), "stale_source_count": len(stale), "stale_draft_count": len({item["reply_queue_id"] for item in stale})},
        "stale_draft_examples": stale[:50],
        "source_type_breakdowns": dict(sorted(source_counts.items())),
        "author_breakdowns": dict(sorted(author_counts.items())),
        "missing_tables": [],
        "missing_columns": {},
    }


def format_reply_draft_knowledge_freshness_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_draft_knowledge_freshness_text(report: dict[str, Any]) -> str:
    t = report["totals"]
    lines = [
        "Reply Draft Knowledge Freshness",
        f"Generated: {report['generated_at']}",
        f"Filters: days={report['filters']['days']} stale_days={report['filters']['stale_days']}",
        f"Totals: linked={t['linked_source_count']} stale_sources={t['stale_source_count']} stale_drafts={t['stale_draft_count']}",
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report.get("missing_columns"):
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if report["stale_draft_examples"]:
        lines.extend(["", "Stale draft examples:"])
        for item in report["stale_draft_examples"]:
            lines.append(f"- reply={item['reply_queue_id']} author={item['inbound_author']} source={item['source_type']} age_d={item['source_age_days']} knowledge={item['knowledge_id']}")
    else:
        lines.append("No stale reply draft knowledge found.")
    return "\n".join(lines)


format_reply_draft_knowledge_freshness_table = format_reply_draft_knowledge_freshness_text


def _schema_gaps(schema: dict[str, set[str]]) -> tuple[list[str], dict[str, list[str]]]:
    required = {
        "reply_queue": {"id", "detected_at"},
        "reply_knowledge_links": {"reply_queue_id", "knowledge_id"},
        "knowledge": {"id"},
    }
    optional_required = {"knowledge": {"source_type", "published_at", "ingested_at"}}
    missing_tables = [table for table in required if table not in schema]
    missing_columns = {
        table: sorted(cols - schema[table])
        for table, cols in {**required, **optional_required}.items()
        if table in schema and cols - schema[table]
    }
    return missing_tables, missing_columns


def _empty(generated_at: datetime, filters: dict[str, Any], missing_tables: list[str], missing_columns: dict[str, list[str]]) -> dict[str, Any]:
    return {
        "artifact_type": "reply_draft_knowledge_freshness",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {"linked_source_count": 0, "stale_source_count": 0, "stale_draft_count": 0},
        "stale_draft_examples": [],
        "source_type_breakdowns": {},
        "author_breakdowns": {},
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
