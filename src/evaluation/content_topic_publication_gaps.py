"""Report extracted content topics whose content has not been published."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_MIN_CONFIDENCE = 0.0
DEFAULT_LIMIT = 25


def build_content_topic_publication_gaps_report(
    rows: list[dict[str, Any]],
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    min_confidence: float = DEFAULT_MIN_CONFIDENCE,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: tuple[str, ...] = (),
) -> dict[str, Any]:
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")
    if not 0 <= min_confidence <= 1:
        raise ValueError("min_confidence must be between 0 and 1")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    lookback_start = generated_at - timedelta(days=lookback_days)
    normalized = [_normalize_row(row) for row in rows]
    orphan = [row for row in normalized if row["content_id"] is None]
    unpublished = [row for row in normalized if row["content_id"] is not None and row["published"] != 1]
    orphan.sort(key=_sort_key)
    unpublished.sort(key=_sort_key)
    return {
        "artifact_type": "content_topic_publication_gaps",
        "generated_at": generated_at.isoformat(),
        "thresholds": {
            "lookback_days": lookback_days,
            "lookback_start": lookback_start.isoformat(),
            "lookback_end": generated_at.isoformat(),
            "min_confidence": min_confidence,
            "limit": limit,
        },
        "missing_tables": list(missing_tables),
        "summary": {
            "topic_rows_scanned": len(rows),
            "orphan_topic_count": len(orphan),
            "unpublished_topic_count": len(unpublished),
            "group_count": len(_grouped_summaries(orphan + unpublished)),
        },
        "orphan_topic_rows": orphan[:limit],
        "unpublished_topic_rows": unpublished[:limit],
        "grouped_topic_summaries": _grouped_summaries(orphan + unpublished),
    }


def build_content_topic_publication_gaps_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    required = ("content_topics", "generated_content")
    missing_tables = tuple(table for table in required if table not in schema)
    if missing_tables:
        return build_content_topic_publication_gaps_report([], missing_tables=missing_tables, **kwargs)
    now = _utc(kwargs.get("now") or datetime.now(timezone.utc))
    lookback_days = kwargs.get("lookback_days", DEFAULT_LOOKBACK_DAYS)
    min_confidence = kwargs.get("min_confidence", DEFAULT_MIN_CONFIDENCE)
    cutoff = now - timedelta(days=lookback_days)
    rows = _load_rows(conn, schema, cutoff=cutoff, window_end=now, min_confidence=min_confidence)
    return build_content_topic_publication_gaps_report(
        rows,
        missing_tables=missing_tables,
        **{**kwargs, "now": now},
    )


def format_content_topic_publication_gaps_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_content_topic_publication_gaps_text(report: dict[str, Any]) -> str:
    thresholds = report["thresholds"]
    summary = report["summary"]
    lines = [
        "Content Topic Publication Gaps",
        f"Generated: {report['generated_at']}",
        f"Window: days={thresholds['lookback_days']} start={thresholds['lookback_start']} end={thresholds['lookback_end']}",
        f"Thresholds: min_confidence={thresholds['min_confidence']:.2f} limit={thresholds['limit']}",
        (
            f"Totals: scanned={summary['topic_rows_scanned']} orphan={summary['orphan_topic_count']} "
            f"unpublished={summary['unpublished_topic_count']} groups={summary['group_count']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    for label, key in (("Orphan topic rows", "orphan_topic_rows"), ("Unpublished topic rows", "unpublished_topic_rows")):
        if report[key]:
            lines.extend(["", f"{label}:"])
            for item in report[key]:
                lines.append(
                    f"- topic_id={item['topic_id']} content_id={item['content_id'] or '-'} "
                    f"topic={item['topic']} subtopic={item['subtopic']} confidence={item['confidence']:.2f}"
                )
    if not report["orphan_topic_rows"] and not report["unpublished_topic_rows"] and not report["missing_tables"]:
        lines.append("No content topic publication gaps found.")
    return "\n".join(lines)


def _load_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    window_end: datetime,
    min_confidence: float,
) -> list[dict[str, Any]]:
    ct = schema["content_topics"]
    gc = schema["generated_content"]
    content_fk = _topic_content_fk(ct)
    selected = [
        _column_expr(ct, "id", "ct", "topic_id", default="ct.rowid"),
        _column_expr(ct, "topic", "ct", "topic", default="'unknown'"),
        _column_expr(ct, "subtopic", "ct", "subtopic", default="NULL"),
        _column_expr(ct, "confidence", "ct", "confidence", default="1.0"),
        _column_expr(ct, "created_at", "ct", "topic_created_at", default="NULL"),
        _column_expr(gc, "id", "gc", "content_id", default="gc.rowid"),
        _column_expr(gc, "published", "gc", "published", default="0"),
        _column_expr(gc, "published_at", "gc", "published_at", default="NULL"),
        _column_expr(gc, "created_at", "gc", "content_created_at", default="NULL"),
    ]
    filters = []
    params: list[Any] = []
    if "confidence" in ct:
        filters.append("COALESCE(ct.confidence, 0) >= ?")
        params.append(min_confidence)
    if "created_at" in ct:
        filters.append("datetime(ct.created_at) >= datetime(?) AND datetime(ct.created_at) <= datetime(?)")
        params.extend([_sqlite_ts(cutoff), _sqlite_ts(window_end)])
    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT {', '.join(selected)}
                FROM content_topics ct
                LEFT JOIN generated_content gc ON gc.id = ct.{content_fk}
                {where}
                ORDER BY ct.id""",
            params,
        )
    ]


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "topic_id": _int(row.get("topic_id")),
        "content_id": _maybe_int(row.get("content_id")),
        "topic": _norm(row.get("topic")),
        "subtopic": _norm(row.get("subtopic")),
        "confidence": _float(row.get("confidence"), 0.0),
        "topic_created_at": row.get("topic_created_at"),
        "content_created_at": row.get("content_created_at"),
        "published": _int(row.get("published")),
        "published_at": row.get("published_at"),
    }


def _grouped_summaries(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(row["topic"], row["subtopic"])].append(row)
    summaries = []
    for (topic, subtopic), bucket in grouped.items():
        summaries.append(
            {
                "topic": topic,
                "subtopic": subtopic,
                "count": len(bucket),
                "orphan_count": sum(1 for row in bucket if row["content_id"] is None),
                "unpublished_count": sum(1 for row in bucket if row["content_id"] is not None and row["published"] != 1),
                "max_confidence": max(row["confidence"] for row in bucket),
            }
        )
    summaries.sort(key=lambda item: (-item["count"], item["topic"], item["subtopic"]))
    return summaries


def _topic_content_fk(columns: set[str]) -> str:
    for name in ("content_id", "generated_content_id", "generated_content_row_id"):
        if name in columns:
            return name
    return "content_id"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _column_expr(columns: set[str], name: str, alias: str, out: str, *, default: str) -> str:
    return f"{alias}.{name} AS {out}" if name in columns else f"{default} AS {out}"


def _sort_key(row: dict[str, Any]) -> tuple[str, str, int]:
    return (row["topic"], row["subtopic"], row["topic_id"])


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _sqlite_ts(value: datetime) -> str:
    return _utc(value).strftime("%Y-%m-%d %H:%M:%S")


def _norm(value: Any) -> str:
    text = "" if value is None else str(value).strip().lower()
    return text or "unknown"


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _maybe_int(value: Any) -> int | None:
    if value is None:
        return None
    return _int(value)


def _float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
