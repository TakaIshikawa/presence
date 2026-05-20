"""Report missing, weak, and duplicate generated content topic assignments."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_WINDOW_DAYS = 30
DEFAULT_CONFIDENCE_THRESHOLD = 0.6
DEFAULT_LIMIT = 100


def build_content_topic_assignment_gaps_report(
    content_rows: list[dict[str, Any]],
    topic_rows: list[dict[str, Any]],
    *,
    window_days: int = DEFAULT_WINDOW_DAYS,
    confidence_threshold: float = DEFAULT_CONFIDENCE_THRESHOLD,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic topic-assignment gap report from loaded rows."""
    if window_days <= 0:
        raise ValueError("window_days must be positive")
    if confidence_threshold < 0:
        raise ValueError("confidence_threshold must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    window_start = generated_at - timedelta(days=window_days)
    assignments_by_content: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in topic_rows:
        content_id = _int_or_none(row.get("content_id"))
        if content_id is not None:
            assignments_by_content[content_id].append(row)

    gap_items: list[dict[str, Any]] = []
    scanned = 0
    for row in content_rows:
        content_id = _int_or_none(row.get("content_id") or row.get("id"))
        created_at = _parse_dt(row.get("created_at"))
        if content_id is None or (created_at is not None and created_at < window_start):
            continue
        scanned += 1
        assignments = assignments_by_content.get(content_id, [])
        if _should_have_topic(row, confidence_threshold) and not assignments:
            gap_items.append(_gap_item(row, None, "missing_topic_assignment"))

    for row in topic_rows:
        confidence = _float_or_none(row.get("confidence"))
        if confidence is not None and confidence < confidence_threshold:
            gap_items.append(_gap_item(_content_for(row, content_rows), row, "low_confidence_topic_assignment"))

    for (content_id, topic, subtopic), rows in _duplicate_groups(topic_rows).items():
        if len(rows) > 1:
            content = _content_for({"content_id": content_id}, content_rows)
            representative = rows[0]
            gap_items.append(
                {
                    **_gap_item(content, representative, "duplicate_topic_assignment"),
                    "assignment_count": len(rows),
                    "content_topic_ids": [_int_or_none(row.get("content_topic_id") or row.get("id")) for row in rows],
                    "topic": topic,
                    "subtopic": subtopic,
                }
            )

    gap_items.sort(key=_sort_key)
    shown = gap_items[:limit]
    return {
        "artifact_type": "content_topic_assignment_gaps",
        "generated_at": generated_at.isoformat(),
        "thresholds": {
            "window_days": window_days,
            "window_start": window_start.isoformat(),
            "confidence_threshold": confidence_threshold,
            "limit": limit,
        },
        "summary": {
            "content_rows_scanned": scanned,
            "topic_assignment_count": len(topic_rows),
            "gap_count": len(gap_items),
            "shown_count": len(shown),
            "by_issue_type": dict(sorted(Counter(item["issue_type"] for item in gap_items).items())),
        },
        "gap_items": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(columns) for table, columns in sorted((missing_columns or {}).items())},
    }


def build_content_topic_assignment_gaps_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables: list[str] = []
    missing_columns: dict[str, list[str]] = {}
    content_rows: list[dict[str, Any]] = []
    topic_rows: list[dict[str, Any]] = []

    generated_at = _utc(kwargs.get("now") or datetime.now(timezone.utc))
    window_days = kwargs.get("window_days", DEFAULT_WINDOW_DAYS)
    if window_days <= 0:
        raise ValueError("window_days must be positive")
    cutoff = generated_at - timedelta(days=window_days)

    if "generated_content" not in schema:
        missing_tables.append("generated_content")
    else:
        required = {"id", "created_at"}
        missing = required - schema["generated_content"]
        if missing:
            missing_columns["generated_content"] = sorted(missing)
        else:
            content_rows = _load_generated_content(conn, schema, cutoff)

    if "content_topics" not in schema:
        missing_tables.append("content_topics")
    else:
        required = {"content_id", "topic", "subtopic", "confidence"}
        missing = required - schema["content_topics"]
        if missing:
            missing_columns["content_topics"] = sorted(missing)
        else:
            topic_rows = _load_content_topics(conn, schema, cutoff)

    return build_content_topic_assignment_gaps_report(
        content_rows,
        topic_rows,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        **kwargs,
    )


def format_content_topic_assignment_gaps_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_content_topic_assignment_gaps_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    thresholds = report["thresholds"]
    lines = [
        "Content Topic Assignment Gaps",
        f"Generated: {report['generated_at']}",
        f"Window: {thresholds['window_days']} days",
        f"Confidence threshold: {thresholds['confidence_threshold']}",
        f"Limit: {thresholds['limit']}",
        (
            "Totals: "
            f"content={summary['content_rows_scanned']} assignments={summary['topic_assignment_count']} "
            f"gaps={summary['gap_count']} shown={summary['shown_count']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append(
            "Missing columns: "
            + "; ".join(f"{table}({', '.join(columns)})" for table, columns in report["missing_columns"].items())
        )
    if not report["gap_items"]:
        lines.append("No content topic assignment gaps found.")
        return "\n".join(lines)

    lines.extend(["", "content_id | content_type | issue_type | topic | subtopic | confidence"])
    for item in report["gap_items"]:
        confidence = item["confidence"] if item["confidence"] is not None else "-"
        lines.append(
            f"{item['content_id'] or '-'} | {item['content_type']} | {item['issue_type']} | "
            f"{item['topic'] or '-'} | {item['subtopic'] or '-'} | {confidence}"
        )
    return "\n".join(lines)


def _load_generated_content(conn: sqlite3.Connection, schema: dict[str, set[str]], cutoff: datetime) -> list[dict[str, Any]]:
    cols = schema["generated_content"]
    select = [
        "id AS content_id",
        _expr(cols, "content_type", "content_type", "'unknown'"),
        _expr(cols, "published", "published", "0"),
        _expr(cols, "published_at", "published_at", "NULL"),
        _expr(cols, "status", "status", "NULL"),
        _expr(cols, "eval_score", "eval_score", "NULL"),
        "created_at AS created_at",
    ]
    return [
        dict(row)
        for row in conn.execute(
            f"SELECT {', '.join(select)} FROM generated_content WHERE datetime(created_at) >= datetime(?) ORDER BY created_at DESC, id ASC",
            (cutoff.isoformat(),),
        )
    ]


def _load_content_topics(conn: sqlite3.Connection, schema: dict[str, set[str]], cutoff: datetime) -> list[dict[str, Any]]:
    topic_cols = schema["content_topics"]
    gc_cols = schema.get("generated_content", set())
    can_join = "generated_content" in schema and {"id", "created_at"}.issubset(gc_cols)
    select = [
        _expr(topic_cols, "id", "content_topic_id", "NULL", alias="ct"),
        "ct.content_id AS content_id",
        "ct.topic AS topic",
        "ct.subtopic AS subtopic",
        "ct.confidence AS confidence",
    ]
    if can_join:
        select.extend(
            [
                _expr(gc_cols, "content_type", "content_type", "'unknown'", alias="gc"),
                "gc.created_at AS content_created_at",
            ]
        )
        sql = (
            f"SELECT {', '.join(select)} FROM content_topics ct "
            "JOIN generated_content gc ON gc.id = ct.content_id "
            "WHERE datetime(gc.created_at) >= datetime(?) "
            "ORDER BY ct.content_id ASC, ct.topic ASC, ct.subtopic ASC, ct.rowid ASC"
        )
        return [dict(row) for row in conn.execute(sql, (cutoff.isoformat(),))]
    select.extend(["'unknown' AS content_type", "NULL AS content_created_at"])
    return [
        dict(row)
        for row in conn.execute(
            f"SELECT {', '.join(select)} FROM content_topics ct ORDER BY ct.content_id ASC, ct.topic ASC, ct.subtopic ASC, ct.rowid ASC"
        )
    ]


def _should_have_topic(row: dict[str, Any], confidence_threshold: float) -> bool:
    return _is_published(row) or ((_float_or_none(row.get("eval_score")) or 0.0) >= confidence_threshold)


def _is_published(row: dict[str, Any]) -> bool:
    if _clean(row.get("status")).lower() == "published":
        return True
    if _clean(row.get("published_at")):
        return True
    value = row.get("published")
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "published"}
    return bool(value)


def _content_for(row: dict[str, Any], content_rows: list[dict[str, Any]]) -> dict[str, Any]:
    content_id = _int_or_none(row.get("content_id"))
    for content in content_rows:
        if _int_or_none(content.get("content_id") or content.get("id")) == content_id:
            return content
    return {"content_id": content_id, "content_type": row.get("content_type") or "unknown"}


def _gap_item(content: dict[str, Any], assignment: dict[str, Any] | None, issue_type: str) -> dict[str, Any]:
    return {
        "content_id": _int_or_none(content.get("content_id") or content.get("id") or (assignment or {}).get("content_id")),
        "content_type": _clean(content.get("content_type"), "unknown"),
        "issue_type": issue_type,
        "topic": _clean((assignment or {}).get("topic")) or None,
        "subtopic": _clean((assignment or {}).get("subtopic")) or None,
        "confidence": _float_or_none((assignment or {}).get("confidence")),
        "content_topic_id": _int_or_none((assignment or {}).get("content_topic_id") or (assignment or {}).get("id")),
    }


def _duplicate_groups(topic_rows: list[dict[str, Any]]) -> dict[tuple[int, str, str], list[dict[str, Any]]]:
    groups: dict[tuple[int, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in topic_rows:
        content_id = _int_or_none(row.get("content_id"))
        if content_id is None:
            continue
        groups[(content_id, _clean(row.get("topic"), "unknown"), _clean(row.get("subtopic")))].append(row)
    return groups


def _sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (item["content_id"] or 0, item["issue_type"], item.get("topic") or "", item.get("subtopic") or "", item.get("content_topic_id") or 0)


def _expr(columns: set[str], column: str, output: str, default: str, *, alias: str | None = None) -> str:
    prefix = f"{alias}." if alias else ""
    return f"{prefix}{column} AS {output}" if column in columns else f"{default} AS {output}"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {
        str(row[0]): {str(column[1]) for column in conn.execute(f"PRAGMA table_info({row[0]})")}
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip().replace("Z", "+00:00")
    for candidate in (text, text.replace(" ", "T", 1)):
        try:
            return _utc(datetime.fromisoformat(candidate))
        except ValueError:
            continue
    return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _clean(value: Any, default: str = "") -> str:
    text = "" if value is None else str(value).strip()
    return text or default


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
