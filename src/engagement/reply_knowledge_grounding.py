"""Audit reply drafts for knowledge grounding coverage."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_MIN_RELEVANCE = 0.65
DEFAULT_LOOKBACK_DAYS = 14
DEFAULT_LIMIT = 25
GROUNDING_STATUSES = (
    "grounded",
    "weakly_grounded",
    "ungrounded",
    "posted_without_grounding",
)
POSTED_STATUSES = {"posted", "published", "sent"}
REQUIRED_REPLY_COLUMNS = {
    "id",
    "inbound_author_handle",
    "status",
    "draft_text",
    "detected_at",
}


def build_reply_knowledge_grounding_report(
    db_or_conn: Any,
    *,
    min_relevance: float = DEFAULT_MIN_RELEVANCE,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return grounding classifications for recent reply drafts."""

    if not 0 <= min_relevance <= 1:
        raise ValueError("min_relevance must be between 0 and 1")
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=lookback_days)
    filters = {
        "min_relevance": min_relevance,
        "lookback_days": lookback_days,
        "lookback_start": cutoff.isoformat(),
        "lookback_end": generated_at.isoformat(),
        "limit": limit,
    }

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "reply_queue" not in schema:
        return _empty_report(generated_at, filters, missing_tables=["reply_queue"])
    missing = sorted(REQUIRED_REPLY_COLUMNS - schema["reply_queue"])
    if missing:
        return _empty_report(
            generated_at,
            filters,
            missing_columns={"reply_queue": missing},
        )

    rows = _reply_rows(conn, schema, cutoff, generated_at)
    link_stats = _link_stats(conn, schema, [int(row["id"]) for row in rows])
    items = [
        _build_item(row, link_stats.get(int(row["id"]), []), min_relevance)
        for row in rows
    ]
    items.sort(key=_item_sort_key)

    return {
        "artifact_type": "reply_knowledge_grounding",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": _totals(items),
        "items": items[:limit],
        "missing_tables": [],
        "missing_columns": {},
        "missing_optional_tables": [
            table
            for table in ("reply_knowledge_links", "knowledge")
            if table not in schema
        ],
    }


def format_reply_knowledge_grounding_json(report: dict[str, Any]) -> str:
    """Serialize the report as stable JSON."""

    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_knowledge_grounding_text(report: dict[str, Any]) -> str:
    """Render a compact human-readable grounding report."""

    filters = report["filters"]
    totals = report["totals"]
    lines = [
        "Reply Knowledge Grounding",
        f"Generated: {report['generated_at']}",
        (
            f"Filters: lookback_days={filters['lookback_days']} "
            f"min_relevance={filters['min_relevance']:g} limit={filters['limit']}"
        ),
        (
            "Grounding counts: "
            + " ".join(
                f"{status}={totals['by_grounding_status'][status]}"
                for status in GROUNDING_STATUSES
            )
            + f" total={totals['total']}"
        ),
        "Reply status counts: "
        + " ".join(
            f"{status}={count}"
            for status, count in totals["by_reply_status"].items()
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
    if report.get("missing_optional_tables"):
        lines.append(
            "Missing optional tables: " + ", ".join(report["missing_optional_tables"])
        )
    if not report["items"]:
        lines.append("No reply drafts matched the lookback window.")
        return "\n".join(lines)

    lines.append("")
    lines.append("Items:")
    for item in report["items"]:
        handle = item["author_handle"] or "-"
        max_rel = "-" if item["max_relevance"] is None else f"{item['max_relevance']:.3f}"
        ids = ",".join(str(value) for value in item["representative_knowledge_ids"]) or "-"
        lines.append(
            f"- #{item['reply_queue_id']} @{handle} status={item['status']} "
            f"grounding={item['grounding_status']} links={item['knowledge_link_count']} "
            f"max_relevance={max_rel} knowledge={ids}"
        )
    return "\n".join(lines)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    try:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    except sqlite3.Error:
        return {}
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    *,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    return {
        "artifact_type": "reply_knowledge_grounding",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "total": 0,
            "by_grounding_status": {status: 0 for status in GROUNDING_STATUSES},
            "by_reply_status": {},
        },
        "items": [],
        "missing_tables": missing_tables or [],
        "missing_columns": missing_columns or {},
        "missing_optional_tables": [],
    }


def _reply_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    cutoff: datetime,
    generated_at: datetime,
) -> list[dict[str, Any]]:
    columns = schema["reply_queue"]
    created_expr = _column_expr(columns, "detected_at", "created_at")
    reviewed_expr = _column_expr(columns, "reviewed_at")
    posted_expr = _column_expr(columns, "posted_at")
    cursor = conn.execute(
        f"""SELECT id,
                  inbound_author_handle,
                  status,
                  draft_text,
                  {created_expr} AS detected_at,
                  {reviewed_expr} AS reviewed_at,
                  {posted_expr} AS posted_at
           FROM reply_queue
           WHERE datetime({created_expr}) >= datetime(?)
             AND datetime({created_expr}) <= datetime(?)
           ORDER BY datetime({created_expr}) DESC, id DESC""",
        (cutoff.isoformat(), generated_at.isoformat()),
    )
    return [dict(row) for row in cursor.fetchall()]


def _link_stats(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    reply_ids: list[int],
) -> dict[int, list[dict[str, Any]]]:
    if not reply_ids or "reply_knowledge_links" not in schema:
        return {}
    columns = schema["reply_knowledge_links"]
    if not {"reply_queue_id", "knowledge_id"}.issubset(columns):
        return {}
    relevance = "relevance_score" if "relevance_score" in columns else "NULL"
    placeholders = ", ".join("?" for _ in reply_ids)
    join = ""
    select_knowledge = "NULL AS knowledge_source_type"
    if "knowledge" in schema and "id" in schema["knowledge"]:
        join = "LEFT JOIN knowledge k ON k.id = rkl.knowledge_id"
        select_knowledge = _column_expr(schema["knowledge"], "source_type", alias="k", as_name="knowledge_source_type")
    rows = conn.execute(
        f"""SELECT rkl.reply_queue_id,
                  rkl.knowledge_id,
                  {relevance} AS relevance_score,
                  {select_knowledge}
           FROM reply_knowledge_links rkl
           {join}
           WHERE rkl.reply_queue_id IN ({placeholders})
           ORDER BY rkl.reply_queue_id ASC, rkl.relevance_score DESC, rkl.knowledge_id ASC""",
        reply_ids,
    ).fetchall()
    grouped: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(int(row["reply_queue_id"]), []).append(dict(row))
    return grouped


def _build_item(
    row: dict[str, Any],
    links: list[dict[str, Any]],
    min_relevance: float,
) -> dict[str, Any]:
    status = _clean(row.get("status")) or "pending"
    scores = [_float_or_none(link.get("relevance_score")) for link in links]
    valid_scores = [score for score in scores if score is not None]
    max_relevance = max(valid_scores) if valid_scores else None
    link_count = len(links)
    if status in POSTED_STATUSES and (link_count == 0 or (max_relevance or 0) < min_relevance):
        grounding = "posted_without_grounding"
    elif link_count == 0:
        grounding = "ungrounded"
    elif (max_relevance or 0) >= min_relevance:
        grounding = "grounded"
    else:
        grounding = "weakly_grounded"
    representative_ids = [
        int(link["knowledge_id"])
        for link in links[:3]
        if link.get("knowledge_id") is not None
    ]
    return {
        "reply_queue_id": int(row["id"]),
        "author_handle": row.get("inbound_author_handle"),
        "status": status,
        "grounding_status": grounding,
        "knowledge_link_count": link_count,
        "max_relevance": max_relevance,
        "representative_knowledge_ids": representative_ids,
        "detected_at": row.get("detected_at"),
        "reviewed_at": row.get("reviewed_at"),
        "posted_at": row.get("posted_at"),
    }


def _totals(items: list[dict[str, Any]]) -> dict[str, Any]:
    by_grounding = Counter(item["grounding_status"] for item in items)
    by_reply = Counter(item["status"] for item in items)
    return {
        "total": len(items),
        "by_grounding_status": {
            status: by_grounding.get(status, 0) for status in GROUNDING_STATUSES
        },
        "by_reply_status": dict(sorted(by_reply.items())),
    }


def _item_sort_key(item: dict[str, Any]) -> tuple[int, str, int]:
    rank = {
        "posted_without_grounding": 0,
        "ungrounded": 1,
        "weakly_grounded": 2,
        "grounded": 3,
    }
    return (rank[item["grounding_status"]], item.get("detected_at") or "", item["reply_queue_id"])


def _column_expr(
    columns: set[str],
    column: str,
    fallback: str = "NULL",
    *,
    alias: str | None = None,
    as_name: str | None = None,
) -> str:
    prefix = f"{alias}." if alias else ""
    expr = f"{prefix}{column}" if column in columns else fallback
    return f"{expr} AS {as_name}" if as_name else expr


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    return text or None


def _float_or_none(value: Any) -> float | None:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
