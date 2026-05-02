"""Report reply drafts that lacked useful knowledge support."""

from __future__ import annotations

import json
import re
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from statistics import mean
from typing import Any, Iterable


DEFAULT_DAYS = 14
DEFAULT_MIN_QUALITY = 6.0
DEFAULT_STATUS = "pending,reviewed,approved,dismissed"
GENERIC_FLAGS = {
    "bland",
    "boilerplate",
    "generic",
    "low_context",
    "low_contextuality",
    "needs_specificity",
    "sycophantic",
    "vague",
}
LOW_QUALITY_FLAGS = GENERIC_FLAGS | {
    "hallucinated",
    "low_quality",
    "needs_regeneration",
    "off_topic",
    "unsafe",
}
STOP_WORDS = {
    "about",
    "after",
    "again",
    "also",
    "because",
    "been",
    "being",
    "could",
    "from",
    "have",
    "into",
    "just",
    "like",
    "more",
    "need",
    "only",
    "really",
    "reply",
    "should",
    "that",
    "their",
    "there",
    "they",
    "this",
    "what",
    "when",
    "where",
    "with",
    "would",
    "your",
}
TOPIC_KEYWORDS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("developer-experience", ("api", "cli", "developer", "docs", "dx", "sdk")),
    ("product-feedback", ("customer", "feedback", "product", "roadmap", "user")),
    ("reliability", ("bug", "incident", "outage", "reliability", "test")),
    ("ai-workflow", ("agent", "ai", "llm", "model", "prompt")),
    ("release-work", ("deploy", "launch", "release", "ship", "shipped")),
)


def build_reply_knowledge_gap_report(
    db: Any,
    *,
    days: int = DEFAULT_DAYS,
    status: str | Iterable[str] = DEFAULT_STATUS,
    min_quality: float = DEFAULT_MIN_QUALITY,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a deterministic report of reply drafts with weak knowledge support."""
    if days <= 0:
        raise ValueError("days must be positive")
    if not 0 <= min_quality <= 10:
        raise ValueError("min_quality must be between 0 and 10")

    conn = _connection(db)
    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    statuses = _normalize_status_filter(status)
    schema = _schema(conn)
    if "reply_queue" not in schema:
        return _empty_report(days, statuses, min_quality, generated_at, cutoff)

    rows = _reply_rows(conn, schema["reply_queue"], cutoff, generated_at, statuses)
    reply_ids = [int(row["id"]) for row in rows if row.get("id")]
    link_counts = _knowledge_link_counts(conn, schema, reply_ids)
    review_feedback = _review_feedback(conn, schema, reply_ids)
    base_items = [
        _build_item(
            row,
            schema["reply_queue"],
            link_counts.get(int(row.get("id") or 0), 0),
            review_feedback.get(int(row.get("id") or 0), []),
            min_quality,
        )
        for row in rows
    ]

    repeated_keys = {
        key
        for key, count in Counter(
            (item["target_handle"], item["topic"]) for item in base_items
        ).items()
        if count >= 2
    }
    items = []
    for item in base_items:
        if (item["target_handle"], item["topic"]) in repeated_keys:
            item["gap_reasons"].append("repeated_target_topic")
        item["gap_reasons"] = sorted(dict.fromkeys(item["gap_reasons"]))
        if item["gap_reasons"]:
            items.append(item)

    items.sort(key=_item_sort_key)
    groups = _groups(items)
    return {
        "generated_at": generated_at.isoformat(),
        "filters": {
            "days": days,
            "lookback_start": cutoff.isoformat(),
            "lookback_end": generated_at.isoformat(),
            "min_quality": float(min_quality),
            "status": list(statuses),
        },
        "totals": _totals(len(rows), items),
        "groups": groups,
        "items": items,
    }


def format_reply_knowledge_gap_json(report: dict[str, Any]) -> str:
    """Serialize the report as stable JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_knowledge_gap_text(report: dict[str, Any]) -> str:
    """Format a stable human-readable reply knowledge gap report."""
    filters = report["filters"]
    totals = report["totals"]
    lines = [
        "Reply Knowledge Gap Report",
        f"Generated: {report['generated_at']}",
        (
            f"Lookback: {filters['days']} days "
            f"({filters['lookback_start']} to {filters['lookback_end']})"
        ),
        f"Status: {', '.join(filters['status'])}",
        f"Min quality: {filters['min_quality']:g}",
        (
            f"Rows: scanned={totals['replies_scanned']} gaps={totals['gap_replies']} "
            f"unsupported={totals['unsupported_replies']} "
            f"low_quality={totals['low_quality_replies']} "
            f"generic={totals['generic_feedback_replies']} "
            f"repeated={totals['repeated_target_topic_replies']}"
        ),
        "",
    ]
    if not report["groups"]:
        lines.append("No reply knowledge gaps matched.")
        return "\n".join(lines)

    lines.append("Gaps:")
    lines.append(
        f"  {'Handle':<18} {'Topic':<20} {'Rows':>4} {'NoK':>4} {'LowQ':>5} "
        f"{'Gen':>4} {'Rpt':>4} {'AvgQ':>6}  Reply ids"
    )
    lines.append("  " + "-" * 84)
    for group in report["groups"]:
        counts = group["reason_counts"]
        lines.append(
            f"  {group['target_handle'][:18]:<18} {group['topic'][:20]:<20} "
            f"{group['reply_count']:>4} {counts.get('unsupported', 0):>4} "
            f"{counts.get('low_quality', 0):>5} {counts.get('generic_feedback', 0):>4} "
            f"{counts.get('repeated_target_topic', 0):>4} "
            f"{_format_score(group['average_quality_score']):>6}  "
            f"{_format_ids(group['reply_ids'])}"
        )

    lines.extend(["", "Suggested ingestion prompts:"])
    for group in report["groups"]:
        for prompt in group["suggested_ingestion_prompts"]:
            lines.append(f"- {group['target_handle']} / {group['topic']}: {prompt}")
    return "\n".join(lines)


def _connection(db: Any) -> sqlite3.Connection:
    return db.conn if hasattr(db, "conn") else db


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    schema: dict[str, set[str]] = {}
    try:
        tables = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    except sqlite3.Error:
        return schema
    for row in tables:
        table = str(row[0])
        try:
            schema[table] = {
                str(info[1]) for info in conn.execute(f"PRAGMA table_info({table})")
            }
        except sqlite3.Error:
            schema[table] = set()
    return schema


def _reply_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    cutoff: datetime,
    now: datetime,
    statuses: tuple[str, ...],
) -> list[dict[str, Any]]:
    if "id" not in columns:
        return []
    filters = []
    params: list[Any] = []
    if statuses and "all" not in statuses and "status" in columns:
        placeholders = ", ".join("?" for _ in statuses)
        filters.append(f"LOWER(COALESCE(status, 'pending')) IN ({placeholders})")
        params.extend(statuses)
    time_column = _first_present(columns, ("detected_at", "reviewed_at", "created_at"))
    if time_column:
        filters.append(f"({time_column} IS NULL OR datetime({time_column}) >= datetime(?))")
        filters.append(f"({time_column} IS NULL OR datetime({time_column}) <= datetime(?))")
        params.extend([cutoff.isoformat(), now.isoformat()])
    query = "SELECT * FROM reply_queue"
    if filters:
        query += " WHERE " + " AND ".join(filters)
    query += " ORDER BY " + _order_clause(columns, time_column)
    cursor = conn.execute(query, params)
    names = [description[0] for description in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _knowledge_link_counts(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    reply_ids: list[int],
) -> dict[int, int]:
    columns = schema.get("reply_knowledge_links")
    if not reply_ids or not columns or "reply_queue_id" not in columns:
        return {}
    placeholders = ", ".join("?" for _ in reply_ids)
    cursor = conn.execute(
        f"""SELECT reply_queue_id, COUNT(*) AS link_count
            FROM reply_knowledge_links
            WHERE reply_queue_id IN ({placeholders})
            GROUP BY reply_queue_id""",
        reply_ids,
    )
    return {int(row[0]): int(row[1]) for row in cursor.fetchall()}


def _review_feedback(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    reply_ids: list[int],
) -> dict[int, list[str]]:
    columns = schema.get("reply_review_events")
    if not reply_ids or not columns or "reply_queue_id" not in columns:
        return {}
    feedback_columns = [column for column in ("notes", "feedback", "reason") if column in columns]
    if not feedback_columns:
        return {}
    placeholders = ", ".join("?" for _ in reply_ids)
    select_feedback = ", ".join(feedback_columns)
    cursor = conn.execute(
        f"""SELECT reply_queue_id, {select_feedback}
            FROM reply_review_events
            WHERE reply_queue_id IN ({placeholders})
            ORDER BY reply_queue_id ASC""",
        reply_ids,
    )
    grouped: dict[int, list[str]] = defaultdict(list)
    names = [description[0] for description in cursor.description]
    for row in cursor.fetchall():
        item = dict(zip(names, row))
        reply_id = int(item["reply_queue_id"])
        for column in feedback_columns:
            value = str(item.get(column) or "").strip()
            if value:
                grouped[reply_id].append(value)
    return grouped


def _build_item(
    row: dict[str, Any],
    columns: set[str],
    link_count: int,
    feedback: list[str],
    min_quality: float,
) -> dict[str, Any]:
    quality_score = _float_or_none(_value(row, columns, "quality_score"))
    quality_flags = _parse_flags(_value(row, columns, "quality_flags"))
    feedback_text = " ".join(feedback + [_feedback_column(row, columns)]).strip()
    target_handle = _target_handle(row, columns)
    topic = _infer_topic(row, columns)
    reasons: list[str] = []
    if link_count == 0:
        reasons.append("unsupported")
    if (quality_score is not None and quality_score < min_quality) or bool(
        set(quality_flags) & LOW_QUALITY_FLAGS
    ):
        reasons.append("low_quality")
    if bool(set(quality_flags) & GENERIC_FLAGS) or _looks_generic_feedback(feedback_text):
        reasons.append("generic_feedback")

    return {
        "id": int(row.get("id") or 0),
        "target_handle": target_handle,
        "topic": topic,
        "status": str(_value(row, columns, "status") or "pending"),
        "detected_at": _datetime_iso(_value(row, columns, "detected_at")),
        "reviewed_at": _datetime_iso(_value(row, columns, "reviewed_at")),
        "quality_score": quality_score,
        "quality_flags": quality_flags,
        "knowledge_link_count": link_count,
        "gap_reasons": reasons,
        "feedback": feedback_text or None,
        "themes": _themes(row, columns),
    }


def _groups(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        grouped[(item["target_handle"], item["topic"])].append(item)

    summaries = []
    for (handle, topic), group_items in sorted(grouped.items()):
        reason_counts = Counter(reason for item in group_items for reason in item["gap_reasons"])
        scores = [item["quality_score"] for item in group_items if item["quality_score"] is not None]
        themes = _top_themes(group_items)
        summaries.append(
            {
                "target_handle": handle,
                "topic": topic,
                "reply_count": len(group_items),
                "reply_ids": [
                    item["id"] for item in sorted(group_items, key=lambda item: item["id"])
                ],
                "statuses": sorted({item["status"] for item in group_items}),
                "reason_counts": dict(sorted(reason_counts.items())),
                "average_quality_score": round(mean(scores), 2) if scores else None,
                "themes": themes,
                "suggested_ingestion_prompts": [
                    _ingestion_prompt(handle, topic, themes, reason_counts)
                ],
            }
        )
    summaries.sort(key=lambda group: (-group["reply_count"], group["target_handle"], group["topic"]))
    return summaries


def _totals(scanned: int, items: list[dict[str, Any]]) -> dict[str, int]:
    reason_counts = Counter(reason for item in items for reason in item["gap_reasons"])
    return {
        "replies_scanned": scanned,
        "gap_replies": len(items),
        "unsupported_replies": reason_counts.get("unsupported", 0),
        "low_quality_replies": reason_counts.get("low_quality", 0),
        "generic_feedback_replies": reason_counts.get("generic_feedback", 0),
        "repeated_target_topic_replies": reason_counts.get("repeated_target_topic", 0),
    }


def _empty_report(
    days: int,
    statuses: tuple[str, ...],
    min_quality: float,
    generated_at: datetime,
    cutoff: datetime,
) -> dict[str, Any]:
    return {
        "generated_at": generated_at.isoformat(),
        "filters": {
            "days": days,
            "lookback_start": cutoff.isoformat(),
            "lookback_end": generated_at.isoformat(),
            "min_quality": float(min_quality),
            "status": list(statuses),
        },
        "totals": _totals(0, []),
        "groups": [],
        "items": [],
    }


def _normalize_status_filter(status: str | Iterable[str]) -> tuple[str, ...]:
    if isinstance(status, str):
        raw = status.split(",")
    else:
        raw = list(status)
    statuses = tuple(
        sorted({str(item).strip().lower() for item in raw if str(item).strip()})
    )
    if not statuses:
        raise ValueError("status must not be empty")
    return statuses


def _first_present(columns: set[str], names: Iterable[str]) -> str | None:
    for name in names:
        if name in columns:
            return name
    return None


def _order_clause(columns: set[str], time_column: str | None) -> str:
    parts = []
    if time_column:
        parts.append(f"datetime({time_column}) ASC")
    parts.append("id ASC" if "id" in columns else "rowid ASC")
    return ", ".join(parts)


def _target_handle(row: dict[str, Any], columns: set[str]) -> str:
    value = _value(row, columns, "inbound_author_handle") or _value(
        row, columns, "author_handle"
    )
    handle = str(value or "unknown").strip()
    if not handle:
        return "@unknown"
    return handle if handle.startswith("@") else f"@{handle}"


def _infer_topic(row: dict[str, Any], columns: set[str]) -> str:
    intent = str(_value(row, columns, "intent") or "").strip().lower().replace("_", "-")
    if intent and intent not in {"other", "unknown", "none"}:
        return intent
    text = _combined_text(row, columns).lower()
    tokens = set(_tokens(text))
    for topic, keywords in TOPIC_KEYWORDS:
        if tokens.intersection(keywords):
            return topic
    themes = _themes(row, columns)
    return "-".join(themes[:2]) if themes else "general"


def _themes(row: dict[str, Any], columns: set[str]) -> list[str]:
    counts = Counter(
        token
        for token in _tokens(_combined_text(row, columns))
        if len(token) >= 4 and token not in STOP_WORDS
    )
    return [token for token, _count in counts.most_common(5)]


def _top_themes(items: list[dict[str, Any]]) -> list[str]:
    counts = Counter(theme for item in items for theme in item["themes"])
    return [theme for theme, _count in counts.most_common(5)]


def _combined_text(row: dict[str, Any], columns: set[str]) -> str:
    return " ".join(
        str(_value(row, columns, column) or "")
        for column in ("inbound_text", "our_post_text", "draft_text", "relationship_context")
    )


def _tokens(text: str) -> list[str]:
    return re.findall(r"[a-z0-9][a-z0-9_-]*", text.lower())


def _ingestion_prompt(
    handle: str,
    topic: str,
    themes: list[str],
    reason_counts: Counter[str],
) -> str:
    theme_text = ", ".join(themes[:4]) if themes else topic
    reasons = ", ".join(reason.replace("_", " ") for reason in sorted(reason_counts))
    return (
        f"Collect first-party knowledge, prior posts, and relationship notes for {handle} "
        f"on {topic}; focus on {theme_text}. Address gaps: {reasons}."
    )


def _feedback_column(row: dict[str, Any], columns: set[str]) -> str:
    for column in ("feedback", "eval_feedback", "review_feedback", "quality_feedback"):
        if column in columns and row.get(column):
            return str(row[column])
    return ""


def _looks_generic_feedback(value: str) -> bool:
    normalized = value.lower()
    if not normalized:
        return False
    return any(word in normalized for word in ("generic", "vague", "boilerplate", "bland", "too broad"))


def _parse_flags(flags_json: Any) -> list[str]:
    if not flags_json:
        return []
    try:
        parsed = json.loads(flags_json) if isinstance(flags_json, str) else flags_json
    except (TypeError, json.JSONDecodeError):
        return [str(flags_json).strip().lower()]
    if not isinstance(parsed, list):
        return []
    return sorted(str(item).strip().lower() for item in parsed if str(item).strip())


def _float_or_none(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _value(row: dict[str, Any], columns: set[str], column: str) -> Any:
    return row.get(column) if column in columns else None


def _datetime_iso(value: Any) -> str | None:
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed else None


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    normalized = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        try:
            parsed = datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return _as_utc(parsed)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _item_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (
        item["target_handle"],
        item["topic"],
        "unsupported" not in item["gap_reasons"],
        item["quality_score"] is None,
        item["quality_score"] if item["quality_score"] is not None else 99,
        item["id"],
    )


def _format_score(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.2f}"


def _format_ids(ids: list[int]) -> str:
    return ", ".join(str(item_id) for item_id in ids) if ids else "none"
