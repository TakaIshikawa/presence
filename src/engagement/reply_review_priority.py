"""Rank drafted replies awaiting human review."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_DAYS = 14
DEFAULT_LIMIT = 20
LOW_QUALITY_FLAGS = {"sycophantic", "generic", "low_quality", "unsafe", "spam"}


def build_reply_review_priority_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    include_low_quality: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Rank pending reply_queue rows with deterministic explainable signals."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    rows = _load_rows(conn, schema, cutoff, generated_at)
    duplicate_counts = Counter(_intent_key(row) for row in rows)
    scored = [_score(row, generated_at, duplicate_counts) for row in rows]
    if not include_low_quality:
        scored = [row for row in scored if not row["low_quality"]]
    scored.sort(key=lambda row: (-row["priority_score"], -row["age_hours"], row["reply_queue_id"]))
    for index, row in enumerate(scored[:limit], start=1):
        row["rank"] = index
    return {
        "artifact_type": "reply_review_priority",
        "generated_at": generated_at.isoformat(),
        "filters": {"days": days, "limit": limit, "include_low_quality": include_low_quality},
        "totals": {
            "rows_scanned": len(rows),
            "returned": min(len(scored), limit),
            "low_quality_excluded": 0 if include_low_quality else sum(1 for row in [_score(r, generated_at, duplicate_counts) for r in rows] if row["low_quality"]),
        },
        "items": scored[:limit],
        "missing_tables": [] if "reply_queue" in schema else ["reply_queue"],
    }


def format_reply_review_priority_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_review_priority_text(report: dict[str, Any]) -> str:
    lines = [
        "Reply Review Priority",
        f"Generated: {report['generated_at']}",
        (
            f"Filters: days={report['filters']['days']} limit={report['filters']['limit']} "
            f"include_low_quality={report['filters']['include_low_quality']}"
        ),
        f"Totals: scanned={report['totals']['rows_scanned']} returned={report['totals']['returned']}",
    ]
    if not report["items"]:
        lines.extend(["", "No pending reply drafts found."])
        return "\n".join(lines)
    lines.extend(["", "Replies:"])
    for item in report["items"]:
        lines.append(
            f"- rank={item['rank']} reply_id={item['reply_queue_id']} "
            f"score={item['priority_score']} signals={';'.join(item['signals'])}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]], cutoff: datetime, now: datetime) -> list[dict[str, Any]]:
    if "reply_queue" not in schema:
        return []
    cols = schema["reply_queue"]
    expr = lambda col: f"rq.{col}" if col in cols else f"NULL AS {col}"
    rows = conn.execute(
        f"""SELECT rq.id, {expr('inbound_text')}, {expr('draft_text')}, {expr('intent')},
                  {expr('relationship_context')}, {expr('quality_score')},
                  {expr('quality_flags')}, {expr('status')}, {expr('detected_at')},
                  {expr('inbound_author_handle')}
           FROM reply_queue rq
           WHERE COALESCE(rq.status, 'pending') = 'pending'
           ORDER BY detected_at ASC, rq.id ASC"""
    ).fetchall()
    out = []
    for row in rows:
        detected = _parse_dt(row["detected_at"]) or now
        if detected < cutoff or detected > now:
            continue
        if not str(row["draft_text"] or "").strip():
            continue
        item = dict(row)
        item["detected_dt"] = detected
        out.append(item)
    return out


def _score(row: dict[str, Any], now: datetime, duplicate_counts: Counter[str]) -> dict[str, Any]:
    score = 50
    signals: list[str] = []
    text = str(row.get("inbound_text") or "")
    age_hours = int((now - row["detected_dt"]).total_seconds() // 3600)
    age_delta = min(30, age_hours // 4)
    score += age_delta
    signals.append(f"age_hours:{age_hours}:+{age_delta}")
    if "?" in text or str(row.get("intent") or "").lower() == "question":
        score += 18
        signals.append("direct_question:+18")
    relationship = _json_obj(row.get("relationship_context"))
    if relationship:
        score += 10
        signals.append("relationship_context:+10")
    else:
        score += 3
        signals.append("relationship_context_missing:+3")
    quality = _float(row.get("quality_score"))
    flags = sorted(str(flag).lower() for flag in _json_list(row.get("quality_flags")))
    low_quality = bool(LOW_QUALITY_FLAGS.intersection(flags)) or (quality is not None and quality < 5)
    if quality is not None:
        delta = int((quality - 6) * 3)
        score += delta
        signals.append(f"quality_score:{quality:g}:{delta:+d}")
    if flags:
        penalty = 12 * len(LOW_QUALITY_FLAGS.intersection(flags))
        score -= penalty
        signals.append(f"quality_flags:{','.join(flags)}:-{penalty}")
    duplicate_count = duplicate_counts[_intent_key(row)]
    if duplicate_count > 1:
        score -= 10
        signals.append(f"duplicate_intent:{duplicate_count}:-10")
    bounded = max(0, min(100, score))
    return {
        "reply_queue_id": int(row["id"]),
        "priority_score": bounded,
        "signals": signals,
        "age_hours": age_hours,
        "direct_question": "?" in text or str(row.get("intent") or "").lower() == "question",
        "has_relationship_context": bool(relationship),
        "quality_score": quality,
        "quality_flags": flags,
        "low_quality": low_quality,
        "duplicate_intent_count": duplicate_count,
        "inbound_author_handle": row.get("inbound_author_handle"),
    }


def _intent_key(row: dict[str, Any]) -> str:
    tokens = re.findall(r"[a-z0-9]+", str(row.get("inbound_text") or "").lower())
    return " ".join(tokens[:10])


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    return {table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")} for table in tables}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return []
    return parsed if isinstance(parsed, list) else []


def _float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
