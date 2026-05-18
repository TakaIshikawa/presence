"""Summarize drafted replies waiting for review."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 100
PENDING_STATUSES = {"pending", "drafted", "review", "needs_review", "awaiting_review"}


def build_reply_review_queue_health_report(
    reply_rows: list[dict[str, Any]],
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return aggregate queue health and per-draft risk rows."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    skipped = Counter({"outside_window": 0, "not_pending": 0})
    risk_rows = []
    for row in reply_rows:
        status = _clean(row.get("status") or row.get("review_status") or "pending").lower()
        if status not in PENDING_STATUSES:
            skipped["not_pending"] += 1
            continue
        created_at = _parse_dt(row.get("drafted_at") or row.get("created_at") or row.get("detected_at") or row.get("queued_at")) or generated_at
        if created_at < cutoff or created_at > generated_at:
            skipped["outside_window"] += 1
            continue
        risk_rows.append(_risk_row(row, created_at=created_at, now=generated_at, status=status))

    risk_rows.sort(key=lambda item: (_risk_rank(item["risk_level"]), -item["age_hours"], item["draft_id"]))
    age_buckets = Counter(item["age_bucket"] for item in risk_rows)
    score_bands = Counter(item["score_band"] for item in risk_rows)
    context_coverage = Counter("missing" if item["missing_relationship_context"] else "present_or_unavailable" for item in risk_rows)
    return {
        "artifact_type": "reply_review_queue_health",
        "generated_at": generated_at.isoformat(),
        "filters": {"days": days, "limit": limit, "window_start": cutoff.isoformat(), "window_end": generated_at.isoformat()},
        "totals": {
            "pending_draft_count": len(risk_rows),
            "risk_row_count": len(risk_rows),
            "missing_relationship_context_count": context_coverage["missing"],
            "age_buckets": _ordered(age_buckets, ("0-4h", "4-24h", "1-3d", "4-7d", "8d+")),
            "score_bands": _ordered(score_bands, ("missing", "low", "medium", "high")),
            "relationship_context": dict(sorted(context_coverage.items())),
            **dict(skipped),
        },
        "risk_rows": risk_rows[:limit],
        "empty_state": {
            "is_empty": not risk_rows,
            "message": "No drafted replies waiting for review found." if not risk_rows else None,
        },
    }


def build_reply_review_queue_health_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    rows = _load_rows(conn, schema)
    report = build_reply_review_queue_health_report(rows, **kwargs)
    report["missing_tables"] = [] if "reply_queue" in schema else ["reply_queue"]
    report["missing_columns"] = _missing_columns(schema)
    return report


def format_reply_review_queue_health_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_review_queue_health_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    lines = [
        "Reply Review Queue Health",
        f"Generated: {report['generated_at']}",
        f"Window: {report['filters']['days']} days limit={report['filters']['limit']}",
        f"Totals: pending={totals['pending_draft_count']} missing_context={totals['missing_relationship_context_count']}",
        "Age buckets: " + ", ".join(f"{key}={value}" for key, value in totals["age_buckets"].items()),
        "Score bands: " + ", ".join(f"{key}={value}" for key, value in totals["score_bands"].items()),
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report.get("missing_columns"):
        lines.append("Missing columns: " + "; ".join(f"{table}({', '.join(cols)})" for table, cols in sorted(report["missing_columns"].items())))
    if not report["risk_rows"]:
        lines.extend(["", report["empty_state"]["message"]])
        return "\n".join(lines)
    lines.extend(["", "Risk rows:"])
    for item in report["risk_rows"]:
        lines.append(
            f"- {item['draft_id']} risk={item['risk_level']} age={item['age_bucket']} "
            f"score={item['score_band']} missing_context={item['missing_relationship_context']} reasons={', '.join(item['risk_reasons']) or 'none'}"
        )
    return "\n".join(lines)


format_reply_review_queue_health_table = format_reply_review_queue_health_text


def _risk_row(row: dict[str, Any], *, created_at: datetime, now: datetime, status: str) -> dict[str, Any]:
    age_hours = max(0, int((now - created_at).total_seconds() // 3600))
    score = _score(row)
    missing_context = _missing_relationship_context(row)
    reasons = []
    if age_hours >= 72:
        reasons.append("stale_review")
    if score is None:
        reasons.append("missing_score")
    elif score < 0.5:
        reasons.append("low_evaluator_score")
    if missing_context:
        reasons.append("missing_relationship_context")
    risk_level = "low"
    if age_hours >= 168 or (missing_context and (score is None or score < 0.5)):
        risk_level = "high"
    elif age_hours >= 72 or missing_context or (score is not None and score < 0.7):
        risk_level = "medium"
    return {
        "draft_id": _text(row.get("draft_id") or row.get("reply_id") or row.get("id")),
        "target_id": _text(row.get("target_id") or row.get("conversation_id") or row.get("mention_id")),
        "author_id": _text(row.get("author_id") or row.get("user_id") or row.get("handle")),
        "platform": _clean(row.get("platform") or row.get("channel") or "unknown").lower(),
        "status": status,
        "drafted_at": created_at.isoformat(),
        "age_hours": age_hours,
        "age_bucket": _age_bucket(age_hours),
        "evaluator_score": score,
        "score_band": _score_band(score),
        "relationship_context_available": _context_fields_available(row),
        "missing_relationship_context": missing_context,
        "risk_level": risk_level,
        "risk_reasons": reasons,
    }


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    columns = schema.get("reply_queue")
    if not columns:
        return []
    selected = [
        _select(columns, ("id",), "id"),
        _select(columns, ("reply_id", "draft_id"), "reply_id"),
        _select(columns, ("target_id", "conversation_id", "mention_id"), "target_id"),
        _select(columns, ("author_id", "user_id", "handle"), "author_id"),
        _select(columns, ("platform", "channel"), "platform"),
        _select(columns, ("status", "review_status"), "status"),
        _select(columns, ("drafted_at", "created_at", "detected_at", "queued_at"), "drafted_at"),
        _select(columns, ("evaluator_score", "quality_score", "score"), "evaluator_score"),
        _select(columns, ("relationship_context", "relationship_context_summary", "relationship_notes"), "relationship_context"),
        _select(columns, ("relationship_context_id", "context_id"), "relationship_context_id"),
        _select(columns, ("metadata",), "metadata"),
        _select(columns, ("draft_text", "reply_text"), "draft_text"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM reply_queue").fetchall()]


def _score(row: dict[str, Any]) -> float | None:
    metadata = _json_obj(row.get("metadata"))
    value = row.get("evaluator_score") or row.get("quality_score") or row.get("score") or metadata.get("evaluator_score") or metadata.get("quality_score")
    try:
        return round(float(value), 4)
    except (TypeError, ValueError):
        return None


def _missing_relationship_context(row: dict[str, Any]) -> bool:
    if not _context_fields_available(row):
        return False
    metadata = _json_obj(row.get("metadata"))
    values = [
        row.get("relationship_context"),
        row.get("relationship_context_summary"),
        row.get("relationship_notes"),
        row.get("relationship_context_id"),
        row.get("context_id"),
        metadata.get("relationship_context"),
        metadata.get("relationship_context_summary"),
        metadata.get("relationship_context_id"),
    ]
    return not any(_clean(value) for value in values)


def _context_fields_available(row: dict[str, Any]) -> bool:
    metadata = _json_obj(row.get("metadata"))
    return any(key in row for key in ("relationship_context", "relationship_context_summary", "relationship_notes", "relationship_context_id", "context_id")) or any(
        key in metadata for key in ("relationship_context", "relationship_context_summary", "relationship_context_id")
    )


def _missing_columns(schema: dict[str, set[str]]) -> dict[str, list[str]]:
    if "reply_queue" not in schema:
        return {}
    optional = {"status", "drafted_at", "created_at", "detected_at", "evaluator_score", "quality_score", "relationship_context", "metadata"}
    missing = sorted(optional - schema["reply_queue"])
    return {"reply_queue": missing} if missing else {}


def _select(columns: set[str], names: tuple[str, ...], alias: str) -> str:
    for name in names:
        if name in columns:
            return f"{name} AS {alias}"
    return f"NULL AS {alias}"


def _age_bucket(age_hours: int) -> str:
    if age_hours < 4:
        return "0-4h"
    if age_hours < 24:
        return "4-24h"
    if age_hours < 96:
        return "1-3d"
    if age_hours < 192:
        return "4-7d"
    return "8d+"


def _score_band(score: float | None) -> str:
    if score is None:
        return "missing"
    if score < 0.5:
        return "low"
    if score < 0.8:
        return "medium"
    return "high"


def _ordered(counter: Counter[str], order: tuple[str, ...]) -> dict[str, int]:
    return {key: counter[key] for key in order}


def _risk_rank(level: str) -> int:
    return {"high": 0, "medium": 1, "low": 2}.get(level, 9)


def _parse_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _utc(value)
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value in (None, ""):
        return {}
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _text(value: Any) -> str:
    return "" if value is None else str(value)
