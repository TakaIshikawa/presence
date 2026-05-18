"""Rank reviewed generated content at risk of expiring before publication."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
DEFAULT_APPROVAL_MAX_AGE_DAYS = 14
DEFAULT_EVIDENCE_MAX_AGE_DAYS = 30
REVIEWABLE_STATUSES = {"approved", "ready", "reviewable", "needs_review", "pending_review"}
PUBLISHED_ATTEMPT_STATUSES = {"published", "success", "succeeded"}


def build_publication_review_expiry_risk_report(
    content_rows: list[dict[str, Any]],
    review_rows: list[dict[str, Any]] | None = None,
    evidence_rows: list[dict[str, Any]] | None = None,
    *,
    approval_max_age_days: int = DEFAULT_APPROVAL_MAX_AGE_DAYS,
    evidence_max_age_days: int = DEFAULT_EVIDENCE_MAX_AGE_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    schema_gaps: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if approval_max_age_days <= 0:
        raise ValueError("approval_max_age_days must be positive")
    if evidence_max_age_days <= 0:
        raise ValueError("evidence_max_age_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    reviews_by_content = _latest_reviews(review_rows or [])
    evidence_by_content = _evidence_by_content(evidence_rows or [])
    risks = []
    scanned = 0

    for raw in content_rows:
        content = _normalize_content(raw)
        review = reviews_by_content.get(content["id"], {})
        status = _review_status(content, review)
        if status not in REVIEWABLE_STATUSES:
            continue
        if content["published_at"] or content["publication_url"]:
            continue

        scanned += 1
        decision_at = content["approved_at"] or _parse_ts(review.get("decided_at") or review.get("reviewed_at") or review.get("updated_at"))
        age_basis = decision_at or content["created_at"]
        age_days = _age_days(generated_at, age_basis)
        evidence_age_days = _max_evidence_age(generated_at, evidence_by_content.get(content["id"], []), content["metadata"])
        scheduled_at = content["scheduled_at"]
        attempts = content["publish_attempt_count"] + _int(review.get("publish_attempt_count"))
        reasons: list[str] = []

        if age_days is not None and age_days > approval_max_age_days:
            reasons.append("old_approval" if status == "approved" else "old_review_state")
        if evidence_age_days is not None and evidence_age_days > evidence_max_age_days:
            reasons.append("old_evidence")
        if scheduled_at and scheduled_at < generated_at:
            reasons.append("missed_scheduled_window")
        if attempts <= 0:
            reasons.append("no_publish_attempt")

        score = _risk_score(
            age_days=age_days,
            evidence_age_days=evidence_age_days,
            scheduled_at=scheduled_at,
            attempts=attempts,
            generated_at=generated_at,
            approval_max_age_days=approval_max_age_days,
            evidence_max_age_days=evidence_max_age_days,
        )
        if score <= 0 and not reasons:
            continue
        risks.append(
            {
                "content_id": content["id"],
                "content_type": content["content_type"],
                "review_status": status,
                "age_days": age_days,
                "evidence_age_days": evidence_age_days,
                "scheduled_at": _iso(scheduled_at),
                "risk_score": score,
                "reasons": reasons,
            }
        )

    risks.sort(key=lambda item: (-item["risk_score"], item["content_id"]))
    shown = risks[:limit]
    return {
        "artifact_type": "publication_review_expiry_risk",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "approval_max_age_days": approval_max_age_days,
            "evidence_max_age_days": evidence_max_age_days,
            "limit": limit,
        },
        "totals": {
            "content_count": len(content_rows),
            "reviewable_count": scanned,
            "risk_count": len(risks),
            "shown_count": len(shown),
        },
        "risks": shown,
        "schema_gaps": schema_gaps or {"missing_tables": [], "missing_columns": {}},
        "empty_state": {
            "is_empty": not risks,
            "message": "No publication review expiry risks found." if not risks else None,
        },
    }


def build_publication_review_expiry_risk_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    gaps = _schema_gaps(schema)
    content_rows = _load_content(conn, schema) if "generated_content" in schema else []
    review_rows = _load_reviews(conn, schema)
    evidence_rows = _load_evidence(conn, schema)
    return build_publication_review_expiry_risk_report(content_rows, review_rows, evidence_rows, schema_gaps=gaps, **kwargs)


def format_publication_review_expiry_risk_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_publication_review_expiry_risk_text(report: dict[str, Any]) -> str:
    lines = [
        "Publication Review Expiry Risk",
        f"Generated: {report['generated_at']}",
        f"Totals: reviewable={report['totals']['reviewable_count']} risks={report['totals']['risk_count']}",
    ]
    if not report["risks"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "content_id | type | status | age_days | evidence_age_days | scheduled_at | score | reasons"])
    for row in report["risks"]:
        lines.append(
            f"{row['content_id']} | {row['content_type'] or '-'} | {row['review_status']} | "
            f"{_dash(row['age_days'])} | {_dash(row['evidence_age_days'])} | {row['scheduled_at'] or '-'} | "
            f"{row['risk_score']} | {','.join(row['reasons']) or '-'}"
        )
    return "\n".join(lines)


format_publication_review_expiry_risk_table = format_publication_review_expiry_risk_text


def _normalize_content(row: dict[str, Any]) -> dict[str, Any]:
    metadata = _json_object(_first(row, "metadata", "raw_metadata"))
    return {
        "id": _text(_first(row, "content_id", "id", "generated_content_id")) or "unknown",
        "content_type": _text(_first(row, "content_type", "type", "format")),
        "status": _text(_first(row, "review_status", "status", "state")).lower(),
        "created_at": _parse_ts(_first(row, "created_at", "generated_at")),
        "approved_at": _parse_ts(_first(row, "approved_at", "reviewed_at")),
        "scheduled_at": _parse_ts(_first(row, "scheduled_at", "publish_at", "intended_publish_at")),
        "published_at": _parse_ts(_first(row, "published_at")),
        "publication_url": _text(_first(row, "publication_url", "published_url", "url")),
        "publish_attempt_count": _int(_first(row, "publish_attempt_count", "attempt_count", "publication_attempt_count")),
        "metadata": metadata,
    }


def _latest_reviews(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[_text(_first(row, "content_id", "generated_content_id", "id"))].append(row)
    return {
        content_id: sorted(items, key=lambda item: _parse_ts(_first(item, "decided_at", "reviewed_at", "updated_at", "created_at")) or datetime.min.replace(tzinfo=timezone.utc))[-1]
        for content_id, items in grouped.items()
        if content_id and items
    }


def _evidence_by_content(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        content_id = _text(_first(row, "content_id", "generated_content_id", "draft_id"))
        if content_id:
            grouped[content_id].append(row)
    return grouped


def _review_status(content: dict[str, Any], review: dict[str, Any]) -> str:
    status = _text(_first(review, "status", "review_status", "decision") or content["status"]).lower()
    if status in {"approve", "approved_for_publication"}:
        return "approved"
    return status


def _max_evidence_age(generated_at: datetime, rows: list[dict[str, Any]], metadata: dict[str, Any]) -> int | None:
    dates = [_parse_ts(_first(row, "evidence_at", "source_published_at", "published_at", "created_at", "updated_at", "fetched_at")) for row in rows]
    dates.extend(_parse_ts(item) for item in _items(metadata.get("source_dates") or metadata.get("evidence_dates")))
    dates = [item for item in dates if item]
    if not dates:
        return None
    return max(_age_days(generated_at, item) or 0 for item in dates)


def _risk_score(
    *,
    age_days: int | None,
    evidence_age_days: int | None,
    scheduled_at: datetime | None,
    attempts: int,
    generated_at: datetime,
    approval_max_age_days: int,
    evidence_max_age_days: int,
) -> int:
    score = 0
    if age_days is not None:
        score += max(age_days - approval_max_age_days, 0) * 3
    if evidence_age_days is not None:
        score += max(evidence_age_days - evidence_max_age_days, 0) * 2
    if scheduled_at and scheduled_at < generated_at:
        score += 25 + min(_age_days(generated_at, scheduled_at) or 0, 30)
    if attempts <= 0:
        score += 20
    return int(score)


def _load_content(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    cols = schema["generated_content"]
    select = [
        _select(cols, ("id", "content_id"), "id"),
        _select(cols, ("content_type", "type", "format"), "content_type"),
        _select(cols, ("status", "review_status", "state"), "status"),
        _select(cols, ("created_at", "generated_at"), "created_at"),
        _select(cols, ("approved_at", "reviewed_at"), "approved_at"),
        _select(cols, ("scheduled_at", "publish_at", "intended_publish_at"), "scheduled_at"),
        _select(cols, ("published_at",), "published_at"),
        _select(cols, ("publication_url", "published_url", "url"), "publication_url"),
        _select(cols, ("publish_attempt_count", "attempt_count"), "publish_attempt_count"),
        _select(cols, ("metadata", "raw_metadata"), "metadata"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM generated_content").fetchall()]


def _load_reviews(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    table = next((name for name in ("content_reviews", "generation_reviews", "review_queue") if name in schema), "")
    if not table:
        return []
    cols = schema[table]
    select = [
        _select(cols, ("content_id", "generated_content_id", "id"), "content_id"),
        _select(cols, ("status", "review_status", "decision"), "status"),
        _select(cols, ("decided_at", "reviewed_at", "updated_at", "created_at"), "decided_at"),
        _select(cols, ("publish_attempt_count", "attempt_count"), "publish_attempt_count"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM {table}").fetchall()]


def _load_evidence(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    table = next((name for name in ("generated_content_sources", "content_sources", "source_evidence") if name in schema), "")
    if not table:
        return []
    cols = schema[table]
    select = [
        _select(cols, ("content_id", "generated_content_id", "draft_id"), "content_id"),
        _select(cols, ("evidence_at", "source_published_at", "published_at", "created_at", "updated_at", "fetched_at"), "evidence_at"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM {table}").fetchall()]


def _schema_gaps(schema: dict[str, set[str]]) -> dict[str, Any]:
    missing = [] if "generated_content" in schema else ["generated_content"]
    return {"missing_tables": missing, "missing_columns": {}}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def _select(columns: set[str], candidates: tuple[str, ...], alias: str) -> str:
    for candidate in candidates:
        if candidate in columns:
            return candidate if candidate == alias else f"{candidate} AS {alias}"
    return f"NULL AS {alias}"


def _first(row: dict[str, Any], *keys: str) -> Any:
    return next((row[key] for key in keys if key in row and row[key] not in (None, "")), None)


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _items(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple | set):
        return list(value)
    if isinstance(value, str):
        parsed = _json_object_or_list(value)
        if isinstance(parsed, list):
            return parsed
        return [part.strip() for part in value.split(",") if part.strip()]
    return [value]


def _json_object(value: Any) -> dict[str, Any]:
    parsed = _json_object_or_list(value)
    return parsed if isinstance(parsed, dict) else {}


def _json_object_or_list(value: Any) -> Any:
    if isinstance(value, dict | list):
        return value
    if not value:
        return {}
    try:
        return json.loads(str(value))
    except (TypeError, ValueError):
        return {}


def _parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return _utc(value)
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _age_days(now: datetime, value: datetime | None) -> int | None:
    if not value:
        return None
    return max((now - value).days, 0)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _dash(value: Any) -> Any:
    return "-" if value is None else value
