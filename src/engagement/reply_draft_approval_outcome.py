"""Summarize reply draft approval outcomes."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100


def build_reply_draft_approval_outcome_report(
    rows: list[dict[str, Any]],
    *,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    records = []
    outcomes = Counter()
    by_score: dict[str, Counter[str]] = defaultdict(Counter)
    for row in rows:
        created_at = _parse_ts(_first(row, "created_at", "drafted_at", "updated_at"))
        age_days = round((generated_at - created_at).total_seconds() / 86400, 2) if created_at else None
        score_bucket = _score_bucket(_first(row, "score", "quality_score", "confidence"))
        context_status = "has_context" if _text(_first(row, "relationship_context", "context", "context_summary")) else "missing_context"
        outcome = _outcome(_first(row, "review_outcome", "outcome", "status", "decision"))
        disposition = _text(_first(row, "final_disposition", "disposition", "sent_status")) or outcome
        outcomes[outcome] += 1
        by_score[score_bucket][outcome] += 1
        records.append(
            {
                "draft_id": _text(_first(row, "draft_id", "id")) or "unknown",
                "mention_id": _text(_first(row, "mention_id", "inbound_id")) or None,
                "score_bucket": score_bucket,
                "context_status": context_status,
                "review_outcome": outcome,
                "final_disposition": disposition,
                "age_days": age_days,
            }
        )
    records.sort(key=lambda item: (item["review_outcome"], -(item["age_days"] or 0), item["draft_id"]))
    total = len(records)
    approved = outcomes["approved"]
    return {
        "artifact_type": "reply_draft_approval_outcome",
        "generated_at": generated_at.isoformat(),
        "filters": {"limit": limit},
        "totals": {
            "draft_count": total,
            "approved": approved,
            "rejected": outcomes["rejected"],
            "pending": outcomes["pending"],
            "revised": outcomes["revised"],
            "approval_rate": round(approved / total, 4) if total else 0.0,
            "by_score_bucket": [
                {"score_bucket": bucket, "draft_count": sum(counts.values()), **{name: counts[name] for name in ("approved", "rejected", "pending", "revised")}}
                for bucket, counts in sorted(by_score.items())
            ],
        },
        "drafts": records[:limit],
        "empty_state": {"is_empty": not records, "message": "No reply draft rows found." if not records else None},
    }


def build_reply_draft_approval_outcome_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    return build_reply_draft_approval_outcome_report(_load_rows(conn, _schema(conn)), **kwargs)


def format_reply_draft_approval_outcome_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_draft_approval_outcome_text(report: dict[str, Any]) -> str:
    lines = [
        "Reply Draft Approval Outcome",
        f"Generated: {report['generated_at']}",
        (
            f"Totals: drafts={report['totals']['draft_count']} approved={report['totals']['approved']} "
            f"rejected={report['totals']['rejected']} pending={report['totals']['pending']} revised={report['totals']['revised']} "
            f"approval_rate={report['totals']['approval_rate']:.2f}"
        ),
    ]
    if not report["drafts"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "draft_id | mention_id | score_bucket | context | outcome | disposition | age_days"])
    for row in report["drafts"]:
        lines.append(
            f"{row['draft_id']} | {row['mention_id'] or '-'} | {row['score_bucket']} | {row['context_status']} | "
            f"{row['review_outcome']} | {row['final_disposition']} | {row['age_days'] if row['age_days'] is not None else '-'}"
        )
    return "\n".join(lines)


format_reply_draft_approval_outcome_table = format_reply_draft_approval_outcome_text


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    for table in ("reply_drafts", "reply_queue", "reply_reviews"):
        if table not in schema:
            continue
        cols = schema[table]
        selected = [
            _col(cols, "id", "draft_id", default="NULL") + " AS draft_id",
            _col(cols, "mention_id", "inbound_id", default="NULL") + " AS mention_id",
            _col(cols, "score", "quality_score", "confidence", default="NULL") + " AS score",
            _col(cols, "relationship_context", "context", "context_summary", default="NULL") + " AS relationship_context",
            _col(cols, "review_outcome", "outcome", "status", "decision", default="NULL") + " AS review_outcome",
            _col(cols, "final_disposition", "disposition", "sent_status", default="NULL") + " AS final_disposition",
            _col(cols, "created_at", "drafted_at", "updated_at", default="NULL") + " AS created_at",
        ]
        return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM {table}").fetchall()]
    return []


def _outcome(value: Any) -> str:
    text = _text(value).lower()
    if text in {"approved", "accepted", "sent", "published"}:
        return "approved"
    if text in {"rejected", "declined", "discarded"}:
        return "rejected"
    if text in {"revised", "edited", "needs_revision", "changes_requested"}:
        return "revised"
    return "pending"


def _score_bucket(value: Any) -> str:
    try:
        score = float(value)
    except (TypeError, ValueError):
        return "unknown"
    if score <= 1:
        score *= 100
    if score >= 80:
        return "high"
    if score >= 50:
        return "medium"
    return "low"


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _col(columns: set[str], *names: str, default: str = "NULL") -> str:
    for name in names:
        if name in columns:
            return name
    return default


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def _parse_ts(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
