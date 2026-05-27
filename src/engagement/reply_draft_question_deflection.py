"""Report reply drafts that appear to deflect question-like targets."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any


ARTIFACT_TYPE = "reply_draft_question_deflection"
DEFAULT_STATUS = "pending,queued"
DEFAULT_WINDOW_DAYS = 14
DEFAULT_MIN_QUESTION_SCORE = 2
DEFAULT_LIMIT = 100
TABLES = ("reply_drafts", "reply_queue", "reply_draft_queue")
TARGET_COLUMNS = ("target_text", "inbound_text", "mention_text", "target_body")
DRAFT_COLUMNS = ("draft_text", "content", "body", "reply_text")
METADATA_COLUMNS = ("raw_target_metadata", "target_metadata", "platform_metadata", "metadata")
QUESTION_TERMS = re.compile(r"\b(what|why|how|when|where|who|which|can|could|should|would|is|are|do|does|did)\b", re.I)
ANSWER_TERMS = re.compile(r"\b(because|means|you can|you should|the reason|next step|try|use|check|here'?s|i don'?t know|not sure|depends|http)\b", re.I)
GENERIC_TERMS = re.compile(r"\b(thanks|interesting|great question|good question|appreciate|will look|circle back)\b", re.I)


def build_reply_draft_question_deflection_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    table = next((name for name in TABLES if name in schema), None)
    if not table:
        return build_reply_draft_question_deflection_report([], missing_tables=["reply_drafts|reply_queue"], **kwargs)
    columns = schema[table]
    missing = sorted({"id"} - columns)
    if not set(DRAFT_COLUMNS) & columns:
        missing.append("|".join(DRAFT_COLUMNS))
    rows = _load_rows(conn, table, columns) if not missing else []
    return build_reply_draft_question_deflection_report(rows, missing_columns={table: missing} if missing else None, **kwargs)


def build_reply_draft_question_deflection_report(
    rows: list[dict[str, Any]],
    *,
    status: str | None = DEFAULT_STATUS,
    window_days: int = DEFAULT_WINDOW_DAYS,
    min_question_score: int = DEFAULT_MIN_QUESTION_SCORE,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    if window_days <= 0:
        raise ValueError("window_days must be positive")
    if min_question_score <= 0:
        raise ValueError("min_question_score must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=window_days)
    wanted = _status_set(status)
    findings: list[dict[str, Any]] = []
    scanned = 0
    for row in rows:
        draft_status = _norm(row.get("status") or row.get("draft_status")) or "unknown"
        if wanted and draft_status not in wanted:
            continue
        created_at = _parse_time(row.get("created_at") or row.get("updated_at"))
        if created_at and created_at < cutoff:
            continue
        scanned += 1
        target_text = str(row.get("target_text") or _target_from_metadata(row.get("metadata")) or "")
        draft_text = str(row.get("draft_text") or "")
        score = question_score(target_text)
        reason = deflection_reason(target_text, draft_text, score, min_question_score)
        if reason:
            findings.append(
                {
                    "reply_draft_id": row.get("id") or row.get("reply_draft_id"),
                    "platform": _norm(row.get("platform")) or "unknown",
                    "status": draft_status,
                    "target_text": target_text,
                    "draft_text": draft_text,
                    "deflection_reason": reason,
                    "question_score": score,
                }
            )
    findings.sort(key=lambda item: (-item["question_score"], item["platform"], _sort_id(item["reply_draft_id"])))
    shown = findings[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated_at.isoformat(),
        "filters": {"status": status, "window_days": window_days, "min_question_score": min_question_score, "limit": limit},
        "summary": {
            "drafts_scanned": scanned,
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_reason": dict(sorted(Counter(item["deflection_reason"] for item in findings).items())),
        },
        "findings": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(cols) for table, cols in sorted((missing_columns or {}).items()) if cols},
    }


def question_score(text: str) -> int:
    cleaned = str(text or "")
    return cleaned.count("?") * 2 + len(QUESTION_TERMS.findall(cleaned))


def deflection_reason(target_text: str, draft_text: str, score: int, min_question_score: int) -> str | None:
    if score < min_question_score:
        return None
    draft = str(draft_text or "").strip()
    if not draft:
        return "missing_reply_text"
    if ANSWER_TERMS.search(draft) or re.search(r"\b\d+(\.\d+)?\b", draft):
        return None
    if GENERIC_TERMS.search(draft):
        return "generic_acknowledgement_without_answer"
    if len(draft.split()) < 8:
        return "reply_too_short_to_answer"
    return "no_answer_signal"


def format_reply_draft_question_deflection_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_draft_question_deflection_text(report: dict[str, Any]) -> str:
    lines = [
        "Reply Draft Question Deflection",
        f"Generated: {report['generated_at']}",
        f"Filters: status={report['filters']['status'] or '*'} window_days={report['filters']['window_days']} min_question_score={report['filters']['min_question_score']} limit={report['filters']['limit']}",
        f"Totals: drafts={report['summary']['drafts_scanned']} findings={report['summary']['finding_count']} shown={report['summary']['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + ", ".join(f"{t}.{c}" for t, cols in report["missing_columns"].items() for c in cols))
    if not report["findings"]:
        lines.append("No question deflection issues found.")
        return "\n".join(lines)
    lines.append("Findings:")
    for f in report["findings"]:
        lines.append(f"  - draft={f['reply_draft_id']} platform={f['platform']} status={f['status']} score={f['question_score']} reason={f['deflection_reason']}")
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, table: str, columns: set[str]) -> list[dict[str, Any]]:
    aliases = {
        "id": ("id",),
        "platform": ("platform",),
        "status": ("draft_status", "status"),
        "target_text": TARGET_COLUMNS,
        "draft_text": DRAFT_COLUMNS,
        "metadata": METADATA_COLUMNS,
        "created_at": ("created_at",),
        "updated_at": ("updated_at",),
    }
    select = ", ".join(f"{_coalesce(columns, names)} AS {alias}" for alias, names in aliases.items())
    return [dict(row) for row in conn.execute(f"SELECT {select} FROM {table} ORDER BY id ASC").fetchall()]


def _target_from_metadata(value: Any) -> str:
    data = _json(value)
    if isinstance(data, dict):
        for key in ("target_text", "text", "body", "content"):
            if data.get(key):
                return str(data[key])
    return ""


def _json(value: Any) -> Any:
    if not value:
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except json.JSONDecodeError:
        return None


def _coalesce(columns: set[str], names: tuple[str, ...]) -> str:
    available = [name for name in names if name in columns]
    return "COALESCE(" + ", ".join(available) + ")" if len(available) > 1 else (available[0] if available else "NULL")


def _status_set(value: str | None) -> set[str]:
    return {_norm(part) for part in str(value or "").split(",") if _norm(part)}


def _norm(value: Any) -> str | None:
    text = str(value or "").strip().lower()
    return text or None


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _sort_id(value: Any) -> tuple[int, Any]:
    try:
        return (0, int(value))
    except (TypeError, ValueError):
        return (1, str(value or ""))


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {row["name"]: {col["name"] for col in conn.execute(f"PRAGMA table_info({row['name']})")} for row in rows}


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
