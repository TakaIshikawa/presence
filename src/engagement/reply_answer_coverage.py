"""Report pending reply drafts that do not answer inbound questions."""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any


DEFAULT_STATUS = "pending"
DEFAULT_LIMIT = 100
EXCERPT_CHARS = 160

REASON_EMPTY_DRAFT = "empty_draft"
REASON_EVASIVE_DRAFT = "evasive_draft"
REASON_MISSING_ANSWER_SIGNAL = "missing_answer_signal"

_URL_RE = re.compile(r"https?://\S+|www\.\S+", re.I)
_TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9'-]*")
_QUESTION_SENTENCE_RE = re.compile(r"([^.!?\n\r]{0,260}\?+)")

_QUESTION_OPENERS = {
    "can",
    "could",
    "do",
    "does",
    "did",
    "how",
    "is",
    "are",
    "was",
    "were",
    "what",
    "when",
    "where",
    "which",
    "who",
    "why",
    "will",
    "would",
    "should",
}

_QUESTION_PHRASES = (
    "any advice",
    "any idea",
    "any recommendations",
    "can you help",
    "could you explain",
    "could you share",
    "do you recommend",
    "how should i",
    "please explain",
    "please help",
    "what should i",
    "would love your take",
)

_EVASIVE_PHRASES = (
    "good question",
    "hard to say",
    "i don't know",
    "i do not know",
    "i'm not sure",
    "im not sure",
    "it depends",
    "let me check",
    "need more context",
    "not sure",
    "would need more context",
)

_ANSWER_PATTERNS = (
    re.compile(r"\b(?:yes|no)\b", re.I),
    re.compile(
        r"\b(?:because|the answer is|the fix is|the issue is|it is|it's|"
        r"try|use|set|run|check|ship|add|remove|change|avoid|prefer)\b",
        re.I,
    ),
)

_STOP_WORDS = {
    "about",
    "after",
    "again",
    "also",
    "because",
    "been",
    "being",
    "could",
    "does",
    "from",
    "have",
    "help",
    "into",
    "just",
    "like",
    "more",
    "need",
    "only",
    "please",
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
    "which",
    "with",
    "would",
    "your",
}


@dataclass(frozen=True)
class ReplyAnswerCoverageFinding:
    """A pending reply draft that appears not to answer an inbound question."""

    reply_queue_id: int | None
    inbound_id: str
    author_handle: str | None
    platform: str
    priority: str
    status: str
    question_excerpt: str
    draft_excerpt: str
    missing_answer_reason: str
    detected_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_reply_answer_coverage_report(
    db: Any,
    *,
    status: str | None = DEFAULT_STATUS,
    platform: str | None = None,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a deterministic report of question replies with weak answer coverage."""

    if limit <= 0:
        raise ValueError("limit must be positive")
    if status is not None and not status.strip():
        raise ValueError("status must not be blank")
    if platform is not None and not platform.strip():
        raise ValueError("platform must not be blank")

    generated_at = _as_utc(now or datetime.now(timezone.utc)).isoformat()
    rows = _reply_rows(_connection(db), status=status, platform=platform, limit=limit)

    question_rows = []
    findings: list[ReplyAnswerCoverageFinding] = []
    for row in rows:
        if not is_question_like(row):
            continue
        question_rows.append(row)
        reason = missing_answer_reason(row)
        if reason:
            findings.append(_finding(row, reason))

    findings.sort(
        key=lambda item: (
            _priority_rank(item.priority),
            item.platform,
            item.detected_at or "",
            item.reply_queue_id or 0,
            item.inbound_id,
        )
    )
    return {
        "artifact_type": "reply_answer_coverage",
        "generated_at": generated_at,
        "filters": {
            "status": status,
            "platform": platform,
            "limit": limit,
        },
        "counts": {
            "rows_scanned": len(rows),
            "question_replies": len(question_rows),
            "non_question_replies": len(rows) - len(question_rows),
            "unresolved_questions": len(findings),
        },
        "items": [finding.to_dict() for finding in findings],
    }


def is_question_like(row: dict[str, Any]) -> bool:
    """Return whether a reply_queue-like row has deterministic question signals."""

    inbound = str(row.get("inbound_text") or "")
    normalized = _normalize(inbound)
    if not normalized:
        return False
    if "?" in inbound:
        return True
    intent = str(row.get("intent") or "").strip().lower()
    if intent in {"question", "bug_report", "support_request"}:
        return True
    tokens = _TOKEN_RE.findall(normalized)
    if tokens and tokens[0] in _QUESTION_OPENERS:
        return True
    if len(tokens) > 1 and tokens[0] in {"hey", "hi", "hello"} and tokens[1] in _QUESTION_OPENERS:
        return True
    return any(phrase in normalized for phrase in _QUESTION_PHRASES)


def missing_answer_reason(row: dict[str, Any]) -> str | None:
    """Return a stable missing-answer reason, or None when the draft likely answers."""

    draft = str(row.get("draft_text") or "").strip()
    if not draft:
        return REASON_EMPTY_DRAFT

    normalized_draft = _normalize(draft)
    if _contains_evasive_phrase(normalized_draft) and not _has_answer_signal(draft):
        return REASON_EVASIVE_DRAFT

    inbound_terms = _content_terms(str(row.get("inbound_text") or ""))
    draft_terms = _content_terms(draft)
    overlap = inbound_terms.intersection(draft_terms)
    if _has_answer_signal(draft) or len(overlap) >= 2:
        return None
    return REASON_MISSING_ANSWER_SIGNAL


def format_reply_answer_coverage_json(report: dict[str, Any]) -> str:
    """Render a reply answer coverage report as deterministic JSON."""

    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_answer_coverage_text(report: dict[str, Any]) -> str:
    """Render a concise human-readable reply answer coverage report."""

    counts = report["counts"]
    lines = [
        "Reply Answer Coverage Report",
        f"Generated: {report['generated_at']}",
        (
            f"Filters: status={report['filters']['status'] or 'all'} "
            f"platform={report['filters']['platform'] or 'all'} "
            f"limit={report['filters']['limit']}"
        ),
        (
            f"Rows: scanned={counts['rows_scanned']} "
            f"questions={counts['question_replies']} "
            f"non_questions={counts['non_question_replies']} "
            f"unresolved={counts['unresolved_questions']}"
        ),
    ]
    if not report["items"]:
        lines.append("No unanswered question-like reply drafts found.")
        return "\n".join(lines)

    lines.extend(["", "Unresolved questions:"])
    for item in report["items"]:
        author = _display_handle(item.get("author_handle"))
        lines.append(
            f"- reply={item['reply_queue_id']} inbound={item['inbound_id']} "
            f"{item['platform']} {author} priority={item['priority']} "
            f"reason={item['missing_answer_reason']}"
        )
        lines.append(f"  question: {item['question_excerpt']}")
        lines.append(f"  draft: {item['draft_excerpt'] or '(empty)'}")
    return "\n".join(lines)


def _reply_rows(
    conn: sqlite3.Connection,
    *,
    status: str | None,
    platform: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    columns = _table_columns(conn, "reply_queue")
    required = {"inbound_text", "draft_text"}
    if not columns or not required.issubset(columns):
        return []

    filters: list[str] = []
    params: list[Any] = []
    if status is not None and "status" in columns:
        filters.append("LOWER(COALESCE(status, ?)) = ?")
        params.extend([DEFAULT_STATUS, status.lower()])
    if platform is not None and "platform" in columns:
        filters.append("LOWER(COALESCE(platform, 'x')) = ?")
        params.append(platform.lower())

    query = "SELECT * FROM reply_queue"
    if filters:
        query += " WHERE " + " AND ".join(filters)
    query += " ORDER BY " + _order_clause(columns) + " LIMIT ?"
    params.append(limit)
    return [dict(row) for row in conn.execute(query, params).fetchall()]


def _finding(row: dict[str, Any], reason: str) -> ReplyAnswerCoverageFinding:
    return ReplyAnswerCoverageFinding(
        reply_queue_id=_int_or_none(row.get("id")),
        inbound_id=str(row.get("inbound_tweet_id") or row.get("id") or ""),
        author_handle=row.get("inbound_author_handle"),
        platform=str(row.get("platform") or "x"),
        priority=str(row.get("priority") or "normal"),
        status=str(row.get("status") or DEFAULT_STATUS),
        question_excerpt=_question_excerpt(str(row.get("inbound_text") or "")),
        draft_excerpt=_excerpt(str(row.get("draft_text") or "")),
        missing_answer_reason=reason,
        detected_at=row.get("detected_at"),
    )


def _connection(db: Any) -> sqlite3.Connection:
    return db.conn if hasattr(db, "conn") else db


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _order_clause(columns: set[str]) -> str:
    parts = []
    if "priority" in columns:
        parts.append(
            "CASE LOWER(COALESCE(priority, 'normal')) "
            "WHEN 'urgent' THEN 0 WHEN 'high' THEN 1 WHEN 'normal' THEN 2 "
            "WHEN 'low' THEN 3 ELSE 4 END"
        )
    if "detected_at" in columns:
        parts.append("datetime(detected_at) ASC")
    parts.append("id ASC" if "id" in columns else "rowid ASC")
    return ", ".join(parts)


def _has_answer_signal(draft: str) -> bool:
    return any(pattern.search(draft) for pattern in _ANSWER_PATTERNS)


def _contains_evasive_phrase(normalized: str) -> bool:
    return any(phrase in normalized for phrase in _EVASIVE_PHRASES)


def _content_terms(text: str) -> set[str]:
    return {
        token
        for token in _TOKEN_RE.findall(_normalize(text))
        if len(token) >= 4 and token not in _STOP_WORDS
    }


def _question_excerpt(text: str) -> str:
    compact = " ".join(text.split())
    match = _QUESTION_SENTENCE_RE.search(compact)
    return _excerpt(match.group(1).strip() if match else compact)


def _excerpt(text: str) -> str:
    compact = " ".join(text.split())
    if len(compact) <= EXCERPT_CHARS:
        return compact
    return compact[: EXCERPT_CHARS - 3].rstrip() + "..."


def _normalize(text: str) -> str:
    value = _URL_RE.sub(" ", text.lower())
    value = re.sub(r"@\w+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def _display_handle(value: Any) -> str:
    handle = str(value or "unknown").strip()
    if not handle:
        return "@unknown"
    return handle if handle.startswith("@") else f"@{handle}"


def _priority_rank(priority: str) -> int:
    return {"urgent": 0, "high": 1, "normal": 2, "low": 3}.get(priority.lower(), 4)


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
