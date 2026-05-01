"""Detect inbound mentions that are likely direct unanswered questions."""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


DEFAULT_DAYS = 7
DEFAULT_MIN_SCORE = 45.0
QUESTION_PREVIEW_CHARS = 140

_TOKEN_RE = re.compile(r"[a-z0-9']+")
_HANDLE_RE = re.compile(r"@\w+")
_URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
_QUESTION_SENTENCE_RE = re.compile(r"([^.!?\n\r]{0,220}\?+)")

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

_INTERROGATIVE_PHRASES = (
    "can you",
    "could you",
    "do you",
    "does this",
    "how do",
    "how would",
    "what do you",
    "what would",
    "why does",
    "would you",
)

_REQUEST_PHRASES = (
    "any advice",
    "any idea",
    "any recommendations",
    "can you help",
    "could you explain",
    "could you share",
    "do you recommend",
    "help me",
    "how should i",
    "i need help",
    "please explain",
    "please help",
    "what should i",
    "would love your take",
)

_SUPPORT_PHRASES = (
    "bug",
    "broken",
    "can't install",
    "cant install",
    "crash",
    "crashes",
    "doesn't work",
    "doesnt work",
    "error",
    "exception",
    "fails",
    "failure",
    "not working",
    "regression",
    "traceback",
)

_RHETORICAL_PHRASES = (
    "am i right",
    "does anyone else",
    "does that make sense",
    "how hard can it be",
    "isn't it",
    "isnt it",
    "right?",
    "what could go wrong",
    "who knew",
    "why bother",
)

_GENERIC_LOW_SIGNAL = (
    "any thoughts",
    "thoughts?",
    "wdyt",
    "what do you think?",
)

_RESOLVED_STATUSES = {"approved", "posted", "dismissed", "done", "sent", "resolved"}


@dataclass(frozen=True)
class ReplyQuestionFinding:
    """A direct-question candidate from an inbound mention."""

    mention_id: str
    reply_queue_id: int | None
    platform: str
    author: str | None
    question_preview: str
    score: float
    reasons: list[str]
    status: str
    resolved: bool
    detected_at: str | None = None
    inbound_url: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def detect_reply_questions(
    db: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_score: float = DEFAULT_MIN_SCORE,
    include_resolved: bool = False,
    now: datetime | None = None,
) -> list[ReplyQuestionFinding]:
    """Return inbound mentions likely asking direct questions.

    The detector is intentionally heuristic and deterministic. It works from the
    reply_queue table when present and returns an empty list for schemas without
    inbound reply data.
    """

    if days <= 0:
        raise ValueError("days must be positive")
    if min_score < 0:
        raise ValueError("min_score must be non-negative")

    conn = _connection(db)
    columns = _table_columns(conn, "reply_queue")
    if not columns or "inbound_text" not in columns:
        return []

    now = _as_utc(now or datetime.now(timezone.utc))
    rows = _fetch_candidate_rows(conn, columns, days=days, now=now)
    findings: list[ReplyQuestionFinding] = []
    for row in rows:
        finding = score_reply_question(row, include_resolved=include_resolved)
        if finding is None:
            continue
        if finding.score >= min_score:
            findings.append(finding)

    return sorted(
        findings,
        key=lambda item: (
            -item.score,
            item.detected_at or "",
            item.reply_queue_id or 0,
            item.mention_id,
        ),
    )


def score_reply_question(
    row: dict[str, Any],
    *,
    include_resolved: bool = False,
) -> ReplyQuestionFinding | None:
    """Score a single reply_queue row as a direct inbound question candidate."""

    text = str(row.get("inbound_text") or "")
    normalized = _normalize(text)
    if not normalized:
        return None

    resolved = _is_resolved(row)
    if resolved and not include_resolved:
        return None

    score = 0.0
    reasons: list[str] = []

    question_marks = text.count("?")
    if question_marks:
        score += min(30.0, 18.0 + question_marks * 4.0)
        reasons.append("question mark")

    opener = _question_opener(normalized)
    if opener:
        score += 22.0
        reasons.append(f"interrogative opener: {opener}")

    phrase = _first_phrase(normalized, _INTERROGATIVE_PHRASES)
    if phrase:
        score += 16.0
        reasons.append(f"direct ask phrase: {phrase}")

    request = _first_phrase(normalized, _REQUEST_PHRASES)
    if request:
        score += 18.0
        reasons.append(f"request intent: {request}")

    support = _first_phrase(normalized, _SUPPORT_PHRASES)
    if support:
        score += 16.0
        reasons.append(f"support signal: {support}")

    intent = str(row.get("intent") or "").strip().lower()
    if intent == "question":
        score += 16.0
        reasons.append("classified as question")
    elif intent == "bug_report":
        score += 12.0
        reasons.append("classified as bug report")
    elif intent in {"appreciation", "spam"}:
        score -= 20.0
        reasons.append(f"classified as {intent}")

    priority = str(row.get("priority") or "").strip().lower()
    if priority == "high":
        score += 6.0
        reasons.append("high priority")
    elif priority == "low":
        score -= 8.0
        reasons.append("low priority")

    rhetorical = _first_phrase(normalized, _RHETORICAL_PHRASES)
    if rhetorical:
        score -= 26.0
        reasons.append(f"rhetorical pattern: {rhetorical}")

    generic = _first_phrase(normalized, _GENERIC_LOW_SIGNAL)
    if generic and score < 65.0:
        score -= 10.0
        reasons.append(f"generic mention: {generic}")

    if resolved:
        reasons.append("resolved reply state")
    elif _has_draft(row):
        reasons.append("unresolved draft exists")
    else:
        score += 8.0
        reasons.append("no draft yet")

    if not reasons or score <= 0:
        return None

    return ReplyQuestionFinding(
        mention_id=str(row.get("inbound_tweet_id") or row.get("id") or ""),
        reply_queue_id=_int_or_none(row.get("id")),
        platform=str(row.get("platform") or "x"),
        author=row.get("inbound_author_handle"),
        question_preview=_question_preview(text),
        score=round(max(0.0, score), 1),
        reasons=reasons,
        status=str(row.get("status") or "pending"),
        resolved=resolved,
        detected_at=row.get("detected_at"),
        inbound_url=row.get("inbound_url"),
    )


def build_reply_question_report(
    db: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_score: float = DEFAULT_MIN_SCORE,
    include_resolved: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    findings = detect_reply_questions(
        db,
        days=days,
        min_score=min_score,
        include_resolved=include_resolved,
        now=now,
    )
    generated_at = _as_utc(now or datetime.now(timezone.utc)).isoformat()
    return {
        "generated_at": generated_at,
        "filters": {
            "days": days,
            "include_resolved": include_resolved,
            "min_score": min_score,
        },
        "total": len(findings),
        "questions": [finding.to_dict() for finding in findings],
    }


def format_reply_question_report_text(report: dict[str, Any]) -> str:
    if not report["questions"]:
        return "No direct unanswered reply questions matched."

    lines = [f"Reply Questions ({report['total']})"]
    for item in report["questions"]:
        author = f"@{item['author']}" if item.get("author") else "@unknown"
        resolved = " resolved" if item.get("resolved") else ""
        lines.append(
            f"#{item['mention_id']} {author} score={item['score']:.1f}{resolved} "
            f"{item['question_preview']}"
        )
        lines.append(f"  reasons: {', '.join(item['reasons'])}")
    return "\n".join(lines)


def format_reply_question_report_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def _fetch_candidate_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    days: int,
    now: datetime,
) -> list[dict[str, Any]]:
    filters: list[str] = []
    params: list[Any] = []
    if "detected_at" in columns:
        cutoff = now - timedelta(days=days)
        filters.append("(detected_at IS NULL OR datetime(detected_at) >= datetime(?))")
        params.append(cutoff.isoformat())

    query = "SELECT * FROM reply_queue"
    if filters:
        query += " WHERE " + " AND ".join(filters)
    query += " ORDER BY " + _order_clause(columns)
    return [dict(row) for row in conn.execute(query, params).fetchall()]


def _order_clause(columns: set[str]) -> str:
    parts = []
    if "detected_at" in columns:
        parts.append("datetime(detected_at) ASC")
    if "id" in columns:
        parts.append("id ASC")
    return ", ".join(parts) or "rowid ASC"


def _connection(db: Any) -> sqlite3.Connection:
    return db.conn if hasattr(db, "conn") else db


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _is_resolved(row: dict[str, Any]) -> bool:
    status = str(row.get("status") or "").strip().lower()
    if status in _RESOLVED_STATUSES:
        return True
    return any(row.get(column) for column in ("posted_at", "posted_tweet_id", "posted_platform_id"))


def _has_draft(row: dict[str, Any]) -> bool:
    return bool(str(row.get("draft_text") or "").strip())


def _normalize(text: str) -> str:
    value = _URL_RE.sub(" ", text.lower())
    value = _HANDLE_RE.sub(" ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def _question_opener(normalized: str) -> str:
    tokens = _TOKEN_RE.findall(normalized)
    if not tokens:
        return ""
    if tokens[0] in _QUESTION_OPENERS:
        return tokens[0]
    if len(tokens) > 1 and tokens[0] in {"hey", "hi", "hello"} and tokens[1] in _QUESTION_OPENERS:
        return tokens[1]
    return ""


def _first_phrase(normalized: str, phrases: tuple[str, ...]) -> str:
    for phrase in phrases:
        if phrase in normalized:
            return phrase
    return ""


def _question_preview(text: str) -> str:
    compact = " ".join(text.split())
    match = _QUESTION_SENTENCE_RE.search(compact)
    preview = match.group(1).strip() if match else compact
    if len(preview) <= QUESTION_PREVIEW_CHARS:
        return preview
    return preview[: QUESTION_PREVIEW_CHARS - 3].rstrip() + "..."


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
