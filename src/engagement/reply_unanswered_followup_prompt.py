"""Report reply drafts where follow-up questions remain unanswered."""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


DEFAULT_DAYS = 7
DEFAULT_LIMIT = 100
EXCERPT_CHARS = 180

_SPACE_RE = re.compile(r"\s+")
_TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9'-]*")
_URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
_QUESTION_SENTENCE_RE = re.compile(r"([^.!?\n\r]{0,260}\?+)")

_QUESTION_OPENERS = {
    "are",
    "can",
    "could",
    "did",
    "do",
    "does",
    "how",
    "is",
    "should",
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
}

_DIRECT_ANSWER_RE = re.compile(
    r"\b(?:yes|no|because|the answer is|the fix is|the issue is|"
    r"use|try|run|set|check|add|remove|change|avoid|prefer)\b",
    re.IGNORECASE,
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
class ReplyUnansweredFollowupPromptFinding:
    """A reply draft where one or more follow-up questions remain unanswered."""

    mention_id: str | None
    draft_id: int | None
    author_handle: str | None
    question_count: int
    answered_question_count: int
    unanswered_question_count: int
    drafted_at: str | None
    warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["warnings"] = list(self.warnings)
        return result


@dataclass(frozen=True)
class ReplyUnansweredFollowupPromptReport:
    """Aggregate report of reply drafts with unanswered follow-up questions."""

    generated_at: str
    filters: dict[str, Any]
    summary: dict[str, int]
    findings: tuple[ReplyUnansweredFollowupPromptFinding, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "reply_unanswered_followup_prompt",
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "summary": dict(self.summary),
            "findings": [finding.to_dict() for finding in self.findings],
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
        }


def build_reply_unanswered_followup_prompt_report(
    db: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ReplyUnansweredFollowupPromptReport:
    """Build a deterministic report of reply drafts with unanswered follow-up questions."""

    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    filters = {"days": days, "limit": limit}

    conn = _connection(db)
    columns = _table_columns(conn, "reply_queue")
    if not columns:
        return _empty_report(generated_at, filters, missing_tables=("reply_queue",))

    required = ("inbound_text", "draft_text")
    missing = tuple(column for column in required if column not in columns)
    if missing:
        return _empty_report(
            generated_at,
            filters,
            missing_columns={"reply_queue": missing},
        )

    rows = _reply_rows(conn, columns, days=days, limit=limit, now=generated_at)
    findings: list[ReplyUnansweredFollowupPromptFinding] = []
    total_drafts_with_questions = 0

    for row in rows:
        finding = inspect_reply_unanswered_followup_prompt(row)
        if finding is None:
            continue
        total_drafts_with_questions += 1
        if finding.unanswered_question_count > 0:
            findings.append(finding)

    findings.sort(
        key=lambda item: (
            item.drafted_at or "",
            item.draft_id or 0,
            item.mention_id or "",
        )
    )

    summary = {
        "total_drafts_with_questions": total_drafts_with_questions,
        "drafts_with_unanswered_questions": len(findings),
        "total_unanswered_questions": sum(f.unanswered_question_count for f in findings),
    }

    return ReplyUnansweredFollowupPromptReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        summary=summary,
        findings=tuple(findings),
    )


def inspect_reply_unanswered_followup_prompt(
    row: dict[str, Any],
) -> ReplyUnansweredFollowupPromptFinding | None:
    """Inspect a reply_queue row for unanswered follow-up questions.

    Returns None if the row has no questions, otherwise returns a finding.
    """

    warnings: list[str] = []
    inbound_text = row.get("inbound_text")
    draft_text = row.get("draft_text")

    if not inbound_text or not str(inbound_text).strip():
        warnings.append("missing_source_text")
        return None

    if not draft_text or not str(draft_text).strip():
        warnings.append("missing_draft_text")

    questions = extract_questions(str(inbound_text))
    if not questions:
        return None

    answered_count = 0
    draft_str = str(draft_text or "")
    for question in questions:
        if is_question_answered(question, draft_str):
            answered_count += 1

    return ReplyUnansweredFollowupPromptFinding(
        mention_id=_str_or_none(row.get("inbound_tweet_id") or row.get("mention_id")),
        draft_id=_int_or_none(row.get("id") or row.get("draft_id") or row.get("reply_queue_id")),
        author_handle=_str_or_none(
            row.get("inbound_author_handle") or row.get("author_handle")
        ),
        question_count=len(questions),
        answered_question_count=answered_count,
        unanswered_question_count=len(questions) - answered_count,
        drafted_at=_str_or_none(row.get("detected_at") or row.get("drafted_at")),
        warnings=tuple(warnings),
    )


def extract_questions(text: str) -> list[str]:
    """Extract all question-like sentences from text."""

    questions: list[str] = []
    compact = _SPACE_RE.sub(" ", text).strip()

    # Find sentences ending with ?
    for match in _QUESTION_SENTENCE_RE.finditer(compact):
        question_text = match.group(1).strip()
        if question_text:
            questions.append(question_text)

    # If no explicit ? questions, check for question opener patterns
    if not questions:
        normalized = _normalize(text)
        tokens = _TOKEN_RE.findall(normalized)
        if tokens:
            first = tokens[0]
            second = tokens[1] if len(tokens) > 1 else ""
            if first in _QUESTION_OPENERS or (
                first in {"hey", "hi", "hello"} and second in _QUESTION_OPENERS
            ):
                questions.append(_shorten(compact, EXCERPT_CHARS))

    return questions


def is_question_answered(question: str, draft: str) -> bool:
    """Determine if a question is answered in the draft."""

    if not draft or not draft.strip():
        return False

    # Check for direct answer signals
    if _DIRECT_ANSWER_RE.search(draft):
        return True

    # Check for semantic overlap (at least 2 shared content terms)
    question_terms = _content_terms(question)
    draft_terms = _content_terms(draft)
    if len(question_terms.intersection(draft_terms)) >= 2:
        return True

    return False


def format_reply_unanswered_followup_prompt_json(
    report: ReplyUnansweredFollowupPromptReport,
) -> str:
    """Render deterministic JSON for automation."""

    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_reply_unanswered_followup_prompt_text(
    report: ReplyUnansweredFollowupPromptReport,
) -> str:
    """Render a compact human-readable report."""

    summary = report.summary
    lines = [
        "Reply Unanswered Follow-up Prompt Report",
        f"Generated: {report.generated_at}",
        f"Filters: days={report.filters['days']} limit={report.filters['limit']}",
        (
            "Summary: "
            f"drafts_with_questions={summary['total_drafts_with_questions']} "
            f"drafts_with_unanswered={summary['drafts_with_unanswered_questions']} "
            f"total_unanswered={summary['total_unanswered_questions']}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        missing = "; ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
            if columns
        )
        if missing:
            lines.append("Missing columns: " + missing)
    if not report.findings:
        lines.append("No unanswered follow-up questions found.")
        return "\n".join(lines)

    lines.extend(["", "Findings:"])
    for finding in report.findings:
        author = f"@{finding.author_handle}" if finding.author_handle else "@unknown"
        lines.append(
            f"- draft={finding.draft_id or '-'} mention={finding.mention_id or '-'} "
            f"{author} questions={finding.question_count} "
            f"answered={finding.answered_question_count} "
            f"unanswered={finding.unanswered_question_count}"
        )
        if finding.warnings:
            lines.append(f"  warnings: {', '.join(finding.warnings)}")
    return "\n".join(lines)


def _reply_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    days: int,
    limit: int,
    now: datetime,
) -> list[dict[str, Any]]:
    where = ["draft_text IS NOT NULL"]
    params: list[Any] = []
    if "detected_at" in columns:
        cutoff = now - timedelta(days=days)
        where.append("(detected_at IS NULL OR datetime(detected_at) >= datetime(?))")
        params.append(cutoff.isoformat())

    query = "SELECT * FROM reply_queue WHERE " + " AND ".join(where)
    query += " ORDER BY " + _order_clause(columns) + " LIMIT ?"
    params.append(limit)
    cursor = conn.execute(query, params)
    names = [description[0] for description in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _order_clause(columns: set[str]) -> str:
    parts = []
    if "detected_at" in columns:
        parts.append("datetime(detected_at) DESC")
    parts.append("id DESC" if "id" in columns else "rowid DESC")
    return ", ".join(parts)


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    *,
    missing_tables: tuple[str, ...] = (),
    missing_columns: dict[str, tuple[str, ...]] | None = None,
) -> ReplyUnansweredFollowupPromptReport:
    return ReplyUnansweredFollowupPromptReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        summary={
            "total_drafts_with_questions": 0,
            "drafts_with_unanswered_questions": 0,
            "total_unanswered_questions": 0,
        },
        findings=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _connection(db: Any) -> sqlite3.Connection:
    return db.conn if hasattr(db, "conn") else db


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _content_terms(text: str) -> set[str]:
    return {
        token
        for token in _TOKEN_RE.findall(_normalize(text))
        if len(token) >= 4 and token not in _STOP_WORDS
    }


def _normalize(text: str) -> str:
    value = _URL_RE.sub(" ", text.lower())
    value = re.sub(r"@\w+", " ", value)
    value = _SPACE_RE.sub(" ", value)
    return value.strip()


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _str_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _shorten(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 3].rstrip() + "..."
