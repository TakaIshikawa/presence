"""Audit whether reply drafts cover explicit inbound questions."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_DAYS = 7
DEFAULT_LIMIT = 100

REASON_EMPTY_DRAFT = "empty_draft"
REASON_EVASIVE_GENERIC_REPLY = "evasive_generic_reply"
REASON_MISSING_QUESTION_COVERAGE = "missing_question_coverage"

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
_UNCERTAINTY_RE = re.compile(
    r"\b(?:i(?:'| a)m not sure|i do not know|i don't know|not sure|"
    r"need more context|would need more context|can't tell|cannot tell)\b",
    re.IGNORECASE,
)
_GENERIC_REPLY_RE = re.compile(
    r"\b(?:good question|great question|thanks for asking|thanks for sharing|"
    r"appreciate you asking|interesting point)\b",
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
class ReplyQuestionCoverageFinding:
    """One reply draft that appears not to cover an explicit question."""

    mention_id: str | None
    draft_id: int | None
    question_text: str
    reason: str
    author_handle: str | None = None
    platform: str = "x"
    status: str | None = None
    draft_preview: str = ""
    detected_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ReplyQuestionCoverageReport:
    """Aggregate explicit-question coverage audit."""

    ok: bool
    generated_at: str
    filters: dict[str, Any]
    summary: dict[str, int]
    findings: tuple[ReplyQuestionCoverageFinding, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    @property
    def blocking_issue_count(self) -> int:
        return self.summary["uncovered_count"]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "reply_question_coverage",
            "ok": self.ok,
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "summary": dict(self.summary),
            "blocking_issue_count": self.blocking_issue_count,
            "findings": [finding.to_dict() for finding in self.findings],
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
        }


def build_reply_question_coverage_audit(
    source: Any | None = None,
    *,
    reply_records: Iterable[Mapping[str, Any]] | None = None,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ReplyQuestionCoverageReport:
    """Build a deterministic audit of draft coverage for explicit questions."""

    if reply_records is not None and source is not None:
        raise ValueError("provide either source or reply_records, not both")
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    filters = {"days": days, "limit": limit}

    if reply_records is not None or _is_records(source):
        records = reply_records if reply_records is not None else source
        rows = [dict(row) for row in list(records)[:limit]]
        return _report_from_rows(rows, generated_at=generated_at, filters=filters)

    if source is None:
        raise ValueError("source or reply_records is required")

    conn = _connection(source)
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
    return _report_from_rows(rows, generated_at=generated_at, filters=filters)


def inspect_reply_question_coverage_row(
    row: Mapping[str, Any],
) -> tuple[bool, ReplyQuestionCoverageFinding | None]:
    """Inspect one reply_queue-like row.

    Returns ``(is_explicit_question, finding)`` so callers can count covered
    questions without flagging non-question mentions.
    """

    question_text = explicit_question_text(row.get("inbound_text"))
    if not question_text:
        return False, None

    reason = missing_question_coverage_reason(row)
    if reason is None:
        return True, None
    return True, _finding(row, question_text, reason)


def explicit_question_text(value: Any) -> str:
    """Extract the explicit question text from mention text, if present."""

    text = _normalized_text(value)
    if not text:
        return ""
    match = _QUESTION_SENTENCE_RE.search(text)
    if match:
        return _shorten(match.group(1).strip(), 180)
    tokens = _TOKEN_RE.findall(_normalize(text))
    if not tokens:
        return ""
    first = tokens[0]
    second = tokens[1] if len(tokens) > 1 else ""
    if first in _QUESTION_OPENERS or (first in {"hey", "hi", "hello"} and second in _QUESTION_OPENERS):
        return _shorten(text, 180)
    return ""


def missing_question_coverage_reason(row: Mapping[str, Any]) -> str | None:
    """Return why a draft misses question coverage, or None when covered."""

    draft = _normalized_text(row.get("draft_text"))
    if not draft:
        return REASON_EMPTY_DRAFT
    if _DIRECT_ANSWER_RE.search(draft) or _UNCERTAINTY_RE.search(draft):
        return None

    inbound_terms = _content_terms(_normalized_text(row.get("inbound_text")))
    draft_terms = _content_terms(draft)
    if len(inbound_terms.intersection(draft_terms)) >= 2:
        return None
    if _GENERIC_REPLY_RE.search(draft):
        return REASON_EVASIVE_GENERIC_REPLY
    return REASON_MISSING_QUESTION_COVERAGE


def format_reply_question_coverage_json(report: ReplyQuestionCoverageReport) -> str:
    """Render deterministic JSON for automation."""

    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_reply_question_coverage_text(report: ReplyQuestionCoverageReport) -> str:
    """Render a compact human-readable question coverage audit."""

    summary = report.summary
    lines = [
        "Reply Question Coverage Audit",
        f"Generated: {report.generated_at}",
        f"Filters: days={report.filters['days']} limit={report.filters['limit']}",
        (
            "Summary: "
            f"total_questions={summary['total_questions']} "
            f"covered={summary['covered_count']} "
            f"uncovered={summary['uncovered_count']}"
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
        lines.append("No uncovered explicit questions found.")
        return "\n".join(lines)

    lines.extend(["", "Findings:"])
    for finding in report.findings:
        author = f"@{finding.author_handle}" if finding.author_handle else "@unknown"
        lines.append(
            f"- draft={finding.draft_id or '-'} mention={finding.mention_id or '-'} "
            f"platform={finding.platform} {author} reason={finding.reason}"
        )
        lines.append(f"  question={finding.question_text!r}")
        if finding.draft_preview:
            lines.append(f"  draft={finding.draft_preview!r}")
    return "\n".join(lines)


def _report_from_rows(
    rows: list[dict[str, Any]],
    *,
    generated_at: datetime,
    filters: dict[str, Any],
) -> ReplyQuestionCoverageReport:
    total_questions = 0
    findings: list[ReplyQuestionCoverageFinding] = []
    for row in rows:
        is_question, finding = inspect_reply_question_coverage_row(row)
        if not is_question:
            continue
        total_questions += 1
        if finding is not None:
            findings.append(finding)

    findings.sort(key=lambda item: (item.detected_at or "", item.draft_id or 0, item.mention_id or ""))
    summary = {
        "total_questions": total_questions,
        "covered_count": total_questions - len(findings),
        "uncovered_count": len(findings),
    }
    return ReplyQuestionCoverageReport(
        ok=not findings,
        generated_at=generated_at.isoformat(),
        filters=filters,
        summary=summary,
        findings=tuple(findings),
    )


def _finding(
    row: Mapping[str, Any],
    question_text: str,
    reason: str,
) -> ReplyQuestionCoverageFinding:
    return ReplyQuestionCoverageFinding(
        mention_id=_str_or_none(row.get("inbound_tweet_id") or row.get("mention_id")),
        draft_id=_int_or_none(row.get("id") or row.get("draft_id") or row.get("reply_queue_id")),
        question_text=question_text,
        reason=reason,
        author_handle=_str_or_none(row.get("inbound_author_handle") or row.get("author_handle")),
        platform=str(row.get("platform") or "x"),
        status=_str_or_none(row.get("status")),
        draft_preview=_shorten(_normalized_text(row.get("draft_text")), 100),
        detected_at=_str_or_none(row.get("detected_at")),
    )


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    *,
    missing_tables: tuple[str, ...] = (),
    missing_columns: dict[str, tuple[str, ...]] | None = None,
) -> ReplyQuestionCoverageReport:
    return ReplyQuestionCoverageReport(
        ok=False if missing_tables or missing_columns else True,
        generated_at=generated_at.isoformat(),
        filters=filters,
        summary={"total_questions": 0, "covered_count": 0, "uncovered_count": 0},
        findings=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


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
        where.append("(detected_at IS NULL OR datetime(detected_at) <= datetime(?))")
        params.extend([cutoff.isoformat(), now.isoformat()])

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


def _connection(db: Any) -> sqlite3.Connection:
    return db.conn if hasattr(db, "conn") else db


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _is_records(value: Any) -> bool:
    return isinstance(value, Iterable) and not isinstance(value, (str, bytes, sqlite3.Connection))


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


def _normalized_text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


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
