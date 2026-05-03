"""Report reply drafts that don't answer follow-up questions from the source message."""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Sequence


DEFAULT_DAYS = 7
DEFAULT_STATUS = ("pending",)
EXCERPT_CHARS = 160

WARNING_EMPTY_SOURCE = "empty_source_text"
WARNING_EMPTY_DRAFT = "empty_draft_text"
WARNING_MALFORMED_SOURCE = "malformed_source_text"

_QUESTION_SENTENCE_RE = re.compile(r"([^.!?\n\r]{0,260}\?+)")
_TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9'-]*")

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

_ANSWER_PATTERNS = (
    re.compile(r"\b(?:yes|no)\b", re.I),
    re.compile(
        r"\b(?:because|the answer is|the fix is|the issue is|it is|it's|"
        r"try|use|set|run|check|ship|add|remove|change|avoid|prefer)\b",
        re.I,
    ),
)


@dataclass(frozen=True)
class ReplyUnansweredFollowupFinding:
    """A reply draft that doesn't answer follow-up questions."""

    mention_id: str | None
    draft_id: int | None
    author_handle: str | None
    platform: str
    question_count: int
    answered_question_count: int
    unanswered_question_count: int
    drafted_at: str | None
    warnings: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ReplyUnansweredFollowupReport:
    """Aggregated unanswered follow-up report."""

    ok: bool
    generated_at: str
    filters: dict[str, Any]
    scanned_count: int
    unanswered_count: int
    findings: tuple[ReplyUnansweredFollowupFinding, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    @property
    def blocking_issue_count(self) -> int:
        return self.unanswered_count

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "reply_unanswered_followup_prompt",
            "ok": self.ok,
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "scanned_count": self.scanned_count,
            "unanswered_count": self.unanswered_count,
            "blocking_issue_count": self.blocking_issue_count,
            "findings": [finding.to_dict() for finding in self.findings],
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            }
            if self.missing_columns
            else {},
        }


def build_reply_unanswered_followup_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    status: str | Sequence[str] | None = DEFAULT_STATUS,
    platform: str | Sequence[str] | None = None,
    now: datetime | None = None,
) -> ReplyUnansweredFollowupReport:
    """Build a report of reply drafts that don't answer follow-up questions."""
    if days <= 0:
        raise ValueError("days must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    statuses = _normalize_filter(status)
    platforms = _normalize_filter(platform)

    filters = {
        "days": days,
        "status": list(statuses),
        "platform": list(platforms) if platforms else None,
    }

    conn = _connection(db_or_conn)
    schema = _schema(conn)

    if "reply_queue" not in schema:
        return _empty_report(generated_at, filters, missing_tables=("reply_queue",))

    required = {"id", "inbound_tweet_id", "inbound_text", "draft_text"}
    missing_required = tuple(sorted(required - schema["reply_queue"]))
    if missing_required:
        return _empty_report(
            generated_at,
            filters,
            missing_columns={"reply_queue": missing_required},
        )

    rows = _reply_rows(
        conn,
        schema,
        days=days,
        statuses=statuses,
        platforms=platforms,
        generated_at=generated_at,
    )

    all_findings = tuple(
        _analyze_followup_coverage(row) for row in rows
    )

    # Only include findings with unanswered questions
    findings = tuple(
        f for f in all_findings if f.unanswered_question_count > 0
    )

    return ReplyUnansweredFollowupReport(
        ok=len(findings) == 0,
        generated_at=generated_at.isoformat(),
        filters=filters,
        scanned_count=len(all_findings),
        unanswered_count=len(findings),
        findings=tuple(sorted(findings, key=_finding_sort_key)),
    )


def format_reply_unanswered_followup_json(report: ReplyUnansweredFollowupReport) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_reply_unanswered_followup_text(report: ReplyUnansweredFollowupReport) -> str:
    """Render human-readable text summary."""
    lines = [
        "Reply Unanswered Follow-up Prompt Report",
        f"Generated: {report.generated_at}",
        f"Filters: days={report.filters['days']}, status={report.filters['status']}",
        "",
        f"Scanned: {report.scanned_count}",
        f"Unanswered: {report.unanswered_count}",
        f"OK: {report.ok}",
    ]

    if report.missing_tables:
        lines.extend(["", f"Missing tables: {', '.join(report.missing_tables)}"])

    if report.missing_columns:
        lines.append("")
        lines.append("Missing columns:")
        for table, columns in sorted(report.missing_columns.items()):
            lines.append(f"  {table}: {', '.join(columns)}")

    if report.findings:
        lines.extend(["", "Findings:"])
        for finding in report.findings:
            warnings_str = f" [{', '.join(finding.warnings)}]" if finding.warnings else ""
            lines.append(
                f"  {finding.author_handle or 'unknown'} ({finding.mention_id}): "
                f"{finding.unanswered_question_count}/{finding.question_count} unanswered{warnings_str}"
            )

    return "\n".join(lines)


def _analyze_followup_coverage(row: dict[str, Any]) -> ReplyUnansweredFollowupFinding:
    """Analyze one reply draft for follow-up question coverage."""
    draft_id = _int_or_none(row.get("id"))
    mention_id = _clean(row.get("inbound_tweet_id") or row.get("mention_id"))
    author_handle = _clean(row.get("inbound_author_handle") or row.get("author_handle"))
    platform = _clean(row.get("platform")) or "x"
    drafted_at = _clean(row.get("detected_at") or row.get("created_at"))

    inbound_text = _clean(row.get("inbound_text"))
    draft_text = _clean(row.get("draft_text"))

    warnings: list[str] = []

    if not inbound_text:
        warnings.append(WARNING_EMPTY_SOURCE)

    if not draft_text:
        warnings.append(WARNING_EMPTY_DRAFT)

    if not inbound_text or not draft_text:
        return ReplyUnansweredFollowupFinding(
            mention_id=mention_id,
            draft_id=draft_id,
            author_handle=author_handle,
            platform=platform,
            question_count=0,
            answered_question_count=0,
            unanswered_question_count=0,
            drafted_at=drafted_at,
            warnings=tuple(warnings),
        )

    # Detect questions in source
    questions = _extract_questions(inbound_text)
    if not questions:
        return ReplyUnansweredFollowupFinding(
            mention_id=mention_id,
            draft_id=draft_id,
            author_handle=author_handle,
            platform=platform,
            question_count=0,
            answered_question_count=0,
            unanswered_question_count=0,
            drafted_at=drafted_at,
            warnings=tuple(warnings),
        )

    # Check answer coverage
    answered_count = sum(
        1 for q in questions if _draft_answers_question(q, draft_text)
    )
    unanswered_count = len(questions) - answered_count

    return ReplyUnansweredFollowupFinding(
        mention_id=mention_id,
        draft_id=draft_id,
        author_handle=author_handle,
        platform=platform,
        question_count=len(questions),
        answered_question_count=answered_count,
        unanswered_question_count=unanswered_count,
        drafted_at=drafted_at,
        warnings=tuple(warnings),
    )


def _extract_questions(text: str) -> list[str]:
    """Extract question sentences from text."""
    if not text:
        return []

    normalized = " ".join(text.split())
    sentences = _QUESTION_SENTENCE_RE.findall(normalized)

    questions = []
    for sentence in sentences:
        cleaned = sentence.strip()
        if not cleaned:
            continue

        # Check for question markers
        lower = cleaned.lower()
        tokens = _TOKEN_RE.findall(lower)

        if not tokens:
            continue

        # Question opener at start
        if tokens[0] in _QUESTION_OPENERS:
            questions.append(cleaned)
            continue

        # Question phrase anywhere
        if any(phrase in lower for phrase in _QUESTION_PHRASES):
            questions.append(cleaned)
            continue

    return questions


def _draft_answers_question(question: str, draft: str) -> bool:
    """Check if draft contains answer signals for the question."""
    if not question or not draft:
        return False

    draft_lower = draft.lower()

    # Check for answer patterns
    for pattern in _ANSWER_PATTERNS:
        if pattern.search(draft):
            return True

    # Check for keyword overlap (simple heuristic)
    question_tokens = set(_TOKEN_RE.findall(question.lower()))
    draft_tokens = set(_TOKEN_RE.findall(draft_lower))

    # Remove common question words
    question_tokens -= _QUESTION_OPENERS

    # Need meaningful overlap
    if not question_tokens:
        return False

    overlap = question_tokens & draft_tokens
    coverage = len(overlap) / len(question_tokens)

    # Require at least 30% keyword overlap as a signal
    return coverage >= 0.3


def _reply_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    days: int,
    statuses: tuple[str, ...],
    platforms: tuple[str, ...],
    generated_at: datetime,
) -> list[dict[str, Any]]:
    """Load reply queue rows matching filters."""
    columns = schema.get("reply_queue", set())
    where: list[str] = []
    params: list[Any] = []

    if "detected_at" in columns:
        from datetime import timedelta

        cutoff = generated_at - timedelta(days=days)
        where.append("(detected_at IS NULL OR datetime(detected_at) >= datetime(?))")
        params.append(cutoff.isoformat())

    if "status" in columns and statuses:
        where.append(f"LOWER(COALESCE(status, 'pending')) IN ({_placeholders(statuses)})")
        params.extend(statuses)

    if "platform" in columns and platforms:
        where.append(f"LOWER(COALESCE(platform, 'x')) IN ({_placeholders(platforms)})")
        params.extend(platforms)

    query = "SELECT * FROM reply_queue"
    if where:
        query += " WHERE " + " AND ".join(where)
    query += " ORDER BY " + _order_clause(columns)

    return [dict(row) for row in conn.execute(query, params).fetchall()]


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
    ).fetchall()
    return {str(row[0]): _table_columns(conn, str(row[0])) for row in tables}


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {
        str(row[1])
        for row in conn.execute(f"PRAGMA table_info({_quote_identifier(table)})").fetchall()
    }


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    *,
    missing_tables: Sequence[str] = (),
    missing_columns: dict[str, Sequence[str]] | None = None,
) -> ReplyUnansweredFollowupReport:
    return ReplyUnansweredFollowupReport(
        ok=True,
        generated_at=generated_at.isoformat(),
        filters=dict(filters),
        scanned_count=0,
        unanswered_count=0,
        findings=(),
        missing_tables=tuple(missing_tables),
        missing_columns={
            table: tuple(columns) for table, columns in (missing_columns or {}).items()
        }
        or None,
    )


def _normalize_filter(value: str | Sequence[str] | None) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        values = (value,)
    else:
        values = tuple(value)
    return tuple(sorted({item.strip().casefold() for item in values if item and item.strip()}))


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _finding_sort_key(finding: ReplyUnansweredFollowupFinding) -> tuple[Any, ...]:
    return (
        -finding.unanswered_question_count,
        -finding.question_count,
        finding.platform,
        finding.mention_id or "",
    )


def _order_clause(columns: set[str]) -> str:
    parts = []
    if "detected_at" in columns:
        parts.append("datetime(detected_at) DESC")
    if "id" in columns:
        parts.append("id DESC")
    else:
        parts.append("rowid DESC")
    return ", ".join(parts)


def _placeholders(values: Sequence[Any]) -> str:
    return ",".join("?" for _ in values)


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
