"""Deterministic platform-fit linting for reply drafts."""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any


SEVERITY_INFO = "info"
SEVERITY_WARN = "warn"
SEVERITY_ERROR = "error"
SEVERITY_ORDER = (SEVERITY_INFO, SEVERITY_WARN, SEVERITY_ERROR)

DEFAULT_STATUS = "pending"

RULE_CHARACTER_BUDGET = "character_budget"
RULE_MISSING_DIRECT_ANSWER = "missing_direct_answer"
RULE_EXCESSIVE_HEDGING = "excessive_hedging"
RULE_DUPLICATE_GREETING = "duplicate_greeting"
RULE_TOO_MANY_LINKS = "too_many_links"
RULE_UNSUPPORTED_THREAD_FORMATTING = "unsupported_thread_formatting"

PLATFORM_CHARACTER_BUDGETS = {
    "x": 280,
    "twitter": 280,
    "bluesky": 300,
    "mastodon": 500,
    "linkedin": 1250,
}

PLATFORM_MAX_LINKS = {
    "x": 1,
    "twitter": 1,
    "bluesky": 1,
    "mastodon": 2,
    "linkedin": 2,
}

_URL_RE = re.compile(r"https?://\S+|www\.\S+", re.I)
_TOKEN_RE = re.compile(r"[a-z0-9']+")
_GREETING_RE = re.compile(r"^\s*(?:hi|hey|hello|thanks|thank you)\b[,\s!.-]*", re.I)
_THREAD_MARKER_RE = re.compile(
    r"(^|\n)\s*(?:\d+\s*/\s*\d*|\(\s*\d+\s*/\s*\d*\s*\)|thread:|continued:)",
    re.I,
)

_QUESTION_OPENERS = {
    "can",
    "could",
    "do",
    "does",
    "did",
    "how",
    "is",
    "are",
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
    "can you",
    "could you",
    "do you",
    "how do",
    "how should",
    "what should",
    "why does",
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
    "not sure",
    "would need more context",
)

_HEDGE_PHRASES = (
    "i think",
    "i guess",
    "maybe",
    "might",
    "probably",
    "perhaps",
    "sort of",
    "kind of",
    "could be",
    "seems like",
)

_DIRECT_ANSWER_PATTERNS = (
    re.compile(r"\b(?:yes|no)\b", re.I),
    re.compile(r"\b(?:try|use|set|run|check|look at|because|the fix|the issue|it is|it's)\b", re.I),
)


@dataclass(frozen=True)
class ReplyPlatformFitFinding:
    """One flagged reply draft with grouped platform-fit reasons."""

    reply_queue_id: int | None
    mention_id: str
    platform: str
    status: str
    severity: str
    reasons: list[str]
    suggested_action: str
    rule_ids: list[str]
    measured_length: int
    allowed_length: int | None
    author: str | None = None
    detected_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def lint_reply_platform_fit_row(row: dict[str, Any]) -> ReplyPlatformFitFinding | None:
    """Lint a reply_queue-like row and return a grouped finding when flagged."""

    draft = str(row.get("draft_text") or "")
    inbound = str(row.get("inbound_text") or "")
    platform = _normalize_platform(row.get("platform"))
    status = str(row.get("status") or DEFAULT_STATUS)
    measured_length = len(draft)
    allowed_length = PLATFORM_CHARACTER_BUDGETS.get(platform)

    rule_hits: list[tuple[str, str, str]] = []
    if allowed_length is not None and measured_length > allowed_length:
        rule_hits.append(
            (
                RULE_CHARACTER_BUDGET,
                SEVERITY_ERROR,
                f"draft length {measured_length} exceeds {platform} budget {allowed_length}",
            )
        )

    if _is_question_like(inbound, row) and _is_evasive_without_answer(draft):
        rule_hits.append(
            (
                RULE_MISSING_DIRECT_ANSWER,
                SEVERITY_ERROR,
                "question-like inbound text has an evasive draft without a direct answer",
            )
        )

    hedge_count = _hedge_count(draft)
    if hedge_count >= 3:
        rule_hits.append(
            (
                RULE_EXCESSIVE_HEDGING,
                SEVERITY_WARN,
                f"draft contains {hedge_count} hedging phrases",
            )
        )

    if _has_duplicate_greeting(draft):
        rule_hits.append(
            (
                RULE_DUPLICATE_GREETING,
                SEVERITY_WARN,
                "draft starts with repeated greeting language",
            )
        )

    link_count = len(_URL_RE.findall(draft))
    max_links = PLATFORM_MAX_LINKS.get(platform, 1)
    if link_count > max_links:
        rule_hits.append(
            (
                RULE_TOO_MANY_LINKS,
                SEVERITY_WARN,
                f"draft contains {link_count} links; {platform} allows {max_links}",
            )
        )

    if _has_thread_formatting(draft):
        rule_hits.append(
            (
                RULE_UNSUPPORTED_THREAD_FORMATTING,
                SEVERITY_WARN,
                "draft uses thread-style formatting that is unsupported for single replies",
            )
        )

    if not rule_hits:
        return None

    severity = max((hit[1] for hit in rule_hits), key=_severity_rank)
    return ReplyPlatformFitFinding(
        reply_queue_id=_int_or_none(row.get("id")),
        mention_id=str(row.get("inbound_tweet_id") or row.get("id") or ""),
        platform=platform,
        status=status,
        severity=severity,
        reasons=[hit[2] for hit in rule_hits],
        suggested_action=_suggested_action(rule_hits),
        rule_ids=[hit[0] for hit in rule_hits],
        measured_length=measured_length,
        allowed_length=allowed_length,
        author=row.get("inbound_author_handle"),
        detected_at=row.get("detected_at"),
    )


def lint_reply_platform_fit(
    db: Any,
    *,
    platform: str | None = None,
    status: str | None = DEFAULT_STATUS,
    min_severity: str = SEVERITY_WARN,
) -> list[ReplyPlatformFitFinding]:
    """Return platform-fit findings for reply_queue drafts."""

    if platform is not None and not platform.strip():
        raise ValueError("platform must not be blank")
    if status is not None and not status.strip():
        raise ValueError("status must not be blank")
    if min_severity not in SEVERITY_ORDER:
        raise ValueError("min_severity must be one of: info, warn, error")

    conn = _connection(db)
    columns = _table_columns(conn, "reply_queue")
    if not columns or "draft_text" not in columns:
        return []

    rows = _fetch_reply_rows(conn, columns, platform=platform, status=status)
    findings = []
    for row in rows:
        finding = lint_reply_platform_fit_row(row)
        if finding and _severity_rank(finding.severity) >= _severity_rank(min_severity):
            findings.append(finding)

    return sorted(
        findings,
        key=lambda item: (
            -_severity_rank(item.severity),
            item.platform,
            item.detected_at or "",
            item.reply_queue_id or 0,
            item.mention_id,
        ),
    )


def build_reply_platform_fit_report(
    db: Any,
    *,
    platform: str | None = None,
    status: str | None = DEFAULT_STATUS,
    min_severity: str = SEVERITY_WARN,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a deterministic report for reply draft platform fit."""

    generated_at = _as_utc(now or datetime.now(timezone.utc)).isoformat()
    findings = lint_reply_platform_fit(
        db,
        platform=platform,
        status=status,
        min_severity=min_severity,
    )
    counts = {
        "findings": len(findings),
        "warnings": sum(1 for finding in findings if finding.severity == SEVERITY_WARN),
        "errors": sum(1 for finding in findings if finding.severity == SEVERITY_ERROR),
        "by_rule": _count_by_rule(findings),
    }
    return {
        "artifact_type": "reply_platform_fit_lint",
        "generated_at": generated_at,
        "filters": {
            "platform": platform,
            "status": status,
            "min_severity": min_severity,
        },
        "counts": counts,
        "findings": [finding.to_dict() for finding in findings],
    }


def format_reply_platform_fit_json(report: dict[str, Any]) -> str:
    """Render a platform-fit report as deterministic JSON."""

    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_platform_fit_text(report: dict[str, Any]) -> str:
    """Render a stable human-readable platform-fit report."""

    if not report["findings"]:
        return "No reply platform fit findings."

    lines = [
        "Reply Platform Fit Lint",
        (
            f"Counts: findings={report['counts']['findings']} "
            f"warnings={report['counts']['warnings']} errors={report['counts']['errors']}"
        ),
        "",
        "Findings",
    ]
    for item in report["findings"]:
        author = f"@{item['author']}" if item.get("author") else "@unknown"
        allowed = item["allowed_length"] if item["allowed_length"] is not None else "unknown"
        lines.append(
            f"  - {item['severity']} reply={item['reply_queue_id']} "
            f"mention={item['mention_id']} {item['platform']} {author} "
            f"length={item['measured_length']}/{allowed} rules={','.join(item['rule_ids'])}"
        )
        lines.append(f"    reasons: {'; '.join(item['reasons'])}")
        lines.append(f"    suggested_action: {item['suggested_action']}")
    return "\n".join(lines)


def _fetch_reply_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    platform: str | None,
    status: str | None,
) -> list[dict[str, Any]]:
    filters = []
    params: list[Any] = []
    if platform is not None and "platform" in columns:
        filters.append("platform = ?")
        params.append(platform)
    if status is not None and "status" in columns:
        filters.append("COALESCE(status, ?) = ?")
        params.extend([DEFAULT_STATUS, status])

    query = "SELECT * FROM reply_queue"
    if filters:
        query += " WHERE " + " AND ".join(filters)
    query += " ORDER BY " + _order_clause(columns)
    return [dict(row) for row in conn.execute(query, params).fetchall()]


def _order_clause(columns: set[str]) -> str:
    parts = []
    if "platform" in columns:
        parts.append("platform ASC")
    if "detected_at" in columns:
        parts.append("datetime(detected_at) ASC")
    parts.append("id ASC" if "id" in columns else "rowid ASC")
    return ", ".join(parts)


def _is_question_like(inbound: str, row: dict[str, Any]) -> bool:
    normalized = _normalize_text(inbound)
    if "?" in inbound:
        return True
    intent = str(row.get("intent") or "").strip().lower()
    if intent in {"question", "bug_report"}:
        return True
    tokens = _TOKEN_RE.findall(normalized)
    if tokens and tokens[0] in _QUESTION_OPENERS:
        return True
    return any(phrase in normalized for phrase in _QUESTION_PHRASES)


def _is_evasive_without_answer(draft: str) -> bool:
    normalized = _normalize_text(draft)
    if not normalized:
        return True
    if any(pattern.search(draft) for pattern in _DIRECT_ANSWER_PATTERNS):
        return False
    return any(phrase in normalized for phrase in _EVASIVE_PHRASES)


def _hedge_count(draft: str) -> int:
    normalized = _normalize_text(draft)
    return sum(1 for phrase in _HEDGE_PHRASES if phrase in normalized)


def _has_duplicate_greeting(draft: str) -> bool:
    first = _GREETING_RE.match(draft)
    if not first:
        return False
    remainder = draft[first.end() :]
    second = _GREETING_RE.match(remainder)
    return second is not None


def _has_thread_formatting(draft: str) -> bool:
    if _THREAD_MARKER_RE.search(draft):
        return True
    normalized = _normalize_text(draft)
    return " thread " in f" {normalized} " and "\n" in draft


def _suggested_action(rule_hits: list[tuple[str, str, str]]) -> str:
    rule_ids = {hit[0] for hit in rule_hits}
    if RULE_CHARACTER_BUDGET in rule_ids:
        return "shorten the draft to fit the platform budget before review"
    if RULE_MISSING_DIRECT_ANSWER in rule_ids:
        return "rewrite the draft to answer the inbound question directly"
    if RULE_UNSUPPORTED_THREAD_FORMATTING in rule_ids:
        return "convert the draft into a single reply without thread markers"
    if RULE_TOO_MANY_LINKS in rule_ids:
        return "remove extra links or pick one canonical reference"
    if RULE_DUPLICATE_GREETING in rule_ids:
        return "remove the repeated greeting"
    return "tighten the wording before review"


def _count_by_rule(findings: list[ReplyPlatformFitFinding]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for finding in findings:
        for rule_id in finding.rule_ids:
            counts[rule_id] = counts.get(rule_id, 0) + 1
    return dict(sorted(counts.items()))


def _connection(db: Any) -> sqlite3.Connection:
    return db.conn if hasattr(db, "conn") else db


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _normalize_platform(value: Any) -> str:
    return str(value or "x").strip().lower() or "x"


def _normalize_text(text: str) -> str:
    text = _URL_RE.sub(" ", text.lower())
    text = re.sub(r"@\w+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _severity_rank(severity: str) -> int:
    try:
        return SEVERITY_ORDER.index(severity)
    except ValueError:
        return -1


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
