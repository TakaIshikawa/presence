"""Audit reply drafts for factual claims without visible support."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any, Iterable, Mapping


DEFAULT_DAYS = 7
DEFAULT_LIMIT = 50
DEFAULT_STATUS = "pending"

EVIDENCE_NONE = "none"
EVIDENCE_DRAFT_SOURCE_LINK = "draft_source_link"
EVIDENCE_NEARBY_MARKER = "nearby_evidence_marker"
EVIDENCE_QUOTED_CONTEXT = "quoted_context"
EVIDENCE_RELATIONSHIP_CONTEXT = "relationship_context"
EVIDENCE_KNOWLEDGE_LINK = "knowledge_link"

SEVERITY_RANK = {"high": 0, "medium": 1, "low": 2, "none": 3}
MAX_SNIPPETS = 3

URL_RE = re.compile(r"https?://[^\s)>\"]+", re.IGNORECASE)
SENTENCE_RE = re.compile(r"[^.!?\n]+(?:[.!?]+|$)")
WORD_RE = re.compile(r"[a-z0-9][a-z0-9_-]*", re.IGNORECASE)

NEARBY_EVIDENCE_RE = re.compile(
    r"\b("
    r"according to|based on|cited in|from the docs|in the docs|the source says|"
    r"you said|you mentioned|you wrote|your post says|in your thread|in this thread|"
    r"as noted|as shared|per the link|per your note"
    r")\b",
    re.IGNORECASE,
)

CLAIM_INDICATORS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "numeric_claim",
        re.compile(
            r"\b\d+(?:\.\d+)?\s*(?:%|percent|x|times|days?|weeks?|months?|years?)(?=\W|$)",
            re.IGNORECASE,
        ),
    ),
    (
        "absolute_claim",
        re.compile(r"\b(always|never|only|must|cannot|can't|will|won't|guarantees?)\b", re.IGNORECASE),
    ),
    (
        "causal_claim",
        re.compile(
            r"\b(because|therefore|means|proves|shows|causes|leads to|results in|"
            r"reduces?|increases?|improves?|prevents?)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "comparative_claim",
        re.compile(r"\b(best|worst|faster|slower|cheaper|safer|more reliable|less reliable)\b", re.IGNORECASE),
    ),
    (
        "state_claim",
        re.compile(
            r"\b(is|are|was|were|does|doesn't|has|have|requires?|depends on)\b"
            r".*\b(metric|benchmark|production|release|latency|reliability|security|"
            r"incident|customer|workflow|architecture|model|data|test|deploy)\b",
            re.IGNORECASE,
        ),
    ),
)

HARMLESS_OPINION_RE = re.compile(
    r"\b(i think|i'd|i would|personally|to me|my read|it feels|seems like|might|may|could|"
    r"maybe|probably|usually|often|one way|a way|worth considering)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ReplyUnsupportedClaimFinding:
    """One unsupported claim-like snippet in a drafted reply."""

    reply_id: int
    severity: str
    claim_snippet: str
    reason: str
    evidence_status: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "reply_id": self.reply_id,
            "severity": self.severity,
            "claim_snippet": self.claim_snippet,
            "reason": self.reason,
            "evidence_status": self.evidence_status,
        }


@dataclass(frozen=True)
class ReplyUnsupportedClaimItem:
    """Audit result for one reply draft with unsupported factual claims."""

    id: int
    reply_id: str
    status: str
    platform: str
    author: str
    detected_at: str
    severity: str
    evidence_status: str
    reason: str
    claim_snippets: tuple[str, ...]
    draft_preview: str
    findings: tuple[ReplyUnsupportedClaimFinding, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "reply_id": self.reply_id,
            "status": self.status,
            "platform": self.platform,
            "author": self.author,
            "detected_at": self.detected_at,
            "severity": self.severity,
            "evidence_status": self.evidence_status,
            "reason": self.reason,
            "claim_snippets": list(self.claim_snippets),
            "draft_preview": self.draft_preview,
            "findings": [finding.to_dict() for finding in self.findings],
        }


@dataclass(frozen=True)
class ReplyUnsupportedClaimReport:
    """Aggregated unsupported-claim audit report."""

    ok: bool
    generated_at: str
    filters: dict[str, Any]
    audited_count: int
    finding_count: int
    by_severity: dict[str, int]
    by_reason: dict[str, int]
    items: tuple[ReplyUnsupportedClaimItem, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    @property
    def blocking_issue_count(self) -> int:
        return self.finding_count

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "reply_unsupported_claims",
            "ok": self.ok,
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "audited_count": self.audited_count,
            "finding_count": self.finding_count,
            "blocking_issue_count": self.blocking_issue_count,
            "by_severity": dict(sorted(self.by_severity.items())),
            "by_reason": dict(sorted(self.by_reason.items())),
            "items": [item.to_dict() for item in self.items],
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
        }


def inspect_reply_unsupported_claims(
    row: Mapping[str, Any],
    *,
    knowledge_link_count: int = 0,
) -> ReplyUnsupportedClaimItem | None:
    """Return an unsupported-claim item for one reply row, or None when supported."""
    record = dict(row)
    reply_id = _int_or_zero(record.get("id"))
    draft = str(record.get("draft_text") or "")
    snippets = _claim_snippets(draft)
    if not snippets:
        return None

    evidence_status = _evidence_status(record, knowledge_link_count=knowledge_link_count)
    if evidence_status != EVIDENCE_NONE:
        return None

    severity = _severity(snippets)
    reason = _reason(snippets)
    findings = tuple(
        ReplyUnsupportedClaimFinding(
            reply_id=reply_id,
            severity=severity,
            claim_snippet=snippet,
            reason=reason,
            evidence_status=evidence_status,
        )
        for snippet in snippets
    )
    return ReplyUnsupportedClaimItem(
        id=reply_id,
        reply_id=str(record.get("inbound_tweet_id") or reply_id),
        status=str(record.get("status") or DEFAULT_STATUS),
        platform=str(record.get("platform") or "x"),
        author=str(record.get("inbound_author_handle") or ""),
        detected_at=str(record.get("detected_at") or ""),
        severity=severity,
        evidence_status=evidence_status,
        reason=reason,
        claim_snippets=snippets,
        draft_preview=_shorten(draft, 120),
        findings=findings,
    )


def build_reply_unsupported_claims_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    status: str = DEFAULT_STATUS,
    limit: int | None = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ReplyUnsupportedClaimReport:
    """Audit stored reply drafts for factual claims without supporting evidence."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit is not None and limit < 0:
        raise ValueError("limit must be non-negative")
    if not status:
        raise ValueError("status is required")

    conn = _connection(db_or_conn)
    now = _as_utc(now or datetime.now(timezone.utc))
    filters = {"days": days, "status": status, "limit": limit}
    columns = _table_columns(conn, "reply_queue")
    if not columns:
        return _empty_report(now, filters, missing_tables=("reply_queue",))
    if "draft_text" not in columns:
        return _empty_report(now, filters, missing_columns={"reply_queue": ("draft_text",)})

    rows = _reply_rows(conn, columns, days=days, status=status, limit=limit, now=now)
    link_counts = _knowledge_link_counts(conn, rows)
    items = tuple(
        item
        for row in rows
        if (
            item := inspect_reply_unsupported_claims(
                row,
                knowledge_link_count=link_counts.get(_int_or_zero(row.get("id")), 0),
            )
        )
    )
    findings = [finding for item in items for finding in item.findings]
    return ReplyUnsupportedClaimReport(
        ok=not findings,
        generated_at=now.isoformat(),
        filters=filters,
        audited_count=len(rows),
        finding_count=len(findings),
        by_severity=dict(Counter(finding.severity for finding in findings)),
        by_reason=dict(Counter(finding.reason for finding in findings)),
        items=tuple(sorted(items, key=_item_sort_key)),
    )


def format_reply_unsupported_claims_json(report: ReplyUnsupportedClaimReport) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_reply_unsupported_claims_text(report: ReplyUnsupportedClaimReport) -> str:
    """Render a compact human-readable unsupported-claim report."""
    lines = [
        "Reply Unsupported Claims Audit",
        (
            "Filters: "
            f"status={report.filters.get('status')} days={report.filters.get('days')} "
            f"limit={report.filters.get('limit')}"
        ),
        f"Audited: {report.audited_count}",
        f"Findings: {report.finding_count}",
    ]
    if report.by_severity:
        lines.append(
            "By severity: "
            + ", ".join(f"{key}={value}" for key, value in sorted(report.by_severity.items()))
        )
    if report.by_reason:
        lines.append(
            "By reason: "
            + ", ".join(f"{key}={value}" for key, value in sorted(report.by_reason.items()))
        )
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        lines.append(
            "Missing columns: "
            + ", ".join(
                f"{table}.{column}"
                for table, columns in sorted(report.missing_columns.items())
                for column in columns
            )
        )
    if not report.items:
        lines.append("No unsupported factual claims found.")
        return "\n".join(lines)

    lines.append("")
    for item in report.items:
        lines.append(
            f"#{item.id} {item.status} {item.platform} @{item.author or 'unknown'} "
            f"{item.severity} claims={len(item.claim_snippets)} evidence={item.evidence_status}"
        )
        lines.append(f"  reason: {item.reason}")
        for snippet in item.claim_snippets:
            lines.append(f"  claim: {snippet}")
    return "\n".join(lines)


def _claim_snippets(text: str) -> tuple[str, ...]:
    snippets: list[str] = []
    for sentence in _sentences(text):
        if URL_RE.search(sentence) or NEARBY_EVIDENCE_RE.search(sentence):
            continue
        indicators = [name for name, pattern in CLAIM_INDICATORS if pattern.search(sentence)]
        if not indicators:
            continue
        if _is_harmless_opinion(sentence, indicators):
            continue
        snippets.append(_shorten(sentence, 180))
        if len(snippets) >= MAX_SNIPPETS:
            break
    return tuple(snippets)


def _sentences(text: str) -> list[str]:
    return [
        " ".join(match.group(0).split()).strip()
        for match in SENTENCE_RE.finditer(text or "")
        if match.group(0).strip()
    ]


def _is_harmless_opinion(sentence: str, indicators: list[str]) -> bool:
    if not HARMLESS_OPINION_RE.search(sentence):
        return False
    return "numeric_claim" not in indicators and "absolute_claim" not in indicators


def _evidence_status(record: Mapping[str, Any], *, knowledge_link_count: int) -> str:
    draft = str(record.get("draft_text") or "")
    if URL_RE.search(draft):
        return EVIDENCE_DRAFT_SOURCE_LINK
    if NEARBY_EVIDENCE_RE.search(draft):
        return EVIDENCE_NEARBY_MARKER
    if _has_quoted_context(record.get("platform_metadata")):
        return EVIDENCE_QUOTED_CONTEXT
    if _has_relationship_evidence(record.get("relationship_context")):
        return EVIDENCE_RELATIONSHIP_CONTEXT
    if knowledge_link_count > 0:
        return EVIDENCE_KNOWLEDGE_LINK
    return EVIDENCE_NONE


def _has_quoted_context(value: Any) -> bool:
    metadata = _json_object(value)
    if not metadata:
        return False
    quoted = metadata.get("quoted_text") or metadata.get("quote_text") or metadata.get("quoted_post_text")
    return isinstance(quoted, str) and bool(quoted.strip())


def _has_relationship_evidence(value: Any) -> bool:
    context = _json_object(value)
    if not context:
        return False
    evidence_keys = {
        "notes",
        "relationship_notes",
        "profile_summary",
        "summary",
        "last_interaction",
        "last_interaction_at",
        "history",
        "highlights",
        "prior_conversation",
    }
    for key in evidence_keys:
        if _has_meaningful_value(context.get(key)):
            return True
    return False


def _has_meaningful_value(value: Any) -> bool:
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (int, float, bool)):
        return True
    if isinstance(value, list):
        return any(_has_meaningful_value(item) for item in value)
    if isinstance(value, dict):
        return any(_has_meaningful_value(item) for item in value.values())
    return False


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _severity(snippets: tuple[str, ...]) -> str:
    joined = " ".join(snippets)
    if len(snippets) >= 2 or re.search(r"\b(always|never|only|must|cannot|can't)\b", joined, re.IGNORECASE):
        return "high"
    if re.search(r"\b\d+(?:\.\d+)?\s*(?:%|percent|x|times)(?=\W|$)", joined, re.IGNORECASE):
        return "high"
    return "medium"


def _reason(snippets: tuple[str, ...]) -> str:
    text = " ".join(snippets)
    if re.search(
        r"\b\d+(?:\.\d+)?\s*(?:%|percent|x|times|days?|weeks?|months?|years?)(?=\W|$)",
        text,
        re.IGNORECASE,
    ):
        return "unsupported_numeric_claim"
    if re.search(r"\b(always|never|only|must|cannot|can't|guarantees?)\b", text, re.IGNORECASE):
        return "unsupported_absolute_claim"
    if len(snippets) >= 2:
        return "multiple_unsupported_claims"
    return "unsupported_factual_claim"


def _reply_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    days: int,
    status: str,
    limit: int | None,
    now: datetime,
) -> list[dict[str, Any]]:
    where = ["draft_text IS NOT NULL", "TRIM(draft_text) != ''"]
    params: list[Any] = []
    if "status" in columns and status != "all":
        where.append("COALESCE(status, 'pending') = ?")
        params.append(status)
    if "detected_at" in columns:
        cutoff = now - timedelta(days=days)
        where.append("(detected_at IS NULL OR datetime(detected_at) >= datetime(?))")
        params.append(cutoff.isoformat())

    query = "SELECT * FROM reply_queue"
    if where:
        query += " WHERE " + " AND ".join(where)
    query += " ORDER BY " + _order_clause(columns)
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)
    cursor = conn.execute(query, params)
    names = [description[0] for description in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _knowledge_link_counts(conn: sqlite3.Connection, rows: Iterable[Mapping[str, Any]]) -> dict[int, int]:
    reply_ids = [_int_or_zero(row.get("id")) for row in rows if _int_or_zero(row.get("id"))]
    if not reply_ids or not _table_columns(conn, "reply_knowledge_links"):
        return {}
    placeholders = ", ".join("?" for _ in reply_ids)
    cursor = conn.execute(
        f"""SELECT reply_queue_id, COUNT(*)
            FROM reply_knowledge_links
            WHERE reply_queue_id IN ({placeholders})
            GROUP BY reply_queue_id""",
        reply_ids,
    )
    return {int(row[0]): int(row[1]) for row in cursor.fetchall()}


def _order_clause(columns: set[str]) -> str:
    parts = []
    if "detected_at" in columns:
        parts.append("datetime(detected_at) DESC")
    parts.append("id ASC" if "id" in columns else "rowid ASC")
    return ", ".join(parts)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _empty_report(
    now: datetime,
    filters: dict[str, Any],
    *,
    missing_tables: tuple[str, ...] = (),
    missing_columns: dict[str, tuple[str, ...]] | None = None,
) -> ReplyUnsupportedClaimReport:
    return ReplyUnsupportedClaimReport(
        ok=True,
        generated_at=now.isoformat(),
        filters=filters,
        audited_count=0,
        finding_count=0,
        by_severity={},
        by_reason={},
        items=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _item_sort_key(item: ReplyUnsupportedClaimItem) -> tuple[int, int]:
    return (SEVERITY_RANK.get(item.severity, 99), item.id)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _int_or_zero(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _shorten(value: str, max_chars: int) -> str:
    compact = " ".join((value or "").split())
    if len(compact) <= max_chars:
        return compact
    return compact[: max_chars - 1].rstrip() + "..."
