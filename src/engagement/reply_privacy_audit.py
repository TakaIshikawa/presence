"""Audit drafted replies for likely private data leaks."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any, Callable, Iterable


DEFAULT_DAYS = 7
DEFAULT_LIMIT = 50
DEFAULT_STATUS = "pending"


@dataclass(frozen=True)
class ReplyPrivacyFinding:
    """One likely privacy leak in a drafted reply."""

    reply_id: int
    detector: str
    severity: str
    evidence: str
    start: int
    end: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "reply_id": self.reply_id,
            "detector": self.detector,
            "severity": self.severity,
            "evidence": self.evidence,
            "start": self.start,
            "end": self.end,
        }


@dataclass(frozen=True)
class ReplyPrivacyAuditItem:
    """Audit result for one reply draft."""

    id: int
    reply_id: str
    status: str
    platform: str
    author: str
    detected_at: str
    draft_preview: str
    findings: tuple[ReplyPrivacyFinding, ...]

    @property
    def highest_severity(self) -> str:
        return _highest_severity(finding.severity for finding in self.findings)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "reply_id": self.reply_id,
            "status": self.status,
            "platform": self.platform,
            "author": self.author,
            "detected_at": self.detected_at,
            "draft_preview": self.draft_preview,
            "highest_severity": self.highest_severity,
            "findings": [finding.to_dict() for finding in self.findings],
        }


@dataclass(frozen=True)
class ReplyPrivacyAuditReport:
    """Aggregated privacy audit report for drafted replies."""

    ok: bool
    generated_at: str
    filters: dict[str, Any]
    audited_count: int
    finding_count: int
    by_detector: dict[str, int]
    by_severity: dict[str, int]
    items: tuple[ReplyPrivacyAuditItem, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    @property
    def blocking_issue_count(self) -> int:
        return self.finding_count

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "reply_privacy_audit",
            "ok": self.ok,
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "audited_count": self.audited_count,
            "finding_count": self.finding_count,
            "blocking_issue_count": self.blocking_issue_count,
            "by_detector": dict(sorted(self.by_detector.items())),
            "by_severity": dict(sorted(self.by_severity.items())),
            "items": [item.to_dict() for item in self.items],
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
        }


@dataclass(frozen=True)
class _Detector:
    name: str
    severity: str
    pattern: re.Pattern[str]
    mask: Callable[[str], str]


EMAIL_RE = re.compile(r"(?<![\w.+-])[\w.+-]{1,64}@(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,63}\b")
PHONE_RE = re.compile(
    r"(?<!\w)(?:\+?1[\s.-]?)?(?:\(?\d{3}\)?[\s.-]?)\d{3}[\s.-]?\d{4}(?!\w)"
)
LOCAL_PATH_RE = re.compile(
    r"(?<!\w)(?:file://)?(?:~|/(?:Users|home|private|var/folders|tmp|etc)|[A-Za-z]:\\)"
    r"(?:[^\s`'\"<>)]{2,})"
)
INTERNAL_PATH_RE = re.compile(
    r"(?<![\w./-])(?:src|tests|scripts|config|secrets|credentials|\.env)"
    r"/[A-Za-z0-9._/-]{3,}"
)
PRIVATE_ID_RE = re.compile(
    r"(?i)\b(?:account|customer|user|member|session|conversation|thread|ticket|tenant)"
    r"[_ -]?id\s*[:=]\s*['\"]?[A-Za-z0-9][A-Za-z0-9_-]{7,}\b"
)
SECRET_TOKEN_RE = re.compile(
    r"(?i)\b(?:"
    r"sk-proj-[A-Za-z0-9_-]{16,}|"
    r"sk-[A-Za-z0-9_-]{16,}|"
    r"gh[pousr]_[A-Za-z0-9_]{20,}|"
    r"xox[baprs]-[A-Za-z0-9-]{20,}|"
    r"AKIA[0-9A-Z]{16}|"
    r"eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}|"
    r"(?:api[_-]?key|access[_-]?token|auth[_-]?token|bearer|secret|password)"
    r"\s*[:=]\s*['\"]?[A-Za-z0-9_./+=-]{12,}"
    r")\b"
)

DETECTORS: tuple[_Detector, ...] = (
    _Detector("secret_token", "critical", SECRET_TOKEN_RE, lambda value: _mask_token(value)),
    _Detector("email", "high", EMAIL_RE, lambda value: _mask_email(value)),
    _Detector("phone", "medium", PHONE_RE, lambda value: _mask_phone(value)),
    _Detector("local_path", "medium", LOCAL_PATH_RE, lambda value: _mask_path(value)),
    _Detector("internal_path", "medium", INTERNAL_PATH_RE, lambda value: _mask_path(value)),
    _Detector("private_identifier", "medium", PRIVATE_ID_RE, lambda value: _mask_labeled_value(value)),
)
SEVERITY_RANK = {"critical": 0, "high": 1, "medium": 2, "low": 3, "none": 4}


def audit_reply_privacy_text(text: str, *, reply_id: int = 0) -> tuple[ReplyPrivacyFinding, ...]:
    """Return masked privacy findings for one reply draft."""
    findings: list[ReplyPrivacyFinding] = []
    occupied: list[tuple[int, int]] = []
    for detector in DETECTORS:
        for match in detector.pattern.finditer(text or ""):
            start, end, value = _trimmed_match(match)
            if not value or _overlaps(start, end, occupied):
                continue
            findings.append(
                ReplyPrivacyFinding(
                    reply_id=reply_id,
                    detector=detector.name,
                    severity=detector.severity,
                    evidence=detector.mask(value),
                    start=start,
                    end=end,
                )
            )
            occupied.append((start, end))
    return tuple(sorted(findings, key=lambda item: (item.start, item.detector)))


def audit_reply_privacy(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    status: str = DEFAULT_STATUS,
    limit: int | None = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ReplyPrivacyAuditReport:
    """Audit stored reply drafts matching status/date filters."""
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
    items: list[ReplyPrivacyAuditItem] = []
    all_findings: list[ReplyPrivacyFinding] = []
    for row in rows:
        item = _audit_row(row, columns)
        items.append(item)
        all_findings.extend(item.findings)

    return ReplyPrivacyAuditReport(
        ok=not all_findings,
        generated_at=now.isoformat(),
        filters=filters,
        audited_count=len(items),
        finding_count=len(all_findings),
        by_detector=dict(Counter(finding.detector for finding in all_findings)),
        by_severity=dict(Counter(finding.severity for finding in all_findings)),
        items=tuple(items),
    )


def format_reply_privacy_audit_json(report: ReplyPrivacyAuditReport) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_reply_privacy_audit_text(report: ReplyPrivacyAuditReport) -> str:
    """Render a compact human-readable privacy audit report."""
    lines = [
        "Reply Privacy Audit",
        (
            "Filters: "
            f"status={report.filters.get('status')} days={report.filters.get('days')} "
            f"limit={report.filters.get('limit')}"
        ),
        f"Audited: {report.audited_count}",
        f"Findings: {report.finding_count}",
    ]
    if report.by_detector:
        lines.append(
            "By detector: "
            + ", ".join(f"{key}={value}" for key, value in sorted(report.by_detector.items()))
        )
    if report.by_severity:
        lines.append(
            "By severity: "
            + ", ".join(f"{key}={value}" for key, value in sorted(report.by_severity.items()))
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
        lines.append("No reply drafts matched.")
        return "\n".join(lines)

    flagged = [item for item in report.items if item.findings]
    if not flagged:
        lines.append("No privacy findings.")
        return "\n".join(lines)

    lines.append("")
    for item in flagged:
        lines.append(
            f"#{item.id} {item.status} {item.platform} @{item.author or 'unknown'} "
            f"{item.highest_severity} findings={len(item.findings)}"
        )
        for finding in item.findings:
            lines.append(f"  {finding.severity} {finding.detector}: {finding.evidence}")
    return "\n".join(lines)


def _audit_row(row: dict[str, Any], columns: set[str]) -> ReplyPrivacyAuditItem:
    row_id = int(row.get("id") or 0)
    draft_text = str(row.get("draft_text") or "")
    findings = audit_reply_privacy_text(draft_text, reply_id=row_id)
    return ReplyPrivacyAuditItem(
        id=row_id,
        reply_id=str(row.get("inbound_tweet_id") or row_id),
        status=str(row.get("status") if "status" in columns else "pending"),
        platform=str(row.get("platform") if "platform" in columns else "x"),
        author=str(row.get("inbound_author_handle") if "inbound_author_handle" in columns else ""),
        detected_at=str(row.get("detected_at") if "detected_at" in columns else ""),
        draft_preview=_shorten(draft_text, 96),
        findings=findings,
    )


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
    return [dict(row) for row in conn.execute(query, params).fetchall()]


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
) -> ReplyPrivacyAuditReport:
    return ReplyPrivacyAuditReport(
        ok=True,
        generated_at=now.isoformat(),
        filters=filters,
        audited_count=0,
        finding_count=0,
        by_detector={},
        by_severity={},
        items=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _highest_severity(severities: Iterable[str]) -> str:
    return min(severities, key=lambda item: SEVERITY_RANK.get(item, 99), default="none")


def _overlaps(start: int, end: int, ranges: list[tuple[int, int]]) -> bool:
    return any(start < range_end and end > range_start for range_start, range_end in ranges)


def _trimmed_match(match: re.Match[str]) -> tuple[int, int, str]:
    value = match.group(0)
    start = match.start()
    end = match.end()
    while value and value[-1] in ".,;:":
        value = value[:-1]
        end -= 1
    return start, end, value


def _mask_email(value: str) -> str:
    local, _, domain = value.partition("@")
    return f"{_mask_keep_edges(local, 1, 1)}@{domain}"


def _mask_phone(value: str) -> str:
    digits = re.sub(r"\D", "", value)
    return f"***-***-{digits[-4:]}" if len(digits) >= 4 else "***"


def _mask_path(value: str) -> str:
    separator = "\\" if "\\" in value and "/" not in value else "/"
    parts = value.split(separator)
    tail = parts[-1] if parts else ""
    prefix = parts[0] if parts and parts[0] else separator
    return f"{prefix}{separator}...{separator}{tail}" if tail else f"{prefix}{separator}..."


def _mask_token(value: str) -> str:
    if "=" in value or ":" in value:
        separator = "=" if "=" in value else ":"
        label, secret = value.split(separator, 1)
        cleaned = secret.strip().strip("\"'")
        return f"{label}{separator}{_mask_keep_edges(cleaned, 4, 4)}"
    return _mask_keep_edges(value, 6, 4)


def _mask_labeled_value(value: str) -> str:
    match = re.match(r"(?is)(.+?[:=]\s*['\"]?)([A-Za-z0-9][A-Za-z0-9_-]+)\b", value)
    if not match:
        return _mask_keep_edges(value, 4, 4)
    return f"{match.group(1)}{_mask_keep_edges(match.group(2), 3, 3)}"


def _mask_keep_edges(value: str, prefix: int, suffix: int) -> str:
    if len(value) <= prefix + suffix:
        return "*" * len(value)
    return f"{value[:prefix]}...{value[-suffix:]}"


def _shorten(value: str, max_chars: int) -> str:
    compact = " ".join((value or "").split())
    if len(compact) <= max_chars:
        return compact
    return compact[: max_chars - 1].rstrip() + "..."
