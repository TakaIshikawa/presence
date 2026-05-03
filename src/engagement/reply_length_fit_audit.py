"""Audit reply drafts against platform length budgets."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
DEFAULT_NEAR_THRESHOLD = 0.9
DEFAULT_PLATFORM_LIMITS = {
    "x": 280,
    "twitter": 280,
    "bluesky": 300,
}

ISSUE_OVER_LIMIT = "over_limit"
ISSUE_NEAR_LIMIT = "near_limit"
ISSUE_EMPTY_DRAFT = "empty_draft"
ISSUE_MISSING_DRAFT = "missing_draft"

SEVERITY_ERROR = "error"
SEVERITY_WARN = "warn"

_ISSUE_RANK = {
    ISSUE_OVER_LIMIT: 0,
    ISSUE_MISSING_DRAFT: 1,
    ISSUE_EMPTY_DRAFT: 2,
    ISSUE_NEAR_LIMIT: 3,
}
_SPACE_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class ReplyLengthFitFinding:
    """One reply_queue row whose draft cannot be reviewed as ready to post."""

    reply_queue_id: int | None
    inbound_id: str | None
    platform: str
    issue_type: str
    severity: str
    measured_length: int
    allowed_length: int
    near_threshold: float
    status: str | None = None
    author_handle: str | None = None
    inbound_text_present: bool = False
    draft_preview: str = ""
    detected_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ReplyLengthFitAuditReport:
    """Aggregate reply length fit audit report."""

    ok: bool
    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    findings: tuple[ReplyLengthFitFinding, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    @property
    def blocking_issue_count(self) -> int:
        return (
            self.totals["over_limit_count"]
            + self.totals["missing_draft_count"]
            + self.totals["empty_draft_count"]
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "reply_length_fit_audit",
            "ok": self.ok,
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "totals": dict(self.totals),
            "blocking_issue_count": self.blocking_issue_count,
            "findings": [finding.to_dict() for finding in self.findings],
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
        }


def build_reply_length_fit_audit(
    source: Any | None = None,
    *,
    reply_records: Iterable[Mapping[str, Any]] | None = None,
    platform_limits: Mapping[str, int] | None = None,
    near_threshold: float = DEFAULT_NEAR_THRESHOLD,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ReplyLengthFitAuditReport:
    """Build a deterministic length-fit audit from rows or a reply_queue database."""

    if reply_records is not None and source is not None:
        raise ValueError("provide either source or reply_records, not both")
    if limit <= 0:
        raise ValueError("limit must be positive")
    if not 0 < near_threshold <= 1:
        raise ValueError("near_threshold must be greater than 0 and at most 1")

    limits = _normalize_platform_limits(platform_limits)
    generated_at = _as_utc(now or datetime.now(timezone.utc))
    filters = {
        "limit": limit,
        "near_threshold": near_threshold,
        "platform_limits": dict(sorted(limits.items())),
    }

    if reply_records is not None or _is_records(source):
        input_records = reply_records if reply_records is not None else source
        rows = [dict(row) for row in list(input_records)[:limit]]
        return _report_from_rows(
            rows,
            generated_at=generated_at,
            filters=filters,
            limits=limits,
            near_threshold=near_threshold,
        )

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

    rows = _fetch_reply_rows(conn, columns, limit=limit)
    return _report_from_rows(
        rows,
        generated_at=generated_at,
        filters=filters,
        limits=limits,
        near_threshold=near_threshold,
    )


def inspect_reply_length_fit_row(
    row: Mapping[str, Any],
    *,
    platform_limits: Mapping[str, int] | None = None,
    near_threshold: float = DEFAULT_NEAR_THRESHOLD,
) -> ReplyLengthFitFinding | None:
    """Inspect one reply_queue-like row for length readiness."""

    if not 0 < near_threshold <= 1:
        raise ValueError("near_threshold must be greater than 0 and at most 1")

    limits = _normalize_platform_limits(platform_limits)
    platform = _normalize_platform(row.get("platform"))
    allowed = limits.get(platform, DEFAULT_PLATFORM_LIMITS["x"])
    draft_value = row.get("draft_text")
    inbound_text = _normalized_text(row.get("inbound_text"))
    inbound_present = bool(inbound_text)

    if draft_value is None:
        if not inbound_present:
            return None
        return _finding(row, platform, ISSUE_MISSING_DRAFT, 0, allowed, near_threshold, "")

    draft = _normalized_text(draft_value)
    measured = len(draft)
    if measured == 0:
        return _finding(row, platform, ISSUE_EMPTY_DRAFT, measured, allowed, near_threshold, draft)
    if measured > allowed:
        return _finding(row, platform, ISSUE_OVER_LIMIT, measured, allowed, near_threshold, draft)
    if measured >= int(allowed * near_threshold):
        return _finding(row, platform, ISSUE_NEAR_LIMIT, measured, allowed, near_threshold, draft)
    return None


def format_reply_length_fit_audit_json(report: ReplyLengthFitAuditReport) -> str:
    """Render deterministic JSON for automation."""

    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_reply_length_fit_audit_text(report: ReplyLengthFitAuditReport) -> str:
    """Render a compact human-readable reply length audit."""

    totals = report.totals
    lines = [
        "Reply Length Fit Audit",
        f"Generated: {report.generated_at}",
        (
            "Totals: "
            f"checked={totals['checked_replies']} "
            f"over_limit={totals['over_limit_count']} "
            f"near_limit={totals['near_limit_count']} "
            f"missing_draft={totals['missing_draft_count']} "
            f"empty={totals['empty_draft_count']}"
        ),
        (
            "Limits: "
            + ", ".join(
                f"{platform}={limit}"
                for platform, limit in report.filters["platform_limits"].items()
            )
            + f" near_threshold={report.filters['near_threshold']:g}"
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
        lines.extend(["", "No reply length fit issues found."])
        return "\n".join(lines)

    lines.extend(["", "Findings:"])
    for finding in report.findings:
        author = f"@{finding.author_handle}" if finding.author_handle else "@unknown"
        lines.append(
            f"- {finding.severity} reply={finding.reply_queue_id or '-'} "
            f"inbound={finding.inbound_id or '-'} platform={finding.platform} {author} "
            f"issue={finding.issue_type} length={finding.measured_length}/{finding.allowed_length}"
        )
        if finding.draft_preview:
            lines.append(f"  draft={finding.draft_preview!r}")
    return "\n".join(lines)


def _report_from_rows(
    rows: list[dict[str, Any]],
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    limits: dict[str, int],
    near_threshold: float,
) -> ReplyLengthFitAuditReport:
    findings = [
        finding
        for row in rows
        if (
            finding := inspect_reply_length_fit_row(
                row,
                platform_limits=limits,
                near_threshold=near_threshold,
            )
        )
        is not None
    ]
    findings.sort(key=_finding_sort_key)
    totals = {
        "checked_replies": len(rows),
        "over_limit_count": sum(1 for item in findings if item.issue_type == ISSUE_OVER_LIMIT),
        "near_limit_count": sum(1 for item in findings if item.issue_type == ISSUE_NEAR_LIMIT),
        "missing_draft_count": sum(1 for item in findings if item.issue_type == ISSUE_MISSING_DRAFT),
        "empty_draft_count": sum(1 for item in findings if item.issue_type == ISSUE_EMPTY_DRAFT),
    }
    return ReplyLengthFitAuditReport(
        ok=not findings,
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals=totals,
        findings=tuple(findings),
    )


def _finding(
    row: Mapping[str, Any],
    platform: str,
    issue_type: str,
    measured: int,
    allowed: int,
    near_threshold: float,
    draft: str,
) -> ReplyLengthFitFinding:
    severity = SEVERITY_WARN if issue_type == ISSUE_NEAR_LIMIT else SEVERITY_ERROR
    return ReplyLengthFitFinding(
        reply_queue_id=_int_or_none(row.get("id") or row.get("reply_queue_id")),
        inbound_id=_str_or_none(row.get("inbound_tweet_id") or row.get("inbound_id")),
        platform=platform,
        issue_type=issue_type,
        severity=severity,
        measured_length=measured,
        allowed_length=allowed,
        near_threshold=near_threshold,
        status=_str_or_none(row.get("status")),
        author_handle=_str_or_none(row.get("inbound_author_handle") or row.get("author_handle")),
        inbound_text_present=bool(_normalized_text(row.get("inbound_text"))),
        draft_preview=_shorten(draft, 80),
        detected_at=_str_or_none(row.get("detected_at")),
    )


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    *,
    missing_tables: tuple[str, ...] = (),
    missing_columns: dict[str, tuple[str, ...]] | None = None,
) -> ReplyLengthFitAuditReport:
    return ReplyLengthFitAuditReport(
        ok=False if missing_tables or missing_columns else True,
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "checked_replies": 0,
            "over_limit_count": 0,
            "near_limit_count": 0,
            "missing_draft_count": 0,
            "empty_draft_count": 0,
        },
        findings=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _fetch_reply_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    limit: int,
) -> list[dict[str, Any]]:
    query = "SELECT * FROM reply_queue ORDER BY " + _order_clause(columns) + " LIMIT ?"
    return [dict(row) for row in conn.execute(query, (limit,)).fetchall()]


def _order_clause(columns: set[str]) -> str:
    parts = []
    if "detected_at" in columns:
        parts.append("datetime(detected_at) ASC")
    parts.append("id ASC" if "id" in columns else "rowid ASC")
    return ", ".join(parts)


def _normalize_platform_limits(platform_limits: Mapping[str, int] | None) -> dict[str, int]:
    limits = dict(DEFAULT_PLATFORM_LIMITS)
    for platform, raw_limit in (platform_limits or {}).items():
        normalized = _normalize_platform(platform)
        try:
            limit = int(raw_limit)
        except (TypeError, ValueError) as exc:
            raise ValueError("platform limits must be positive integers") from exc
        if limit <= 0:
            raise ValueError("platform limits must be positive")
        limits[normalized] = limit
    return limits


def _normalize_platform(value: Any) -> str:
    return str(value or "x").strip().lower() or "x"


def _normalized_text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _finding_sort_key(item: ReplyLengthFitFinding) -> tuple[int, int, str, int, str]:
    return (
        _ISSUE_RANK.get(item.issue_type, 99),
        -(item.measured_length - item.allowed_length),
        item.detected_at or "",
        item.reply_queue_id or 0,
        item.inbound_id or "",
    )


def _connection(db: Any) -> sqlite3.Connection:
    return db.conn if hasattr(db, "conn") else db


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _is_records(value: Any) -> bool:
    return isinstance(value, Iterable) and not isinstance(value, (str, bytes, sqlite3.Connection))


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
    return value[: limit - 1].rstrip() + "..."
