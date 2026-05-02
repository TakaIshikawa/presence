"""Audit pending reply drafts for incomplete target metadata."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any, Iterable, Sequence


DEFAULT_DAYS = 7
DEFAULT_LIMIT = 100
DEFAULT_STATUS = "pending"

BUCKETS = (
    "metadata_parse_error",
    "platform_id_mismatch",
    "missing_original_post",
    "missing_inbound_link",
    "missing_platform_metadata",
    "missing_platform_ids",
)
SEVERITY_BY_BUCKET = {
    "metadata_parse_error": "critical",
    "platform_id_mismatch": "high",
    "missing_original_post": "high",
    "missing_inbound_link": "medium",
    "missing_platform_metadata": "medium",
    "missing_platform_ids": "medium",
}
SEVERITY_RANK = {"critical": 0, "high": 1, "medium": 2, "low": 3}
AUDIT_COLUMNS = (
    "id",
    "status",
    "platform",
    "inbound_tweet_id",
    "inbound_author_handle",
    "inbound_author_id",
    "inbound_url",
    "inbound_cid",
    "our_tweet_id",
    "our_platform_id",
    "our_content_id",
    "our_post_text",
    "platform_metadata",
    "draft_text",
    "detected_at",
)


@dataclass(frozen=True)
class ReplyTargetMetadataFinding:
    """One target-context problem on a reply draft."""

    reply_queue_id: int
    bucket: str
    severity: str
    reason: str
    platform: str
    inbound_id: str | None
    inbound_url: str | None
    inbound_author_handle: str | None
    inbound_author_id: str | None
    our_content_id: int | None
    our_platform_id: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "reply_queue_id": self.reply_queue_id,
            "bucket": self.bucket,
            "severity": self.severity,
            "reason": self.reason,
            "platform": self.platform,
            "inbound_id": self.inbound_id,
            "inbound_url": self.inbound_url,
            "inbound_author_handle": self.inbound_author_handle,
            "inbound_author_id": self.inbound_author_id,
            "our_content_id": self.our_content_id,
            "our_platform_id": self.our_platform_id,
        }


@dataclass(frozen=True)
class ReplyTargetMetadataItem:
    """Audit summary for one pending reply draft."""

    id: int
    status: str
    platform: str
    inbound_id: str | None
    inbound_url: str | None
    inbound_author_handle: str | None
    inbound_author_id: str | None
    our_content_id: int | None
    our_platform_id: str | None
    our_tweet_id: str | None
    detected_at: str | None
    draft_preview: str
    findings: tuple[ReplyTargetMetadataFinding, ...]

    @property
    def highest_severity(self) -> str:
        return _highest_severity(finding.severity for finding in self.findings)

    @property
    def is_blocking(self) -> bool:
        return bool(self.findings)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "status": self.status,
            "platform": self.platform,
            "inbound_id": self.inbound_id,
            "inbound_url": self.inbound_url,
            "inbound_author_handle": self.inbound_author_handle,
            "inbound_author_id": self.inbound_author_id,
            "our_content_id": self.our_content_id,
            "our_platform_id": self.our_platform_id,
            "our_tweet_id": self.our_tweet_id,
            "detected_at": self.detected_at,
            "draft_preview": self.draft_preview,
            "highest_severity": self.highest_severity,
            "findings": [finding.to_dict() for finding in self.findings],
        }


@dataclass(frozen=True)
class ReplyTargetMetadataAuditReport:
    """Aggregated target metadata audit report for pending replies."""

    ok: bool
    generated_at: str
    filters: dict[str, Any]
    audited_count: int
    finding_count: int
    by_bucket: dict[str, int]
    by_severity: dict[str, int]
    items: tuple[ReplyTargetMetadataItem, ...]
    findings: tuple[ReplyTargetMetadataFinding, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    @property
    def blocking_issue_count(self) -> int:
        return self.finding_count

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "reply_target_metadata_audit",
            "ok": self.ok,
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "audited_count": self.audited_count,
            "finding_count": self.finding_count,
            "blocking_issue_count": self.blocking_issue_count,
            "by_bucket": dict(sorted(self.by_bucket.items())),
            "by_severity": dict(sorted(self.by_severity.items())),
            "items": [item.to_dict() for item in self.items],
            "findings": [finding.to_dict() for finding in self.findings],
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
        }


def build_reply_target_metadata_audit(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    platform: str | Sequence[str] | None = None,
    limit: int | None = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ReplyTargetMetadataAuditReport:
    """Find pending reply drafts whose target context is unsafe to review/post."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive")

    conn = _connection(db_or_conn)
    now = _as_utc(now or datetime.now(timezone.utc))
    platforms = _normalise_platforms(platform)
    filters = {
        "days": days,
        "status": DEFAULT_STATUS,
        "platform": list(platforms),
        "limit": limit,
    }
    columns = _table_columns(conn, "reply_queue")
    if not columns:
        return _empty_report(now, filters, missing_tables=("reply_queue",))

    missing = tuple(column for column in AUDIT_COLUMNS if column not in columns)
    if missing:
        return _empty_report(now, filters, missing_columns={"reply_queue": missing})

    rows = _reply_rows(conn, columns, days=days, platforms=platforms, limit=limit, now=now)
    items = tuple(_audit_row(row) for row in rows)
    findings = tuple(
        sorted(
            (finding for item in items for finding in item.findings),
            key=_finding_sort_key,
        )
    )
    return ReplyTargetMetadataAuditReport(
        ok=not findings,
        generated_at=now.isoformat(),
        filters=filters,
        audited_count=len(items),
        finding_count=len(findings),
        by_bucket=dict(Counter(finding.bucket for finding in findings)),
        by_severity=dict(Counter(finding.severity for finding in findings)),
        items=items,
        findings=findings,
    )


def format_reply_target_metadata_audit_json(report: ReplyTargetMetadataAuditReport) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_reply_target_metadata_audit_text(report: ReplyTargetMetadataAuditReport) -> str:
    """Render a compact human-readable target metadata report."""
    lines = [
        "Reply Target Metadata Audit",
        (
            "Filters: "
            f"status={report.filters.get('status')} days={report.filters.get('days')} "
            f"platform={_display_platform_filter(report.filters.get('platform'))} "
            f"limit={report.filters.get('limit')}"
        ),
        f"Audited: {report.audited_count}",
        f"Findings: {report.finding_count}",
    ]
    if report.by_bucket:
        lines.append(
            "By bucket: "
            + ", ".join(f"{key}={value}" for key, value in sorted(report.by_bucket.items()))
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
        lines.append("No pending reply drafts matched.")
        return "\n".join(lines)
    if not report.findings:
        lines.append("No target metadata findings.")
        return "\n".join(lines)

    lines.append("")
    for finding in report.findings:
        handle = finding.inbound_author_handle or finding.inbound_author_id or "unknown"
        lines.append(
            f"#{finding.reply_queue_id} {finding.severity} {finding.bucket} "
            f"platform={finding.platform or 'unknown'} author={handle} "
            f"inbound_id={finding.inbound_id or 'missing'} "
            f"content_id={finding.our_content_id if finding.our_content_id is not None else 'missing'}: "
            f"{finding.reason}"
        )
    return "\n".join(lines)


def _audit_row(row: dict[str, Any]) -> ReplyTargetMetadataItem:
    context = _row_context(row)
    metadata, parse_error = _parse_metadata(row.get("platform_metadata"))
    findings: list[ReplyTargetMetadataFinding] = []

    if parse_error:
        findings.append(_finding(context, "metadata_parse_error", parse_error))
    elif not metadata:
        findings.append(_finding(context, "missing_platform_metadata", "platform_metadata is empty"))

    if context["our_content_id"] is None or not _clean(row.get("our_post_text")):
        missing_parts = []
        if context["our_content_id"] is None:
            missing_parts.append("our_content_id")
        if not _clean(row.get("our_post_text")):
            missing_parts.append("our_post_text")
        findings.append(
            _finding(context, "missing_original_post", "missing " + ", ".join(missing_parts))
        )

    if not context["inbound_url"]:
        findings.append(_finding(context, "missing_inbound_link", "missing inbound_url"))

    missing_ids = _missing_platform_id_parts(row)
    if missing_ids:
        findings.append(
            _finding(context, "missing_platform_ids", "missing " + ", ".join(missing_ids))
        )

    mismatch = None if parse_error else _platform_id_mismatch(row, metadata)
    if mismatch:
        findings.append(_finding(context, "platform_id_mismatch", mismatch))

    sorted_findings = tuple(sorted(findings, key=_finding_sort_key))
    return ReplyTargetMetadataItem(
        id=context["reply_queue_id"],
        status=context["status"],
        platform=context["platform"],
        inbound_id=context["inbound_id"],
        inbound_url=context["inbound_url"],
        inbound_author_handle=context["inbound_author_handle"],
        inbound_author_id=context["inbound_author_id"],
        our_content_id=context["our_content_id"],
        our_platform_id=context["our_platform_id"],
        our_tweet_id=_clean(row.get("our_tweet_id")),
        detected_at=_clean(row.get("detected_at")),
        draft_preview=_shorten(str(row.get("draft_text") or ""), 96),
        findings=sorted_findings,
    )


def _row_context(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "reply_queue_id": int(row.get("id") or 0),
        "status": _clean(row.get("status")) or DEFAULT_STATUS,
        "platform": (_clean(row.get("platform")) or "").casefold(),
        "inbound_id": _clean(row.get("inbound_tweet_id")),
        "inbound_url": _clean(row.get("inbound_url")),
        "inbound_author_handle": _clean(row.get("inbound_author_handle")),
        "inbound_author_id": _clean(row.get("inbound_author_id")),
        "our_content_id": _int_value(row.get("our_content_id")),
        "our_platform_id": _clean(row.get("our_platform_id")),
    }


def _finding(
    context: dict[str, Any],
    bucket: str,
    reason: str,
) -> ReplyTargetMetadataFinding:
    return ReplyTargetMetadataFinding(
        reply_queue_id=context["reply_queue_id"],
        bucket=bucket,
        severity=SEVERITY_BY_BUCKET[bucket],
        reason=reason,
        platform=context["platform"],
        inbound_id=context["inbound_id"],
        inbound_url=context["inbound_url"],
        inbound_author_handle=context["inbound_author_handle"],
        inbound_author_id=context["inbound_author_id"],
        our_content_id=context["our_content_id"],
        our_platform_id=context["our_platform_id"],
    )


def _parse_metadata(value: Any) -> tuple[dict[str, Any], str | None]:
    if value is None:
        return {}, None
    if isinstance(value, dict):
        return value, None
    text = str(value).strip()
    if not text:
        return {}, None
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        return {}, f"platform_metadata is not valid JSON: {exc.msg}"
    if not isinstance(parsed, dict):
        return {}, "platform_metadata must be a JSON object"
    return parsed, None


def _missing_platform_id_parts(row: dict[str, Any]) -> list[str]:
    missing = []
    if not _clean(row.get("inbound_tweet_id")):
        missing.append("inbound_tweet_id")
    if not (_clean(row.get("our_platform_id")) or _clean(row.get("our_tweet_id"))):
        missing.append("our_platform_id")
    platform = (_clean(row.get("platform")) or "").casefold()
    if platform == "bluesky" and not _clean(row.get("inbound_cid")):
        missing.append("inbound_cid")
    return missing


def _platform_id_mismatch(row: dict[str, Any], metadata: dict[str, Any]) -> str | None:
    row_our = _clean(row.get("our_platform_id")) or _clean(row.get("our_tweet_id"))
    row_inbound = _clean(row.get("inbound_tweet_id"))
    metadata_our = _first_metadata_value(
        metadata,
        ("our_platform_id", "our_tweet_id", "root_uri", "root_id", "original_post_id"),
    )
    metadata_inbound = _first_metadata_value(
        metadata,
        ("inbound_tweet_id", "inbound_uri", "inbound_id", "reply_uri", "comment_id"),
    )
    root_ref = metadata.get("reply_root")
    if isinstance(root_ref, dict):
        metadata_our = metadata_our or _clean(root_ref.get("uri") or root_ref.get("id"))

    if row_our and metadata_our and row_our != metadata_our:
        return f"row original id {row_our} differs from metadata original id {metadata_our}"
    if row_inbound and metadata_inbound and row_inbound != metadata_inbound:
        return f"row inbound id {row_inbound} differs from metadata inbound id {metadata_inbound}"
    return None


def _first_metadata_value(metadata: dict[str, Any], keys: Sequence[str]) -> str | None:
    for key in keys:
        value = _clean(metadata.get(key))
        if value:
            return value
    return None


def _reply_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    days: int,
    platforms: tuple[str, ...],
    limit: int | None,
    now: datetime,
) -> list[dict[str, Any]]:
    where = ["COALESCE(status, 'pending') = ?"]
    params: list[Any] = [DEFAULT_STATUS]
    cutoff = now - timedelta(days=days)
    where.append("(detected_at IS NULL OR datetime(detected_at) >= datetime(?))")
    params.append(cutoff.isoformat())
    if platforms:
        placeholders = ",".join("?" for _ in platforms)
        where.append(f"LOWER(COALESCE(platform, '')) IN ({placeholders})")
        params.extend(platforms)

    select_columns = ", ".join(_quote_identifier(column) for column in AUDIT_COLUMNS)
    query = f"SELECT {select_columns} FROM reply_queue WHERE " + " AND ".join(where)
    query += " ORDER BY datetime(detected_at) DESC, id ASC"
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)
    return [dict(row) for row in conn.execute(query, params).fetchall()]


def _normalise_platforms(platform: str | Sequence[str] | None) -> tuple[str, ...]:
    if platform is None:
        return ()
    raw = (platform,) if isinstance(platform, str) else tuple(platform)
    return tuple(sorted({item.strip().casefold() for item in raw if item and item.strip()}))


def _finding_sort_key(finding: ReplyTargetMetadataFinding) -> tuple[int, str, int, str]:
    return (
        SEVERITY_RANK.get(finding.severity, 99),
        finding.bucket,
        finding.reply_queue_id,
        finding.reason,
    )


def _highest_severity(severities: Iterable[str]) -> str:
    ordered = sorted(severities, key=lambda value: SEVERITY_RANK.get(value, 99))
    return ordered[0] if ordered else "none"


def _display_platform_filter(value: Any) -> str:
    if not value:
        return "all"
    return ",".join(str(item) for item in value)


def _empty_report(
    now: datetime,
    filters: dict[str, Any],
    *,
    missing_tables: tuple[str, ...] = (),
    missing_columns: dict[str, tuple[str, ...]] | None = None,
) -> ReplyTargetMetadataAuditReport:
    return ReplyTargetMetadataAuditReport(
        ok=True,
        generated_at=now.isoformat(),
        filters=filters,
        audited_count=0,
        finding_count=0,
        by_bucket={},
        by_severity={},
        items=(),
        findings=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3 connection or database wrapper with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({_quote_identifier(table)})")}
    except sqlite3.Error:
        return set()


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _int_value(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _shorten(value: str, limit: int) -> str:
    text = " ".join(value.split())
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'
