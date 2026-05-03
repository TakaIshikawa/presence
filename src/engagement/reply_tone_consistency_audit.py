"""Audit queued reply drafts against approved reply tone baselines."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from statistics import mean
from typing import Any, Iterable, Sequence


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 50
DEFAULT_MIN_BASELINE = 3
DEFAULT_QUEUED_STATUSES = ("pending",)
DEFAULT_BASELINE_STATUSES = ("approved", "posted", "published")

GENERIC_PHRASES = (
    "thanks for sharing",
    "great point",
    "good point",
    "really interesting",
    "interesting point",
    "appreciate you",
    "appreciate the",
    "nice writeup",
    "nice write-up",
    "this is helpful",
    "helpful context",
)
PROMOTIONAL_PHRASES = (
    "book a demo",
    "check out",
    "download our",
    "link in bio",
    "our product",
    "our platform",
    "sign up",
    "subscribe",
    "try our",
    "we built",
)
DEFERENTIAL_PHRASES = (
    "apologies",
    "happy to",
    "hope that helps",
    "if that makes sense",
    "i could be wrong",
    "i might be wrong",
    "just wanted",
    "maybe i'm missing",
    "sorry",
    "totally fair",
)

_WORD_RE = re.compile(r"[a-z0-9]+(?:'[a-z0-9]+)?", re.IGNORECASE)
_SPACE_RE = re.compile(r"\s+")
_URL_RE = re.compile(r"https?://\S+")


@dataclass(frozen=True)
class ReplyToneMetrics:
    """Small tone and style signal set for one reply."""

    word_count: int
    generic_phrase_count: int
    promotional_phrase_count: int
    deferential_phrase_count: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ReplyToneBaseline:
    """Aggregate tone baseline from approved or published replies."""

    sample_count: int
    average_word_count: float
    average_generic_phrase_count: float
    average_promotional_phrase_count: float
    average_deferential_phrase_count: float

    @property
    def sufficient(self) -> bool:
        return self.sample_count > 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "sample_count": self.sample_count,
            "average_word_count": self.average_word_count,
            "average_generic_phrase_count": self.average_generic_phrase_count,
            "average_promotional_phrase_count": self.average_promotional_phrase_count,
            "average_deferential_phrase_count": self.average_deferential_phrase_count,
        }


@dataclass(frozen=True)
class ReplyToneFinding:
    """One queued reply draft whose tone differs from the baseline."""

    reply_queue_id: int | None
    status: str
    platform: str
    severity: str
    reasons: tuple[str, ...]
    metrics: ReplyToneMetrics
    baseline: ReplyToneBaseline
    draft_preview: str
    detected_at: str | None = None
    inbound_author_handle: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "reply_queue_id": self.reply_queue_id,
            "status": self.status,
            "platform": self.platform,
            "severity": self.severity,
            "reasons": list(self.reasons),
            "metrics": self.metrics.to_dict(),
            "baseline": self.baseline.to_dict(),
            "draft_preview": self.draft_preview,
            "detected_at": self.detected_at,
            "inbound_author_handle": self.inbound_author_handle,
        }


@dataclass(frozen=True)
class ReplyToneConsistencyAuditReport:
    """Queued reply tone consistency audit report."""

    ok: bool
    generated_at: str
    filters: dict[str, Any]
    baseline: ReplyToneBaseline
    audited_count: int
    finding_count: int
    by_reason: dict[str, int]
    by_severity: dict[str, int]
    findings: tuple[ReplyToneFinding, ...]
    warnings: tuple[str, ...] = ()
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    @property
    def blocking_issue_count(self) -> int:
        return self.finding_count

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "reply_tone_consistency_audit",
            "ok": self.ok,
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "baseline": self.baseline.to_dict(),
            "audited_count": self.audited_count,
            "finding_count": self.finding_count,
            "blocking_issue_count": self.blocking_issue_count,
            "by_reason": dict(sorted(self.by_reason.items())),
            "by_severity": dict(sorted(self.by_severity.items())),
            "findings": [finding.to_dict() for finding in self.findings],
            "warnings": list(self.warnings),
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
        }


def build_reply_tone_consistency_audit(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_baseline: int = DEFAULT_MIN_BASELINE,
    queued_statuses: Sequence[str] | None = None,
    baseline_statuses: Sequence[str] | None = None,
    platform: str | Sequence[str] | None = None,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ReplyToneConsistencyAuditReport:
    """Compare queued reply drafts to tone metrics from approved or published replies."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_baseline <= 0:
        raise ValueError("min_baseline must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    queued = _normalize_values(queued_statuses or DEFAULT_QUEUED_STATUSES)
    baseline_selected = _normalize_values(baseline_statuses or DEFAULT_BASELINE_STATUSES)
    platforms = _normalize_values((platform,) if isinstance(platform, str) else platform)
    filters = {
        "days": days,
        "min_baseline": min_baseline,
        "queued_statuses": list(queued),
        "baseline_statuses": list(baseline_selected),
        "platform": list(platforms),
        "limit": limit,
    }

    conn = _connection(db_or_conn)
    columns = _table_columns(conn, "reply_queue")
    if not columns:
        return _empty_report(
            generated_at,
            filters,
            ReplyToneBaseline(0, 0.0, 0.0, 0.0, 0.0),
            missing_tables=("reply_queue",),
        )
    missing_required = tuple(column for column in ("id", "draft_text") if column not in columns)
    if missing_required:
        return _empty_report(
            generated_at,
            filters,
            ReplyToneBaseline(0, 0.0, 0.0, 0.0, 0.0),
            missing_columns={"reply_queue": missing_required},
        )

    baseline_rows = _reply_rows(
        conn,
        columns,
        days=days,
        statuses=baseline_selected,
        platforms=platforms,
        now=generated_at,
    )
    baseline = build_reply_tone_baseline(row.get("draft_text") for row in baseline_rows)
    if baseline.sample_count < min_baseline:
        warning = (
            "insufficient_baseline: "
            f"sample_count={baseline.sample_count} min_baseline={min_baseline}"
        )
        return _empty_report(
            generated_at,
            filters,
            baseline,
            warnings=(warning,),
        )

    queued_rows = _reply_rows(
        conn,
        columns,
        days=days,
        statuses=queued,
        platforms=platforms,
        now=generated_at,
    )
    findings = [
        finding
        for row in queued_rows
        if (finding := inspect_reply_tone_consistency(row, baseline)) is not None
    ]
    findings.sort(key=_finding_sort_key)
    findings = findings[:limit]
    return ReplyToneConsistencyAuditReport(
        ok=not findings,
        generated_at=generated_at.isoformat(),
        filters=filters,
        baseline=baseline,
        audited_count=len(queued_rows),
        finding_count=len(findings),
        by_reason=dict(Counter(reason for finding in findings for reason in finding.reasons)),
        by_severity=dict(Counter(finding.severity for finding in findings)),
        findings=tuple(findings),
    )


def build_reply_tone_baseline(texts: Iterable[Any]) -> ReplyToneBaseline:
    """Compute aggregate tone metrics from known-good reply text."""
    metrics = [inspect_reply_tone_metrics(str(text or "")) for text in texts if str(text or "").strip()]
    if not metrics:
        return ReplyToneBaseline(0, 0.0, 0.0, 0.0, 0.0)
    return ReplyToneBaseline(
        sample_count=len(metrics),
        average_word_count=round(mean(metric.word_count for metric in metrics), 2),
        average_generic_phrase_count=round(
            mean(metric.generic_phrase_count for metric in metrics), 2
        ),
        average_promotional_phrase_count=round(
            mean(metric.promotional_phrase_count for metric in metrics), 2
        ),
        average_deferential_phrase_count=round(
            mean(metric.deferential_phrase_count for metric in metrics), 2
        ),
    )


def inspect_reply_tone_consistency(
    row: dict[str, Any],
    baseline: ReplyToneBaseline,
) -> ReplyToneFinding | None:
    """Inspect one reply_queue-style row against a precomputed tone baseline."""
    draft = str(row.get("draft_text") or "")
    metrics = inspect_reply_tone_metrics(draft)
    reasons = _tone_reasons(metrics, baseline)
    if not reasons:
        return None
    return ReplyToneFinding(
        reply_queue_id=_int_or_none(row.get("id")),
        status=str(row.get("status") or "pending"),
        platform=str(row.get("platform") or "x"),
        severity=_severity(reasons),
        reasons=tuple(reasons),
        metrics=metrics,
        baseline=baseline,
        draft_preview=_preview(draft),
        detected_at=_optional_text(row.get("detected_at")),
        inbound_author_handle=_optional_text(row.get("inbound_author_handle")),
    )


def inspect_reply_tone_metrics(text: str) -> ReplyToneMetrics:
    """Compute tone metrics for one reply draft."""
    normalized = _normalize_text(text)
    return ReplyToneMetrics(
        word_count=len(_WORD_RE.findall(normalized)),
        generic_phrase_count=_phrase_count(normalized, GENERIC_PHRASES),
        promotional_phrase_count=_phrase_count(normalized, PROMOTIONAL_PHRASES),
        deferential_phrase_count=_phrase_count(normalized, DEFERENTIAL_PHRASES),
    )


def format_reply_tone_consistency_audit_json(
    report: ReplyToneConsistencyAuditReport,
) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_reply_tone_consistency_audit_markdown(
    report: ReplyToneConsistencyAuditReport,
) -> str:
    """Render a compact markdown tone audit report."""
    filters = report.filters
    lines = [
        "# Reply Tone Consistency Audit",
        "",
        (
            f"- Generated: {report.generated_at}\n"
            f"- Filters: days={filters['days']} min_baseline={filters['min_baseline']} "
            f"queued={','.join(filters['queued_statuses'])} "
            f"baseline={','.join(filters['baseline_statuses'])} "
            f"platform={_display_filter(filters['platform'])} limit={filters['limit']}\n"
            f"- Baseline replies: {report.baseline.sample_count}\n"
            f"- Baseline avg words: {report.baseline.average_word_count:.2f}\n"
            f"- Audited drafts: {report.audited_count}\n"
            f"- Findings: {report.finding_count}"
        ),
    ]
    if report.warnings:
        lines.append("")
        lines.append("## Warnings")
        lines.extend(f"- {warning}" for warning in report.warnings)
    if report.by_reason:
        lines.append("")
        lines.append("## Reason Counts")
        for reason, count in sorted(report.by_reason.items()):
            lines.append(f"- `{reason}`: {count}")
    if report.missing_tables:
        lines.append("")
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        lines.append("")
        lines.append(
            "Missing columns: "
            + ", ".join(
                f"{table}.{column}"
                for table, columns in sorted(report.missing_columns.items())
                for column in columns
            )
        )
    if not report.findings:
        lines.append("")
        lines.append("No queued reply tone consistency findings matched.")
        return "\n".join(lines)

    lines.append("")
    lines.append("## Findings")
    for finding in report.findings:
        metrics = finding.metrics
        lines.append(
            f"- `{finding.severity}` reply_queue:{finding.reply_queue_id} "
            f"reasons={','.join(finding.reasons)} words={metrics.word_count} "
            f"generic={metrics.generic_phrase_count} promotional={metrics.promotional_phrase_count} "
            f"deferential={metrics.deferential_phrase_count} "
            f"@{finding.inbound_author_handle or 'unknown'}"
        )
        lines.append(f"  draft: {finding.draft_preview}")
    return "\n".join(lines)


def _tone_reasons(metrics: ReplyToneMetrics, baseline: ReplyToneBaseline) -> list[str]:
    reasons: list[str] = []
    if metrics.generic_phrase_count >= max(2, round(baseline.average_generic_phrase_count) + 2):
        reasons.append("overly_generic")
    if metrics.promotional_phrase_count >= max(1, round(baseline.average_promotional_phrase_count) + 1):
        reasons.append("too_promotional")
    if metrics.deferential_phrase_count >= max(2, round(baseline.average_deferential_phrase_count) + 2):
        reasons.append("too_deferential")
    long_threshold = max(45.0, baseline.average_word_count * 1.75)
    if metrics.word_count >= long_threshold and metrics.word_count >= baseline.average_word_count + 20:
        reasons.append("excessive_length")
    return reasons


def _severity(reasons: Sequence[str]) -> str:
    if "too_promotional" in reasons or len(reasons) >= 3:
        return "high"
    if "excessive_length" in reasons or len(reasons) >= 2:
        return "medium"
    return "low"


def _reply_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    days: int,
    statuses: tuple[str, ...],
    platforms: tuple[str, ...],
    now: datetime,
) -> list[dict[str, Any]]:
    filters: list[str] = []
    params: list[Any] = []
    if statuses and "all" not in statuses and "status" in columns:
        placeholders = ", ".join("?" for _ in statuses)
        filters.append(f"LOWER(COALESCE(status, 'pending')) IN ({placeholders})")
        params.extend(statuses)
    if platforms and "platform" in columns:
        placeholders = ", ".join("?" for _ in platforms)
        filters.append(f"LOWER(COALESCE(platform, 'x')) IN ({placeholders})")
        params.extend(platforms)
    timestamp_expr = _timestamp_expr(columns)
    if timestamp_expr:
        filters.append(f"({timestamp_expr} IS NULL OR {timestamp_expr} >= ?)")
        params.append((now - timedelta(days=days)).isoformat())

    query = "SELECT * FROM reply_queue"
    if filters:
        query += " WHERE " + " AND ".join(filters)
    query += " ORDER BY " + _order_clause(columns)
    cursor = conn.execute(query, params)
    names = [description[0] for description in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _timestamp_expr(columns: set[str]) -> str:
    available = [column for column in ("posted_at", "reviewed_at", "detected_at") if column in columns]
    if not available:
        return ""
    return "COALESCE(" + ", ".join(available) + ")"


def _order_clause(columns: set[str]) -> str:
    timestamp_expr = _timestamp_expr(columns)
    if timestamp_expr and "id" in columns:
        return f"{timestamp_expr} DESC, id ASC"
    if timestamp_expr:
        return f"{timestamp_expr} DESC"
    return "id ASC" if "id" in columns else "rowid ASC"


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    baseline: ReplyToneBaseline,
    *,
    warnings: tuple[str, ...] = (),
    missing_tables: tuple[str, ...] = (),
    missing_columns: dict[str, tuple[str, ...]] | None = None,
) -> ReplyToneConsistencyAuditReport:
    return ReplyToneConsistencyAuditReport(
        ok=True,
        generated_at=generated_at.isoformat(),
        filters=filters,
        baseline=baseline,
        audited_count=0,
        finding_count=0,
        by_reason={},
        by_severity={},
        findings=(),
        warnings=warnings,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _normalize_values(values: Iterable[str] | None) -> tuple[str, ...]:
    if not values:
        return ()
    normalized = []
    seen = set()
    for value in values:
        item = str(value or "").strip().lower()
        if item and item not in seen:
            normalized.append(item)
            seen.add(item)
    return tuple(normalized)


def _normalize_text(text: str) -> str:
    without_urls = _URL_RE.sub("", text.lower())
    return _SPACE_RE.sub(" ", without_urls).strip()


def _phrase_count(normalized_text: str, phrases: Sequence[str]) -> int:
    return sum(1 for phrase in phrases if phrase in normalized_text)


def _preview(text: str, limit: int = 140) -> str:
    compact = _SPACE_RE.sub(" ", text).strip()
    if len(compact) <= limit:
        return compact
    return compact[: limit - 1].rstrip() + "..."


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _finding_sort_key(finding: ReplyToneFinding) -> tuple[int, int, int]:
    severity_order = {"high": 0, "medium": 1, "low": 2}
    return (
        severity_order.get(finding.severity, 9),
        -len(finding.reasons),
        finding.reply_queue_id or 0,
    )


def _display_filter(values: Sequence[str]) -> str:
    return ",".join(values) if values else "all"
