"""Report reply drafts whose relationship context is missing or too thin."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from typing import Any, Iterable, Mapping, Sequence


DEFAULT_DAYS = 7
DEFAULT_LIMIT = 100
DEFAULT_MAX_INTERACTION_AGE_DAYS = 30
DEFAULT_STATUS = ("pending",)

REASON_MALFORMED_CONTEXT = "malformed_relationship_context"
REASON_MISSING_PROFILE = "missing_profile_context"
REASON_MISSING_LAST_INTERACTION = "missing_last_interaction"
REASON_STALE_LAST_INTERACTION = "stale_last_interaction"
REASON_ABSENT_NOTES = "absent_relationship_notes"

SEVERITY_BY_REASON = {
    REASON_MALFORMED_CONTEXT: "high",
    REASON_MISSING_PROFILE: "high",
    REASON_MISSING_LAST_INTERACTION: "medium",
    REASON_STALE_LAST_INTERACTION: "medium",
    REASON_ABSENT_NOTES: "medium",
}
SEVERITY_RANK = {"critical": 0, "high": 1, "medium": 2, "low": 3, "info": 4}


@dataclass(frozen=True)
class ReplyContextGapFinding:
    """One relationship-context gap for a reply target."""

    reply_id: int | None
    mention_id: str | None
    author_handle: str | None
    author_id: str | None
    platform: str
    severity: str
    reason_code: str
    detail: str
    suggested_action: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "reply_id": self.reply_id,
            "mention_id": self.mention_id,
            "author_handle": self.author_handle,
            "author_id": self.author_id,
            "platform": self.platform,
            "severity": self.severity,
            "reason_code": self.reason_code,
            "detail": self.detail,
            "suggested_action": self.suggested_action,
        }


@dataclass(frozen=True)
class ReplyContextGapItem:
    """Review packet for one inbound mention or drafted reply."""

    reply_id: int | None
    mention_id: str | None
    author_handle: str | None
    author_id: str | None
    platform: str
    status: str | None
    detected_at: str | None
    inbound_preview: str
    draft_preview: str
    profile_summary: str | None
    relationship_notes: str | None
    last_interaction_at: str | None
    last_interaction_age_days: float | None
    highest_severity: str
    findings: tuple[ReplyContextGapFinding, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "reply_id": self.reply_id,
            "mention_id": self.mention_id,
            "author_handle": self.author_handle,
            "author_id": self.author_id,
            "platform": self.platform,
            "status": self.status,
            "detected_at": self.detected_at,
            "inbound_preview": self.inbound_preview,
            "draft_preview": self.draft_preview,
            "profile_summary": self.profile_summary,
            "relationship_notes": self.relationship_notes,
            "last_interaction_at": self.last_interaction_at,
            "last_interaction_age_days": self.last_interaction_age_days,
            "highest_severity": self.highest_severity,
            "findings": [finding.to_dict() for finding in self.findings],
        }


@dataclass(frozen=True)
class ReplyContextGapReport:
    """Aggregated context gap report."""

    ok: bool
    generated_at: str
    filters: dict[str, Any]
    scanned_count: int
    item_count: int
    finding_count: int
    by_reason: dict[str, int]
    by_severity: dict[str, int]
    items: tuple[ReplyContextGapItem, ...]
    findings: tuple[ReplyContextGapFinding, ...]
    missing_tables: tuple[str, ...] = ()

    @property
    def blocking_issue_count(self) -> int:
        return self.finding_count

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "reply_context_gap_report",
            "ok": self.ok,
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "scanned_count": self.scanned_count,
            "item_count": self.item_count,
            "finding_count": self.finding_count,
            "blocking_issue_count": self.blocking_issue_count,
            "by_reason": dict(sorted(self.by_reason.items())),
            "by_severity": dict(sorted(self.by_severity.items())),
            "items": [item.to_dict() for item in self.items],
            "findings": [finding.to_dict() for finding in self.findings],
            "missing_tables": list(self.missing_tables),
        }


def build_reply_context_gap_report(
    rows: Iterable[Mapping[str, Any]],
    *,
    max_interaction_age_days: int = DEFAULT_MAX_INTERACTION_AGE_DAYS,
    min_severity: str | None = None,
    now: datetime | None = None,
    filters: Mapping[str, Any] | None = None,
    missing_tables: Sequence[str] = (),
) -> ReplyContextGapReport:
    """Build a relationship context gap report from plain reply-like records."""
    if max_interaction_age_days <= 0:
        raise ValueError("max_interaction_age_days must be positive")
    severity_floor = _normalise_severity(min_severity)
    generated_at = _as_utc(now or datetime.now(timezone.utc))
    base_filters = {
        "max_interaction_age_days": max_interaction_age_days,
        "min_severity": severity_floor,
    }
    if filters:
        base_filters.update(dict(filters))

    scanned = [
        analyze_reply_context_record(
            row,
            max_interaction_age_days=max_interaction_age_days,
            now=generated_at,
        )
        for row in rows
    ]
    items = tuple(_filter_item_findings(item, severity_floor) for item in scanned)
    items = tuple(item for item in items if item.findings)
    findings = tuple(
        sorted((finding for item in items for finding in item.findings), key=_finding_sort_key)
    )
    return ReplyContextGapReport(
        ok=not findings,
        generated_at=generated_at.isoformat(),
        filters=base_filters,
        scanned_count=len(scanned),
        item_count=len(items),
        finding_count=len(findings),
        by_reason=dict(Counter(finding.reason_code for finding in findings)),
        by_severity=dict(Counter(finding.severity for finding in findings)),
        items=tuple(sorted(items, key=_item_sort_key)),
        findings=findings,
        missing_tables=tuple(missing_tables),
    )


def analyze_reply_context_record(
    row: Mapping[str, Any],
    *,
    max_interaction_age_days: int = DEFAULT_MAX_INTERACTION_AGE_DAYS,
    now: datetime | None = None,
) -> ReplyContextGapItem:
    """Inspect one inbound mention or reply draft for relationship context gaps."""
    if max_interaction_age_days <= 0:
        raise ValueError("max_interaction_age_days must be positive")
    generated_at = _as_utc(now or datetime.now(timezone.utc))
    record = _row_dict(row)
    context, parse_error = _relationship_context(record.get("relationship_context"))
    identity = _identity(record, context)
    profile_summary = _profile_summary(record, context)
    relationship_notes = _relationship_notes(record, context)
    last_interaction_at = _last_interaction_at(record, context)
    last_interaction_age_days = _age_days(last_interaction_at, generated_at)

    findings: list[ReplyContextGapFinding] = []
    if parse_error:
        findings.append(
            _finding(
                identity,
                REASON_MALFORMED_CONTEXT,
                parse_error,
                "Repair relationship_context JSON before review.",
            )
        )
    if not profile_summary:
        findings.append(
            _finding(
                identity,
                REASON_MISSING_PROFILE,
                "No usable profile summary, display name, or bio was found.",
                f"Refresh profile enrichment for {identity['author_label']}.",
            )
        )
    if last_interaction_at is None:
        findings.append(
            _finding(
                identity,
                REASON_MISSING_LAST_INTERACTION,
                "No prior interaction timestamp was found.",
                f"Review recent interactions for {identity['author_label']} before drafting.",
            )
        )
    elif last_interaction_age_days is not None and last_interaction_age_days > max_interaction_age_days:
        findings.append(
            _finding(
                identity,
                REASON_STALE_LAST_INTERACTION,
                (
                    f"Last interaction is {last_interaction_age_days:.1f} days old, "
                    f"older than the {max_interaction_age_days}-day threshold."
                ),
                f"Refresh relationship history for {identity['author_label']}.",
            )
        )
    if not relationship_notes:
        findings.append(
            _finding(
                identity,
                REASON_ABSENT_NOTES,
                "No relationship notes or relationship-specific highlights were found.",
                f"Add relationship notes for {identity['author_label']}.",
            )
        )

    sorted_findings = tuple(sorted(findings, key=_finding_sort_key))
    return ReplyContextGapItem(
        reply_id=identity["reply_id"],
        mention_id=identity["mention_id"],
        author_handle=identity["author_handle"],
        author_id=identity["author_id"],
        platform=identity["platform"],
        status=_clean(record.get("status")),
        detected_at=_clean(record.get("detected_at") or record.get("created_at")),
        inbound_preview=_shorten(str(record.get("inbound_text") or record.get("text") or ""), 120),
        draft_preview=_shorten(str(record.get("draft_text") or ""), 120),
        profile_summary=profile_summary,
        relationship_notes=relationship_notes,
        last_interaction_at=last_interaction_at.isoformat() if last_interaction_at else None,
        last_interaction_age_days=last_interaction_age_days,
        highest_severity=_highest_severity(finding.severity for finding in sorted_findings),
        findings=sorted_findings,
    )


def format_reply_context_gap_report_json(report: ReplyContextGapReport) -> str:
    """Render deterministic JSON for automation and review packets."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_reply_context_gap_report_text(report: ReplyContextGapReport) -> str:
    """Render a compact human-readable context gap report."""
    lines = [
        "Reply Context Gap Report",
        (
            "Filters: "
            f"status={_display_filter(report.filters.get('status'))} "
            f"days={report.filters.get('days', 'all')} "
            f"platform={_display_filter(report.filters.get('platform'))} "
            f"min_severity={report.filters.get('min_severity')} "
            f"limit={report.filters.get('limit', 'none')}"
        ),
        f"Scanned: {report.scanned_count}",
        f"Items with gaps: {report.item_count}",
        f"Findings: {report.finding_count}",
    ]
    if report.by_reason:
        lines.append(
            "By reason: "
            + ", ".join(f"{key}={value}" for key, value in sorted(report.by_reason.items()))
        )
    if report.by_severity:
        lines.append(
            "By severity: "
            + ", ".join(f"{key}={value}" for key, value in sorted(report.by_severity.items()))
        )
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if not report.findings:
        lines.append("No relationship context gaps found.")
        return "\n".join(lines)
    lines.append("")
    for finding in report.findings:
        handle = finding.author_handle or finding.author_id or "unknown"
        lines.append(
            f"#{finding.reply_id or 'untracked'} {finding.severity} {finding.reason_code} "
            f"platform={finding.platform} author={handle} "
            f"mention_id={finding.mention_id or 'missing'}: {finding.suggested_action}"
        )
    return "\n".join(lines)


def _relationship_context(value: Any) -> tuple[dict[str, Any], str | None]:
    if value is None:
        return {}, None
    if isinstance(value, Mapping):
        return dict(value), None
    text = str(value).strip()
    if not text:
        return {}, None
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        return {}, f"relationship_context is not valid JSON: {exc.msg}"
    if not isinstance(parsed, dict):
        return {}, "relationship_context must be a JSON object"
    return parsed, None


def _identity(record: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    author_handle = _clean(
        _first_value(
            record,
            "inbound_author_handle",
            "author_handle",
            "handle",
        )
        or _first_value(context, "x_handle", "handle", "author_handle")
    )
    author_id = _clean(
        _first_value(record, "inbound_author_id", "author_id")
        or _first_value(context, "person_id", "author_id")
    )
    mention_id = _clean(
        _first_value(
            record,
            "mention_id",
            "inbound_tweet_id",
            "inbound_id",
            "inbound_cid",
        )
    )
    return {
        "reply_id": _int_or_none(_first_value(record, "reply_id", "id", "reply_queue_id")),
        "mention_id": mention_id,
        "author_handle": author_handle,
        "author_id": author_id,
        "platform": (_clean(record.get("platform")) or "x").casefold(),
        "author_label": author_handle or author_id or "this author",
    }


def _profile_summary(record: dict[str, Any], context: dict[str, Any]) -> str | None:
    value = _first_clean(
        record,
        "profile_summary",
        "author_profile",
        "profile_context",
        "author_bio",
        "bio",
        "display_name",
    ) or _first_clean(
        context,
        "profile_summary",
        "author_profile",
        "profile_context",
        "bio",
        "display_name",
        "x_handle",
    )
    return _shorten(value, 160) if value else None


def _relationship_notes(record: dict[str, Any], context: dict[str, Any]) -> str | None:
    value = _first_clean(
        record,
        "relationship_notes",
        "relationship_note",
        "relationship_summary",
        "context_notes",
    ) or _first_clean(
        context,
        "relationship_notes",
        "relationship_note",
        "relationship_summary",
        "context_notes",
        "notes",
    )
    if value:
        return _shorten(value, 180)
    recent = context.get("recent_interactions")
    if isinstance(recent, list) and recent:
        return _shorten(f"{len(recent)} recent interaction(s) available", 180)
    return None


def _last_interaction_at(record: dict[str, Any], context: dict[str, Any]) -> datetime | None:
    value = _first_value(
        record,
        "last_interaction_at",
        "last_interaction_timestamp",
        "relationship_context_updated_at",
        "context_updated_at",
    ) or _first_value(context, "last_interaction_at", "last_interaction", "last_seen_at")
    parsed = _parse_datetime(value)
    if parsed:
        return parsed
    recent = context.get("recent_interactions")
    if not isinstance(recent, list):
        return None
    candidates = [
        parsed
        for parsed in (
            _parse_datetime(
                _first_value(item, "created_at", "timestamp", "occurred_at", "interaction_at")
            )
            for item in recent
            if isinstance(item, Mapping)
        )
        if parsed is not None
    ]
    return max(candidates) if candidates else None


def _finding(
    identity: dict[str, Any],
    reason_code: str,
    detail: str,
    suggested_action: str,
) -> ReplyContextGapFinding:
    return ReplyContextGapFinding(
        reply_id=identity["reply_id"],
        mention_id=identity["mention_id"],
        author_handle=identity["author_handle"],
        author_id=identity["author_id"],
        platform=identity["platform"],
        severity=SEVERITY_BY_REASON[reason_code],
        reason_code=reason_code,
        detail=detail,
        suggested_action=suggested_action,
    )


def _filter_item_findings(
    item: ReplyContextGapItem,
    min_severity: str | None,
) -> ReplyContextGapItem:
    if min_severity is None:
        return item
    threshold = SEVERITY_RANK[min_severity]
    findings = tuple(
        finding
        for finding in item.findings
        if SEVERITY_RANK.get(finding.severity, 99) <= threshold
    )
    return ReplyContextGapItem(
        reply_id=item.reply_id,
        mention_id=item.mention_id,
        author_handle=item.author_handle,
        author_id=item.author_id,
        platform=item.platform,
        status=item.status,
        detected_at=item.detected_at,
        inbound_preview=item.inbound_preview,
        draft_preview=item.draft_preview,
        profile_summary=item.profile_summary,
        relationship_notes=item.relationship_notes,
        last_interaction_at=item.last_interaction_at,
        last_interaction_age_days=item.last_interaction_age_days,
        highest_severity=_highest_severity(finding.severity for finding in findings),
        findings=findings,
    )


def _normalise_severity(value: str | None) -> str | None:
    if value is None:
        return None
    severity = value.strip().casefold()
    if severity not in SEVERITY_RANK:
        raise ValueError(f"unknown severity: {value}")
    return severity


def _finding_sort_key(finding: ReplyContextGapFinding) -> tuple[int, str, int, str]:
    return (
        SEVERITY_RANK.get(finding.severity, 99),
        finding.reason_code,
        finding.reply_id or 0,
        finding.mention_id or "",
    )


def _item_sort_key(item: ReplyContextGapItem) -> tuple[int, int, str]:
    return (
        SEVERITY_RANK.get(item.highest_severity, 99),
        item.reply_id or 0,
        item.mention_id or "",
    )


def _highest_severity(severities: Iterable[str]) -> str:
    ordered = sorted(severities, key=lambda value: SEVERITY_RANK.get(value, 99))
    return ordered[0] if ordered else "none"


def _age_days(value: datetime | None, now: datetime) -> float | None:
    if value is None:
        return None
    return round(max((now - value).total_seconds() / 86400, 0.0), 1)


def _parse_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    for parser in (
        lambda candidate: datetime.fromisoformat(candidate.replace("Z", "+00:00")),
        lambda candidate: datetime.strptime(candidate, "%Y-%m-%d %H:%M:%S"),
    ):
        try:
            return _as_utc(parser(text))
        except ValueError:
            continue
    return None


def _row_dict(row: Mapping[str, Any]) -> dict[str, Any]:
    if hasattr(row, "keys"):
        return {str(key): row[key] for key in row.keys()}
    return dict(row)


def _first_value(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return None


def _first_clean(row: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = _clean(row.get(key))
        if value:
            return value
    return None


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _shorten(value: str, limit: int) -> str:
    text = " ".join(value.split())
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _display_filter(value: Any) -> str:
    if not value:
        return "all"
    if isinstance(value, (list, tuple)):
        return ",".join(str(item) for item in value)
    return str(value)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
