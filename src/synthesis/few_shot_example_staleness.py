"""Audit few-shot examples for stale signals and prompt-format mismatch."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
from typing import Any, Mapping, Sequence


DEFAULT_MAX_AGE_DAYS = 90
DEFAULT_RECENT_WINDOW_DAYS = 30
DEFAULT_MIN_ENGAGEMENT_SCORE = 1.0


@dataclass(frozen=True)
class FewShotExampleStalenessRow:
    """One historical example with staleness reasons."""

    example_id: str
    content_type: str
    detected_format: str
    last_engagement_timestamp: str | None
    staleness_reasons: tuple[str, ...]
    priority_score: float

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["staleness_reasons"] = list(self.staleness_reasons)
        return payload


@dataclass(frozen=True)
class FewShotExampleStalenessReport:
    """Few-shot example staleness report."""

    artifact_type: str
    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    rows: tuple[FewShotExampleStalenessRow, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": self.artifact_type,
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "rows": [row.to_dict() for row in self.rows],
            "totals": dict(sorted(self.totals.items())),
        }


def build_few_shot_example_staleness_report(
    recent_generated_rows: Sequence[Mapping[str, Any]],
    historical_example_rows: Sequence[Mapping[str, Any]],
    *,
    max_age_days: int = DEFAULT_MAX_AGE_DAYS,
    recent_window_days: int = DEFAULT_RECENT_WINDOW_DAYS,
    min_engagement_score: float = DEFAULT_MIN_ENGAGEMENT_SCORE,
    now: datetime | None = None,
) -> FewShotExampleStalenessReport:
    """Flag stale few-shot examples relative to recent generation needs."""
    if max_age_days <= 0:
        raise ValueError("max_age_days must be positive")
    if recent_window_days <= 0:
        raise ValueError("recent_window_days must be positive")
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    recent_formats = _recent_formats(recent_generated_rows, generated_at, recent_window_days)
    rows = [
        _audit_example(
            example,
            recent_formats=recent_formats,
            max_age_days=max_age_days,
            min_engagement_score=min_engagement_score,
            now=generated_at,
        )
        for example in historical_example_rows
    ]
    rows = [row for row in rows if row.staleness_reasons]
    rows.sort(key=lambda row: (-row.priority_score, row.content_type, row.example_id))
    reason_counts = Counter(reason for row in rows for reason in row.staleness_reasons)
    return FewShotExampleStalenessReport(
        artifact_type="few_shot_example_staleness",
        generated_at=generated_at.isoformat(),
        filters={
            "max_age_days": max_age_days,
            "min_engagement_score": min_engagement_score,
            "recent_window_days": recent_window_days,
        },
        totals={
            "example_count": len(historical_example_rows),
            "flagged_count": len(rows),
            "missing_engagement_count": reason_counts.get("missing_engagement", 0),
            "reason_counts": dict(sorted(reason_counts.items())),
        },
        rows=tuple(rows),
    )


def format_few_shot_example_staleness_json(report: FewShotExampleStalenessReport) -> str:
    """Serialize the report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_few_shot_example_staleness_text(report: FewShotExampleStalenessReport) -> str:
    """Render a stable text report."""
    lines = [
        "Few-Shot Example Staleness",
        f"Generated: {report.generated_at}",
        (
            f"Examples: {report.totals['example_count']} "
            f"flagged={report.totals['flagged_count']}"
        ),
    ]
    if not report.rows:
        lines.append("No stale few-shot examples found.")
        return "\n".join(lines)
    for row in report.rows:
        lines.append(
            f"- example={row.example_id} type={row.content_type} format={row.detected_format} "
            f"last_engagement={row.last_engagement_timestamp or '-'} "
            f"priority={row.priority_score:g} reasons={','.join(row.staleness_reasons)}"
        )
    return "\n".join(lines)


def _audit_example(
    row: Mapping[str, Any],
    *,
    recent_formats: dict[str, set[str]],
    max_age_days: int,
    min_engagement_score: float,
    now: datetime,
) -> FewShotExampleStalenessRow:
    content_type = _clean(row.get("content_type"), "unknown")
    detected_format = _detect_format(row)
    last_engagement = _timestamp(
        row.get("last_engagement_timestamp")
        or row.get("last_engaged_at")
        or row.get("fetched_at")
    )
    engagement_score = _float(row.get("engagement_score"))
    source_timestamp = _timestamp(row.get("source_timestamp") or row.get("source_updated_at"))
    reasons: list[str] = []
    priority = 0.0

    if last_engagement is None:
        reasons.append("missing_engagement")
        priority += 2.0
    elif now - last_engagement > timedelta(days=max_age_days):
        reasons.append("old_engagement")
        priority += 1.5
    if engagement_score is None:
        if "missing_engagement" not in reasons:
            reasons.append("missing_engagement")
        priority += 1.0
    elif engagement_score < min_engagement_score:
        reasons.append("low_recent_engagement")
        priority += min_engagement_score - engagement_score + 1.0
    expected_formats = recent_formats.get(content_type, set())
    if expected_formats and detected_format not in expected_formats:
        reasons.append("format_mismatch")
        priority += 1.25
    if source_timestamp is not None and now - source_timestamp > timedelta(days=max_age_days):
        reasons.append("stale_source_evidence")
        priority += 1.0

    return FewShotExampleStalenessRow(
        example_id=str(row.get("id") or row.get("example_id") or ""),
        content_type=content_type,
        detected_format=detected_format,
        last_engagement_timestamp=last_engagement.isoformat() if last_engagement else None,
        staleness_reasons=tuple(sorted(dict.fromkeys(reasons))),
        priority_score=round(priority, 3),
    )


def _recent_formats(
    rows: Sequence[Mapping[str, Any]],
    now: datetime,
    recent_window_days: int,
) -> dict[str, set[str]]:
    cutoff = now - timedelta(days=recent_window_days)
    formats: dict[str, set[str]] = {}
    for row in rows:
        created_at = _timestamp(row.get("created_at") or row.get("generated_at"))
        if created_at is not None and created_at < cutoff:
            continue
        content_type = _clean(row.get("content_type"), "unknown")
        formats.setdefault(content_type, set()).add(_detect_format(row))
    return formats


def _detect_format(row: Mapping[str, Any]) -> str:
    explicit = row.get("content_format") or row.get("format") or row.get("prompt_format")
    if explicit:
        return _clean(explicit, "unknown")
    content = str(row.get("content") or "")
    if "\n" in content and len([line for line in content.splitlines() if line.strip()]) >= 3:
        return "thread"
    if content.strip().endswith("?"):
        return "question"
    if len(content) > 500:
        return "longform"
    return "shortform"


def _timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return _ensure_utc(value)
    text = str(value).strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _clean(value: Any, fallback: str) -> str:
    return str(value or fallback).strip() or fallback


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
