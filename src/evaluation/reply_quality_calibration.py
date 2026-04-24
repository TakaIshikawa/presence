"""Calibrate reply evaluator scores against review outcomes."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any


DEFAULT_SCORE_BANDS: tuple[tuple[float, float], ...] = (
    (0.0, 2.0),
    (2.0, 4.0),
    (4.0, 6.0),
    (6.0, 8.0),
    (8.0, 10.0),
)


@dataclass(frozen=True)
class ReplyQualityBand:
    """Review outcomes for one evaluator score band."""

    band: str
    min_score: float
    max_score: float
    count: int
    approval_count: int
    rejection_count: int
    dismissal_count: int
    approval_rate: float
    rejection_rate: float
    dismissal_rate: float
    average_score: float
    common_failure_reasons: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class ReplyThresholdRecommendation:
    """Non-mutating threshold guidance derived from the calibration report."""

    current_threshold: float | None
    recommended_threshold: float | None
    action: str
    rationale: str
    high_rejection_bands: list[str]
    min_samples: int


@dataclass(frozen=True)
class ReplyQualityCalibrationReport:
    """Complete reply quality calibration report."""

    days: int
    min_samples: int
    generated_at: str
    sample_count: int
    reviewed_count: int
    approval_count: int
    rejection_count: int
    dismissal_count: int
    approval_rate: float
    rejection_rate: float
    dismissal_rate: float
    score_bands: list[ReplyQualityBand]
    common_failure_reasons: list[dict[str, Any]]
    threshold_recommendation: ReplyThresholdRecommendation


class ReplyQualityCalibrator:
    """Aggregate reply scores and human review outcomes."""

    def __init__(self, db) -> None:
        self.db = db

    def build_report(
        self,
        *,
        days: int = 30,
        min_samples: int = 5,
        current_threshold: float | None = 6.0,
        now: datetime | None = None,
    ) -> ReplyQualityCalibrationReport:
        """Build a calibration report for replies in the lookback window."""
        if days <= 0:
            raise ValueError("days must be positive")
        if min_samples <= 0:
            raise ValueError("min_samples must be positive")

        now = _as_utc(now or datetime.now(timezone.utc))
        cutoff = now - timedelta(days=days)
        rows = self._fetch_reply_rows(cutoff)
        events_by_reply = self._fetch_review_events([row["id"] for row in rows])
        samples = [
            _sample_from_row(row, events_by_reply.get(row["id"], []))
            for row in rows
            if row.get("quality_score") is not None
        ]

        band_samples: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for sample in samples:
            band = _score_band(float(sample["quality_score"]))
            band_samples[band[0]].append(sample)

        bands = [
            _build_band_summary(label, low, high, band_samples.get(label, []))
            for label, low, high in _band_labels()
        ]

        reviewed = [s for s in samples if s["approved"] or s["rejected"] or s["dismissed"]]
        approval_count = sum(1 for s in samples if s["approved"])
        rejection_count = sum(1 for s in samples if s["rejected"])
        dismissal_count = sum(1 for s in samples if s["dismissed"])
        total = len(samples)

        failure_reasons = _common_failure_reasons(samples)
        recommendation = _threshold_recommendation(
            bands,
            current_threshold=current_threshold,
            min_samples=min_samples,
        )

        return ReplyQualityCalibrationReport(
            days=days,
            min_samples=min_samples,
            generated_at=now.isoformat(),
            sample_count=total,
            reviewed_count=len(reviewed),
            approval_count=approval_count,
            rejection_count=rejection_count,
            dismissal_count=dismissal_count,
            approval_rate=_rate(approval_count, total),
            rejection_rate=_rate(rejection_count, total),
            dismissal_rate=_rate(dismissal_count, total),
            score_bands=bands,
            common_failure_reasons=failure_reasons,
            threshold_recommendation=recommendation,
        )

    def _fetch_reply_rows(self, cutoff: datetime) -> list[dict[str, Any]]:
        cursor = self.db.conn.execute(
            """SELECT id, quality_score, quality_flags, status, detected_at, reviewed_at,
                      posted_at, draft_text, inbound_text, our_post_text
               FROM reply_queue
               WHERE quality_score IS NOT NULL
                 AND datetime(COALESCE(reviewed_at, detected_at)) >= datetime(?)
               ORDER BY quality_score ASC, id ASC""",
            (cutoff.isoformat(),),
        )
        return [dict(row) for row in cursor.fetchall()]

    def _fetch_review_events(self, reply_ids: list[int]) -> dict[int, list[dict[str, Any]]]:
        if not reply_ids:
            return {}
        placeholders = ", ".join("?" for _ in reply_ids)
        cursor = self.db.conn.execute(
            f"""SELECT reply_queue_id, event_type, actor, old_status, new_status, notes, created_at, id
                FROM reply_review_events
                WHERE reply_queue_id IN ({placeholders})
                ORDER BY reply_queue_id ASC, datetime(created_at) ASC, id ASC""",
            reply_ids,
        )
        grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for row in cursor.fetchall():
            item = dict(row)
            grouped[item["reply_queue_id"]].append(item)
        return dict(grouped)


def _sample_from_row(row: dict[str, Any], events: list[dict[str, Any]]) -> dict[str, Any]:
    event_types = {str(event.get("event_type") or "").lower() for event in events}
    new_statuses = {str(event.get("new_status") or "").lower() for event in events}
    status = str(row.get("status") or "").lower()

    approved = bool(event_types & {"approved", "edited", "posted"}) or status in {
        "approved",
        "posted",
    }
    rejected = "rejected" in event_types
    dismissed = status == "dismissed" or "dismissed" in new_statuses or "expired" in event_types

    return {
        "id": row["id"],
        "quality_score": float(row["quality_score"]),
        "quality_flags": _parse_quality_flags(row.get("quality_flags")),
        "status": status,
        "approved": approved,
        "rejected": rejected,
        "dismissed": dismissed,
        "failure_reasons": _failure_reasons(row, events, rejected or dismissed),
    }


def _parse_quality_flags(flags_json: str | None) -> list[str]:
    if not flags_json:
        return []
    try:
        parsed = json.loads(flags_json)
    except (TypeError, json.JSONDecodeError):
        return [str(flags_json)]
    if not isinstance(parsed, list):
        return []
    return sorted(str(item) for item in parsed if item)


def _failure_reasons(
    row: dict[str, Any],
    events: list[dict[str, Any]],
    failed: bool,
) -> list[str]:
    reasons: list[str] = []
    for flag in _parse_quality_flags(row.get("quality_flags")):
        reasons.append(f"flag:{flag}")
    if failed:
        for event in events:
            event_type = str(event.get("event_type") or "").lower()
            if event_type in {"rejected", "expired", "failed"}:
                note = str(event.get("notes") or "").strip()
                if note:
                    reasons.append(_normalize_reason(note))
                else:
                    reasons.append(event_type)
    return sorted(dict.fromkeys(reasons))


def _normalize_reason(reason: str) -> str:
    normalized = " ".join(reason.lower().split())
    prefixes = {
        "dismissed during manual review": "dismissed during manual review",
        "not worth replying": "not worth replying",
    }
    return prefixes.get(normalized, normalized[:120])


def _band_labels() -> list[tuple[str, float, float]]:
    return [(_format_band(low, high), low, high) for low, high in DEFAULT_SCORE_BANDS]


def _score_band(score: float) -> tuple[str, float, float]:
    clamped = max(0.0, min(10.0, score))
    for low, high in DEFAULT_SCORE_BANDS:
        if low <= clamped < high or (high == 10.0 and clamped <= high):
            return _format_band(low, high), low, high
    return "unknown", 0.0, 0.0


def _format_band(low: float, high: float) -> str:
    return f"{low:.0f}-{high:.0f}"


def _build_band_summary(
    label: str,
    low: float,
    high: float,
    samples: list[dict[str, Any]],
) -> ReplyQualityBand:
    count = len(samples)
    approval_count = sum(1 for s in samples if s["approved"])
    rejection_count = sum(1 for s in samples if s["rejected"])
    dismissal_count = sum(1 for s in samples if s["dismissed"])
    avg_score = sum(s["quality_score"] for s in samples) / count if count else 0.0
    return ReplyQualityBand(
        band=label,
        min_score=low,
        max_score=high,
        count=count,
        approval_count=approval_count,
        rejection_count=rejection_count,
        dismissal_count=dismissal_count,
        approval_rate=_rate(approval_count, count),
        rejection_rate=_rate(rejection_count, count),
        dismissal_rate=_rate(dismissal_count, count),
        average_score=round(avg_score, 2),
        common_failure_reasons=_common_failure_reasons(samples),
    )


def _common_failure_reasons(samples: list[dict[str, Any]], limit: int = 5) -> list[dict[str, Any]]:
    counter: Counter[str] = Counter()
    for sample in samples:
        if not (sample["rejected"] or sample["dismissed"]):
            continue
        counter.update(sample["failure_reasons"])
    return [
        {"reason": reason, "count": count}
        for reason, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:limit]
    ]


def _threshold_recommendation(
    bands: list[ReplyQualityBand],
    *,
    current_threshold: float | None,
    min_samples: int,
) -> ReplyThresholdRecommendation:
    high_rejection = [
        band
        for band in bands
        if band.count >= min_samples and band.rejection_rate >= 0.4
    ]
    high_approval_above_threshold = [
        band
        for band in bands
        if current_threshold is not None
        and band.min_score >= current_threshold
        and band.count >= min_samples
        and band.approval_rate >= 0.8
    ]

    if high_rejection:
        recommended = min(band.max_score for band in high_rejection)
        action = "raise" if current_threshold is None or recommended > current_threshold else "monitor"
        rationale = (
            "One or more score bands meet the minimum sample size and have rejection rates "
            "of at least 40%."
        )
    elif high_approval_above_threshold:
        recommended = current_threshold
        action = "keep"
        rationale = "Bands at or above the current threshold have strong approval rates."
    else:
        recommended = current_threshold
        action = "collect_more_data"
        rationale = "No score band has enough evidence for a threshold change."

    return ReplyThresholdRecommendation(
        current_threshold=current_threshold,
        recommended_threshold=recommended,
        action=action,
        rationale=rationale,
        high_rejection_bands=[band.band for band in high_rejection],
        min_samples=min_samples,
    )


def _rate(count: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round(count / total, 4)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def reply_quality_calibration_to_dict(
    report: ReplyQualityCalibrationReport,
) -> dict[str, Any]:
    """Serialize a calibration report with stable keys and primitive values."""
    return {
        "approval_count": report.approval_count,
        "approval_rate": report.approval_rate,
        "common_failure_reasons": report.common_failure_reasons,
        "days": report.days,
        "dismissal_count": report.dismissal_count,
        "dismissal_rate": report.dismissal_rate,
        "generated_at": report.generated_at,
        "min_samples": report.min_samples,
        "rejection_count": report.rejection_count,
        "rejection_rate": report.rejection_rate,
        "reviewed_count": report.reviewed_count,
        "sample_count": report.sample_count,
        "score_bands": [asdict(band) for band in report.score_bands],
        "status": "ok" if report.sample_count else "empty",
        "threshold_recommendation": asdict(report.threshold_recommendation),
    }


def format_reply_quality_calibration_json(
    report: ReplyQualityCalibrationReport,
) -> str:
    """Format the report as stable JSON."""
    return json.dumps(reply_quality_calibration_to_dict(report), indent=2, sort_keys=True)


def format_reply_quality_calibration_markdown(
    report: ReplyQualityCalibrationReport,
) -> str:
    """Format the report as readable Markdown."""
    data = reply_quality_calibration_to_dict(report)
    lines = [
        "# Reply Quality Calibration Report",
        "",
        f"- Lookback: {report.days} days",
        f"- Min samples: {report.min_samples}",
        f"- Samples: {report.sample_count}",
        f"- Reviewed: {report.reviewed_count}",
        f"- Approval rate: {_pct(report.approval_rate)}",
        f"- Rejection rate: {_pct(report.rejection_rate)}",
        f"- Dismissal rate: {_pct(report.dismissal_rate)}",
        "",
        "## Score Bands",
        "",
        "| Band | Count | Avg score | Approval | Rejection | Dismissal | Common failure reasons |",
        "| --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for band in report.score_bands:
        reasons = ", ".join(
            f"{item['reason']} ({item['count']})"
            for item in band.common_failure_reasons
        ) or "-"
        lines.append(
            f"| {band.band} | {band.count} | {band.average_score:.2f} | "
            f"{_pct(band.approval_rate)} | {_pct(band.rejection_rate)} | "
            f"{_pct(band.dismissal_rate)} | {reasons} |"
        )

    lines.extend(["", "## Common Failure Reasons", ""])
    if report.common_failure_reasons:
        for item in report.common_failure_reasons:
            lines.append(f"- {item['reason']}: {item['count']}")
    else:
        lines.append("- None")

    rec = report.threshold_recommendation
    lines.extend(
        [
            "",
            "## Threshold Recommendation",
            "",
            f"- Current threshold: {_score_or_na(rec.current_threshold)}",
            f"- Recommended threshold: {_score_or_na(rec.recommended_threshold)}",
            f"- Action: {rec.action}",
            f"- High rejection bands: {', '.join(rec.high_rejection_bands) or 'none'}",
            f"- Rationale: {rec.rationale}",
        ]
    )
    if data["status"] == "empty":
        lines.extend(["", "No scored reply drafts matched the requested window."])
    return "\n".join(lines)


def _pct(rate: float) -> str:
    return f"{rate * 100:.1f}%"


def _score_or_na(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.1f}"
