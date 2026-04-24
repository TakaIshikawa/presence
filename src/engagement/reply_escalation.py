"""Recommend review actions for pending reply drafts."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


REVIEW_NOW = "review_now"
REVISE = "revise"
DISMISS = "dismiss"
WAIT = "wait"

LOW_QUALITY_THRESHOLD = 6.0
DISMISS_QUALITY_THRESHOLD = 3.0
DISMISS_FLAGS = {"sycophantic"}
REVISE_FLAGS = {"generic", "stage_mismatch", "parse_error", "eval_error"}


@dataclass(frozen=True)
class ReplyEscalation:
    """A review recommendation for one reply draft."""

    draft_id: int
    target: str | None
    age_hours: float
    recommendation: str
    reasons: list[str]
    priority: str
    platform: str
    quality_score: float | None = None
    quality_flags: list[str] | None = None
    relationship_context: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "draft_id": self.draft_id,
            "target": self.target,
            "age_hours": self.age_hours,
            "recommendation": self.recommendation,
            "reasons": self.reasons,
            "priority": self.priority,
            "platform": self.platform,
            "quality_score": self.quality_score,
            "quality_flags": self.quality_flags or [],
            "relationship_context": self.relationship_context,
        }


def recommend_reply_escalations(
    rows: list[dict[str, Any]],
    *,
    min_age_hours: float,
    include_low_priority: bool = False,
    now: datetime | None = None,
) -> list[ReplyEscalation]:
    """Build recommendations for pending reply drafts.

    Rows are expected to come from reply_queue or Database.get_pending_reply_sla().
    Low-priority rows are excluded by default for a focused reviewer queue.
    """
    if min_age_hours < 0:
        raise ValueError("min_age_hours must be non-negative")

    recommendations = []
    for row in rows:
        priority = row.get("priority") or "normal"
        if priority == "low" and not include_low_priority:
            continue
        if row.get("status") not in (None, "pending"):
            continue

        age_hours = _row_age_hours(row, now)
        flags = _parse_flags(row.get("quality_flags"))
        relationship = _relationship_label(row.get("relationship_context"))
        recommendation, reasons = _recommend_action(
            age_hours=age_hours,
            min_age_hours=min_age_hours,
            quality_score=row.get("quality_score"),
            flags=flags,
            priority=priority,
            relationship=relationship,
        )
        recommendations.append(
            ReplyEscalation(
                draft_id=int(row["id"]),
                target=row.get("inbound_author_handle") or row.get("target_author_handle"),
                age_hours=round(age_hours, 2),
                recommendation=recommendation,
                reasons=reasons,
                priority=priority,
                platform=row.get("platform") or "x",
                quality_score=row.get("quality_score"),
                quality_flags=flags,
                relationship_context=relationship,
            )
        )

    recommendations.sort(
        key=lambda item: (
            _recommendation_rank(item.recommendation),
            _priority_rank(item.priority),
            -item.age_hours,
            item.draft_id,
        )
    )
    return recommendations


def recommendations_to_jsonable(
    recommendations: list[ReplyEscalation],
    *,
    min_age_hours: float,
    include_low_priority: bool,
) -> dict[str, Any]:
    """Return stable JSON-ready output for the CLI."""
    return {
        "filters": {
            "min_age_hours": min_age_hours,
            "include_low_priority": include_low_priority,
        },
        "total": len(recommendations),
        "drafts": [item.to_dict() for item in recommendations],
    }


def _recommend_action(
    *,
    age_hours: float,
    min_age_hours: float,
    quality_score: float | None,
    flags: list[str],
    priority: str,
    relationship: str | None,
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    flag_set = set(flags)

    if DISMISS_FLAGS & flag_set:
        reasons.append("quality flag: sycophantic")
        return DISMISS, reasons

    if quality_score is not None and quality_score <= DISMISS_QUALITY_THRESHOLD:
        reasons.append(f"quality score {quality_score:.1f}/10")
        return DISMISS, reasons

    revise_flags = sorted(REVISE_FLAGS & flag_set)
    if revise_flags:
        reasons.append("quality flags: " + ", ".join(revise_flags))
        return REVISE, reasons

    if quality_score is not None and quality_score < LOW_QUALITY_THRESHOLD:
        reasons.append(f"quality score {quality_score:.1f}/10")
        return REVISE, reasons

    if age_hours >= min_age_hours:
        reasons.append(f"older than {min_age_hours:g}h threshold")
        if priority == "high":
            reasons.append("high priority")
        if relationship:
            reasons.append(f"relationship: {relationship}")
        return REVIEW_NOW, reasons

    reasons.append(f"younger than {min_age_hours:g}h threshold")
    return WAIT, reasons


def _parse_flags(flags_json: Any) -> list[str]:
    if not flags_json:
        return []
    if isinstance(flags_json, list):
        return [str(flag) for flag in flags_json]
    try:
        parsed = json.loads(flags_json)
    except (json.JSONDecodeError, TypeError):
        return []
    if not isinstance(parsed, list):
        return []
    return [str(flag) for flag in parsed]


def _relationship_label(relationship_context_json: Any) -> str | None:
    if not relationship_context_json:
        return None
    try:
        context = json.loads(relationship_context_json)
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(context, dict):
        return None

    parts = []
    stage_name = context.get("stage_name")
    stage = context.get("engagement_stage")
    if stage_name and stage is not None:
        parts.append(f"{stage_name} stage {stage}")
    elif stage_name:
        parts.append(str(stage_name))

    tier_name = context.get("tier_name")
    tier = context.get("dunbar_tier")
    if tier_name and tier is not None:
        parts.append(f"{tier_name} tier {tier}")
    elif tier_name:
        parts.append(str(tier_name))

    return " | ".join(parts) or None


def _row_age_hours(row: dict[str, Any], now: datetime | None) -> float:
    if row.get("age_hours") is not None:
        return max(float(row["age_hours"]), 0.0)
    detected_at = row.get("detected_at")
    if not detected_at:
        return 0.0

    try:
        detected = datetime.fromisoformat(str(detected_at).replace("Z", "+00:00"))
    except ValueError:
        try:
            detected = datetime.strptime(str(detected_at), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return 0.0
    if detected.tzinfo is None:
        detected = detected.replace(tzinfo=timezone.utc)

    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    age = (current.astimezone(timezone.utc) - detected.astimezone(timezone.utc)).total_seconds() / 3600
    return max(age, 0.0)


def _recommendation_rank(recommendation: str) -> int:
    return {
        DISMISS: 0,
        REVISE: 1,
        REVIEW_NOW: 2,
        WAIT: 3,
    }.get(recommendation, 4)


def _priority_rank(priority: str) -> int:
    return {
        "high": 0,
        "normal": 1,
        "low": 2,
    }.get(priority, 3)
