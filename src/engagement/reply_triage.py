"""Deterministic triage scoring for inbound reply drafts."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class ReplyTriage:
    """Computed priority signal for a reply queue item."""

    score: float
    reason: str


PRIORITY_POINTS = {
    "high": 35.0,
    "normal": 20.0,
    "low": 5.0,
}

INTENT_POINTS = {
    "bug_report": 30.0,
    "question": 24.0,
    "disagreement": 20.0,
    "appreciation": 8.0,
    "other": 4.0,
    "spam": -25.0,
}


def score_reply_triage(reply: dict[str, Any], now: datetime | None = None) -> ReplyTriage:
    """Score a pending reply for review ordering.

    The score is derived only from fields already stored in reply_queue. Higher
    scores should be reviewed first because they represent either urgency
    (aging/high-priority items) or opportunity (strong relationships/questions).
    """

    now = _as_aware_utc(now or datetime.now(timezone.utc))
    contributions: list[tuple[str, float]] = []

    priority = _normalized_text(reply.get("priority"), "normal")
    contributions.append((f"{priority} priority", PRIORITY_POINTS.get(priority, 12.0)))

    intent = _normalized_text(reply.get("intent"), "other")
    contributions.append((intent.replace("_", " "), INTENT_POINTS.get(intent, 4.0)))

    age_hours = _age_hours(reply.get("detected_at"), now)
    if age_hours is not None:
        contributions.append((_age_label(age_hours), min(30.0, age_hours * 1.25)))

    relationship_points, relationship_label = _relationship_signal(
        reply.get("relationship_context")
    )
    if relationship_points:
        contributions.append((relationship_label, relationship_points))

    quality = _float_or_none(reply.get("quality_score"))
    if quality is not None:
        quality_points = max(-15.0, min(15.0, (quality - 5.0) * 3.0))
        contributions.append((f"quality {quality:.1f}/10", quality_points))

    metadata_points, metadata_label = _metadata_signal(reply.get("platform_metadata"))
    if metadata_points:
        contributions.append((metadata_label, metadata_points))

    score = round(sum(points for _, points in contributions), 1)
    reason = _concise_reason(contributions)
    return ReplyTriage(score=score, reason=reason)


def score_pending_replies(
    replies: list[dict[str, Any]],
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Attach triage_score and triage_reason to reply rows."""

    now = now or datetime.now(timezone.utc)
    scored = []
    for reply in replies:
        item = dict(reply)
        triage = score_reply_triage(item, now=now)
        item["triage_score"] = triage.score
        item["triage_reason"] = triage.reason
        scored.append(item)
    return scored


def sort_by_triage(replies: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return replies ordered by triage score with deterministic tie-breaks."""

    return sorted(
        replies,
        key=lambda reply: (
            -float(reply.get("triage_score") or 0.0),
            _detected_sort_key(reply.get("detected_at")),
            int(reply.get("id") or 0),
        ),
    )


def _relationship_signal(raw: Any) -> tuple[float, str]:
    context = _parse_json_object(raw)
    if not context:
        return 0.0, ""

    points = 0.0
    labels = []
    strength = _float_or_none(context.get("relationship_strength"))
    if strength is not None:
        points += max(0.0, min(1.0, strength)) * 20.0
        labels.append(f"strength {strength:.2f}")

    stage = _float_or_none(context.get("engagement_stage"))
    if stage is not None:
        points += max(0.0, min(5.0, stage)) * 3.0
        stage_name = context.get("stage_name")
        labels.append(str(stage_name) if stage_name else f"stage {stage:g}")

    tier = _float_or_none(context.get("dunbar_tier"))
    if tier is not None:
        points += max(0.0, 15.0 - (tier * 3.0))
        tier_name = context.get("tier_name")
        labels.append(str(tier_name) if tier_name else f"tier {tier:g}")

    if not labels:
        return 0.0, ""
    return round(points, 2), "relationship " + "/".join(labels[:2])


def _metadata_signal(raw: Any) -> tuple[float, str]:
    metadata = _parse_json_object(raw)
    if not metadata:
        return 0.0, ""

    points = 0.0
    labels = []
    if metadata.get("quoted_tweet_id") or metadata.get("quoted_text"):
        points += 5.0
        labels.append("quote context")
    if metadata.get("parent_post_text"):
        points += 3.0
        labels.append("thread context")
    reply_refs = metadata.get("reply_refs")
    if isinstance(reply_refs, list) and len(reply_refs) > 1:
        points += 2.0
        labels.append("reply chain")
    if metadata.get("reason") == "mention":
        points += 2.0
        labels.append("direct mention")

    if not labels:
        return 0.0, ""
    return points, "/".join(labels[:2])


def _concise_reason(contributions: list[tuple[str, float]]) -> str:
    positive = [(label, points) for label, points in contributions if points > 0]
    base = [item for item in positive[:2]]
    ranked = sorted(positive[2:], key=lambda item: item[1], reverse=True)
    selected = base + ranked
    if not ranked:
        selected = positive
    if not selected:
        return "low triage signal"
    return "; ".join(label for label, _ in selected[:3])


def _parse_json_object(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _normalized_text(value: Any, default: str) -> str:
    text = str(value or default).strip().lower()
    return text or default


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _age_hours(raw: Any, now: datetime) -> float | None:
    detected = _parse_datetime(raw)
    if detected is None:
        return None
    return max(0.0, (_as_aware_utc(now) - _as_aware_utc(detected)).total_seconds() / 3600)


def _parse_datetime(raw: Any) -> datetime | None:
    if isinstance(raw, datetime):
        return raw
    if not raw:
        return None
    text = str(raw).strip()
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        pass
    try:
        return datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def _as_aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _age_label(age_hours: float) -> str:
    if age_hours >= 24:
        return f"{age_hours / 24:.1f}d old"
    return f"{age_hours:.1f}h old"


def _detected_sort_key(raw: Any) -> str:
    detected = _parse_datetime(raw)
    if detected is None:
        return ""
    return _as_aware_utc(detected).isoformat()
