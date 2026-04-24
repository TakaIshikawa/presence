"""Deterministic priority scoring for queued reply drafts."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

_TOKEN_RE = re.compile(r"[a-z0-9']+")

_URGENT_PHRASES = (
    "urgent",
    "asap",
    "right now",
    "security",
    "vulnerability",
    "broken",
    "crash",
    "crashes",
    "error",
    "exception",
    "not working",
    "doesn't work",
    "doesnt work",
    "fails",
    "failure",
    "regression",
    "repro",
)

_LOW_VALUE_FLAGS = {
    "generic",
    "sycophantic",
    "too_long",
    "unsupported_claim",
    "unsafe",
    "spam",
}

_INTENT_WEIGHTS = {
    "bug_report": 35,
    "question": 24,
    "disagreement": 18,
    "other": 8,
    "appreciation": 4,
    "spam": -35,
}

_STORED_PRIORITY_WEIGHTS = {
    "high": 12,
    "normal": 0,
    "low": -12,
}


@dataclass(frozen=True)
class ReplyPriorityScore:
    """Computed priority for a queued reply."""

    score: int
    label: str
    reasons: tuple[str, ...]

    @property
    def sort_label(self) -> tuple[int, str]:
        """Stable grouping key for tests and callers."""
        return (-self.score, self.label)


def score_reply_priority(
    reply: dict[str, Any],
    *,
    now: datetime | None = None,
) -> ReplyPriorityScore:
    """Score a queued reply for review ordering.

    Scores are deterministic and intentionally transparent. The database row is
    not mutated; callers can sort on the returned score while preserving the
    existing review status flow.
    """

    now = now or datetime.now(timezone.utc)
    score = 50
    reasons: list[str] = []

    intent = str(reply.get("intent") or "other").lower()
    score += _add_reason(reasons, "intent:%s" % intent, _INTENT_WEIGHTS.get(intent, 8))

    stored_priority = str(reply.get("priority") or "normal").lower()
    score += _add_reason(
        reasons,
        "stored_priority:%s" % stored_priority,
        _STORED_PRIORITY_WEIGHTS.get(stored_priority, 0),
    )

    relationship_delta = _relationship_delta(_parse_json_object(reply.get("relationship_context")))
    score += _add_reason(reasons, "relationship", relationship_delta)

    text_delta = _text_signal_delta(str(reply.get("inbound_text") or ""))
    score += _add_reason(reasons, "text_signals", text_delta)

    metadata_delta = _platform_metadata_delta(_parse_json_object(reply.get("platform_metadata")))
    score += _add_reason(reasons, "platform_context", metadata_delta)

    age_delta = _age_delta(reply.get("detected_at"), now)
    score += _add_reason(reasons, "age", age_delta)

    quality_delta = _quality_delta(reply.get("quality_score"), _parse_json_list(reply.get("quality_flags")))
    score += _add_reason(reasons, "quality", quality_delta)

    bounded = max(0, min(100, score))
    return ReplyPriorityScore(
        score=bounded,
        label=_label_for_score(bounded),
        reasons=tuple(reasons),
    )


def prioritize_replies(
    replies: list[dict[str, Any]],
    *,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Return copies of replies sorted by computed priority, then age and id."""

    scored: list[dict[str, Any]] = []
    for reply in replies:
        enriched = dict(reply)
        enriched["computed_priority"] = score_reply_priority(reply, now=now)
        scored.append(enriched)

    return sorted(
        scored,
        key=lambda row: (
            -row["computed_priority"].score,
            _detected_at_sort_value(row.get("detected_at")),
            int(row.get("id") or 0),
        ),
    )


def _add_reason(reasons: list[str], name: str, delta: int) -> int:
    if delta:
        reasons.append("%s:%+d" % (name, delta))
    return delta


def _relationship_delta(context: dict[str, Any]) -> int:
    delta = 0
    stage = _as_float(context.get("engagement_stage"))
    if stage is not None:
        delta += int(min(12, max(0, stage) * 3))

    tier = _as_float(context.get("dunbar_tier"))
    if tier is not None:
        if tier <= 1:
            delta += 14
        elif tier <= 2:
            delta += 10
        elif tier <= 3:
            delta += 6
        else:
            delta += 2

    strength = _as_float(context.get("relationship_strength"))
    if strength is not None:
        delta += int(max(0, min(1, strength)) * 10)

    if context.get("is_known") is True:
        delta += 4
    return min(delta, 24)


def _text_signal_delta(text: str) -> int:
    normalized = " ".join(text.lower().split())
    if not normalized:
        return -8

    delta = 0
    tokens = _TOKEN_RE.findall(normalized)
    if "?" in text:
        delta += 7
    if any(phrase in normalized for phrase in _URGENT_PHRASES):
        delta += 14
    if len(tokens) >= 30:
        delta += 4
    if len(tokens) <= 3:
        delta -= 6
    if "thanks" in tokens or "thank" in tokens:
        delta -= 3
    return delta


def _platform_metadata_delta(metadata: dict[str, Any]) -> int:
    if not metadata:
        return 0

    delta = 0
    depth = _as_float(metadata.get("conversation_depth") or metadata.get("reply_depth"))
    if depth is not None:
        delta += int(min(8, max(0, depth) * 2))
    if metadata.get("reply_root") or metadata.get("parent_post_uri"):
        delta += 3
    if metadata.get("mentions_our_handle") is True:
        delta += 4
    if metadata.get("thread_has_multiple_participants") is True:
        delta += 4
    return delta


def _age_delta(detected_at: Any, now: datetime) -> int:
    detected = _parse_datetime(detected_at)
    if detected is None:
        return 0
    if detected.tzinfo is None:
        detected = detected.replace(tzinfo=timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    age_hours = max(0.0, (now - detected).total_seconds() / 3600)
    if age_hours >= 72:
        return -8
    if age_hours >= 24:
        return -2
    if age_hours >= 4:
        return 5
    return 8


def _quality_delta(score: Any, flags: list[Any]) -> int:
    delta = 0
    quality_score = _as_float(score)
    if quality_score is not None:
        if quality_score >= 8:
            delta += 6
        elif quality_score >= 6:
            delta += 2
        elif quality_score < 4:
            delta -= 10

    normalized_flags = {str(flag).lower() for flag in flags}
    low_value_count = len(normalized_flags & _LOW_VALUE_FLAGS)
    delta -= low_value_count * 6
    return delta


def _label_for_score(score: int) -> str:
    if score >= 80:
        return "urgent"
    if score >= 62:
        return "high"
    if score >= 38:
        return "normal"
    return "low"


def _parse_json_object(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_json_list(raw: Any) -> list[Any]:
    if isinstance(raw, list):
        return raw
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return []
    return parsed if isinstance(parsed, list) else []


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        pass
    try:
        return datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _detected_at_sort_value(value: Any) -> str:
    detected = _parse_datetime(value)
    if detected is None:
        return ""
    return detected.isoformat()
