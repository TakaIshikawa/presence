"""Select reply follow-up reminder candidates."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


LOW_QUALITY_FLAGS = {
    "generic",
    "low_quality",
    "spam",
    "sycophantic",
    "too_long",
    "unsafe",
}


@dataclass(frozen=True)
class ReplyFollowupCandidate:
    target_handle: str
    source_type: str
    source_id: int
    due_at: str
    reason: str
    notes: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "target_handle": self.target_handle,
            "source_type": self.source_type,
            "source_id": self.source_id,
            "due_at": self.due_at,
            "reason": self.reason,
            "notes": self.notes,
        }


@dataclass(frozen=True)
class ReplyFollowupPolicy:
    min_quality_score: float = 7.0
    min_relevance_score: float = 0.65
    source_lookback_days: int = 14
    target_cooldown_days: int = 14
    due_in_days: int = 7
    limit: int = 25


def _parse_json_object(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_json_list(value: str | None) -> list[str]:
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return [str(value)]
    if not isinstance(parsed, list):
        return [str(value)]
    return [str(item) for item in parsed]


def _has_low_quality_flags(row: dict[str, Any]) -> bool:
    flags = {flag.lower() for flag in _parse_json_list(row.get("quality_flags"))}
    return bool(flags & LOW_QUALITY_FLAGS)


def _relationship_score(context: dict[str, Any]) -> float:
    score = 0.0
    if context.get("is_known"):
        score += 1.0
    try:
        score += float(context.get("relationship_strength") or 0) * 2
    except (TypeError, ValueError):
        pass
    try:
        stage = int(context.get("engagement_stage") or 0)
    except (TypeError, ValueError):
        stage = 0
    if stage >= 2:
        score += 1.0
    try:
        tier = int(context.get("dunbar_tier") or 99)
    except (TypeError, ValueError):
        tier = 99
    if tier <= 3:
        score += 1.0
    return score


def _relationship_reason(context: dict[str, Any]) -> str | None:
    if not context:
        return None
    parts = []
    stage = context.get("stage_name") or context.get("engagement_stage")
    tier = context.get("tier_name") or context.get("dunbar_tier")
    if stage:
        parts.append(f"stage={stage}")
    if tier:
        parts.append(f"tier={tier}")
    if context.get("is_known"):
        parts.append("known contact")
    return ", ".join(parts) if parts else None


def _quality_threshold(row: dict[str, Any], policy: ReplyFollowupPolicy) -> float:
    if row.get("source_type") == "proactive_actions":
        return policy.min_relevance_score
    return policy.min_quality_score


def _is_high_quality(row: dict[str, Any], policy: ReplyFollowupPolicy) -> bool:
    if _has_low_quality_flags(row):
        return False
    score = row.get("quality_score")
    if score is None:
        return False
    try:
        return float(score) >= _quality_threshold(row, policy)
    except (TypeError, ValueError):
        return False


def _is_relationship_worthy(row: dict[str, Any]) -> bool:
    context = _parse_json_object(row.get("relationship_context"))
    if _relationship_score(context) >= 1.0:
        return True
    return (row.get("priority") or "").lower() == "high"


def _normalize_handle(handle: str | None) -> str:
    return (handle or "").strip().lstrip("@")


def select_reply_followup_candidates(
    db,
    *,
    policy: ReplyFollowupPolicy | None = None,
    now: datetime | None = None,
) -> list[ReplyFollowupCandidate]:
    """Select approved/posted reply sources that deserve a future follow-up."""
    policy = policy or ReplyFollowupPolicy()
    now = now or datetime.now(timezone.utc)
    due_at = (now + timedelta(days=policy.due_in_days)).isoformat()
    rows = db.get_reply_followup_source_candidates(
        lookback_days=policy.source_lookback_days,
        limit=policy.limit * 4,
        now=now,
    )
    candidates: list[ReplyFollowupCandidate] = []
    seen_targets: set[str] = set()

    for row in rows:
        handle = _normalize_handle(row.get("target_handle"))
        if not handle:
            continue
        normalized = handle.lower()
        if normalized in seen_targets:
            continue
        if not _is_high_quality(row, policy):
            continue
        if not _is_relationship_worthy(row):
            continue
        if db.count_recent_reply_followups_to_target(
            handle,
            policy.target_cooldown_days,
            now=now,
        ):
            continue

        context = _parse_json_object(row.get("relationship_context"))
        rel_reason = _relationship_reason(context)
        score = row.get("quality_score")
        reason = f"High-value {row.get('status')} reply"
        if score is not None:
            reason += f" (score {float(score):.1f})"
        if rel_reason:
            reason += f"; {rel_reason}"

        candidates.append(
            ReplyFollowupCandidate(
                target_handle=handle,
                source_type=row["source_type"],
                source_id=int(row["source_id"]),
                due_at=due_at,
                reason=reason,
                notes=None,
            )
        )
        seen_targets.add(normalized)
        if len(candidates) >= policy.limit:
            break

    return candidates


def create_reply_followup_reminders(
    db,
    *,
    policy: ReplyFollowupPolicy | None = None,
    now: datetime | None = None,
    notes: str | None = None,
) -> list[dict[str, Any]]:
    """Insert selected reminders and return inserted rows with duplicate status."""
    inserted: list[dict[str, Any]] = []
    for candidate in select_reply_followup_candidates(db, policy=policy, now=now):
        item = candidate.to_dict()
        if notes and item["notes"] is None:
            item["notes"] = notes
        reminder_id = db.insert_reply_followup_reminder(**item)
        item["id"] = reminder_id
        item["inserted"] = reminder_id is not None
        inserted.append(item)
    return inserted
