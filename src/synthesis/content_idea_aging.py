"""Age-based escalation policy for content ideas."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class ContentIdeaAgingAction:
    idea_id: int
    action: str
    age_days: int
    reason: str
    topic: str | None
    note: str
    from_priority: str
    to_priority: str | None = None
    from_status: str = "open"
    to_status: str | None = None
    threshold_days: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "idea_id": self.idea_id,
            "action": self.action,
            "age_days": self.age_days,
            "reason": self.reason,
            "topic": self.topic,
            "note": self.note,
            "from_priority": self.from_priority,
            "to_priority": self.to_priority,
            "from_status": self.from_status,
            "to_status": self.to_status,
            "threshold_days": self.threshold_days,
        }


def age_content_ideas(
    db,
    *,
    promote_after_days: int = 30,
    dismiss_low_after_days: int = 60,
    topic: str | None = None,
    dry_run: bool = False,
    now: datetime | None = None,
) -> list[ContentIdeaAgingAction]:
    """Promote or dismiss stale open content ideas based on priority and age."""
    if promote_after_days < 0:
        raise ValueError("promote_after_days must be non-negative")
    if dismiss_low_after_days < 0:
        raise ValueError("dismiss_low_after_days must be non-negative")

    now = _ensure_aware(now or datetime.now(timezone.utc))
    normalized_topic = db._normalize_content_idea_text(topic) if topic else None
    rows = db.conn.execute(
        """SELECT *
           FROM content_ideas
           WHERE status = 'open'
           ORDER BY created_at ASC, id ASC"""
    ).fetchall()

    actions: list[ContentIdeaAgingAction] = []
    for row in rows:
        idea = dict(row)
        if normalized_topic and normalized_topic != db._normalize_content_idea_text(
            idea.get("topic")
        ):
            continue

        created_at = _parse_datetime(idea.get("created_at"))
        age_days = max(0, int((now - created_at).total_seconds() // 86400))
        priority = db._normalize_content_idea_priority(idea.get("priority"))

        action: ContentIdeaAgingAction | None = None
        if priority == "low" and age_days >= dismiss_low_after_days:
            action = ContentIdeaAgingAction(
                idea_id=int(idea["id"]),
                action="dismiss_low_priority",
                age_days=age_days,
                reason=(
                    f"low priority idea is {age_days} days old; "
                    f"dismissal threshold is {dismiss_low_after_days} days"
                ),
                topic=idea.get("topic"),
                note=idea.get("note") or "",
                from_priority=priority,
                to_priority=priority,
                to_status="dismissed",
                threshold_days=dismiss_low_after_days,
            )
        elif priority == "normal" and age_days >= promote_after_days:
            action = ContentIdeaAgingAction(
                idea_id=int(idea["id"]),
                action="promote_priority",
                age_days=age_days,
                reason=(
                    f"normal priority idea is {age_days} days old; "
                    f"promotion threshold is {promote_after_days} days"
                ),
                topic=idea.get("topic"),
                note=idea.get("note") or "",
                from_priority=priority,
                to_priority="high",
                to_status="open",
                threshold_days=promote_after_days,
            )

        if action is None:
            continue
        actions.append(action)
        if dry_run:
            continue

        db.apply_content_idea_aging_action(
            action.idea_id,
            action=_action_metadata(action, aged_at=now),
            priority=action.to_priority if action.action == "promote_priority" else None,
            status=action.to_status if action.action == "dismiss_low_priority" else None,
            updated_at=now.isoformat(),
        )

    return actions


def _action_metadata(
    action: ContentIdeaAgingAction,
    *,
    aged_at: datetime,
) -> dict[str, Any]:
    return {
        "source": "content_idea_aging",
        "action": action.action,
        "aged_at": aged_at.isoformat(),
        "age_days": action.age_days,
        "threshold_days": action.threshold_days,
        "reason": action.reason,
        "from_priority": action.from_priority,
        "to_priority": action.to_priority,
        "from_status": action.from_status,
        "to_status": action.to_status,
    }


def _parse_datetime(value: object) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    text = str(value).strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    parsed = datetime.fromisoformat(text)
    return _ensure_aware(parsed)


def _ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
