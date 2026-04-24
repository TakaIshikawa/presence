"""Cooldown guards for proactive engagement actions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable


DEFAULT_AUTHOR_COOLDOWN_HOURS = 72
DEFAULT_TARGET_COOLDOWN_HOURS = 168
COOLDOWN_ACTION_TYPES = ("like", "retweet", "reply", "quote_tweet")
AUTHOR_BLOCKING_STATUSES = ("posted", "approved")
TARGET_BLOCKING_STATUSES = ("pending", "posted", "approved")


@dataclass(frozen=True)
class ProactiveCooldownPolicy:
    """Cooldown windows used while reviewing proactive actions."""

    author_cooldown_hours: int = DEFAULT_AUTHOR_COOLDOWN_HOURS
    target_cooldown_hours: int = DEFAULT_TARGET_COOLDOWN_HOURS


@dataclass(frozen=True)
class ProactiveCooldownResult:
    """Cooldown evaluation result for one proactive action."""

    blocked: bool
    reasons: tuple[str, ...] = ()
    author_conflicts: tuple[dict, ...] = ()
    target_conflicts: tuple[dict, ...] = ()

    @property
    def reason(self) -> str | None:
        return "; ".join(self.reasons) if self.reasons else None


def normalize_handle(handle: str | None) -> str:
    """Normalize an X handle for case-insensitive comparisons."""
    return (handle or "").lstrip("@").lower()


def _cutoff(hours: int, now: datetime | None) -> str:
    current = now or datetime.now(timezone.utc)
    return (current - timedelta(hours=hours)).isoformat()


def _placeholders(values: Iterable[object]) -> str:
    return ",".join("?" for _ in values)


def find_author_conflicts(
    db,
    *,
    target_author_handle: str | None,
    current_action_id: int | None = None,
    cooldown_hours: int,
    now: datetime | None = None,
) -> list[dict]:
    """Return recent posted/approved actions against the same author."""
    handle = normalize_handle(target_author_handle)
    if not handle or cooldown_hours <= 0:
        return []

    statuses = AUTHOR_BLOCKING_STATUSES
    action_types = COOLDOWN_ACTION_TYPES
    params: list[object] = [
        handle,
        *statuses,
        *action_types,
        _cutoff(cooldown_hours, now),
    ]
    exclude_sql = ""
    if current_action_id is not None:
        exclude_sql = "AND id != ?"
        params.append(current_action_id)

    cursor = db.conn.execute(
        f"""SELECT *
            FROM proactive_actions
            WHERE LOWER(LTRIM(target_author_handle, '@')) = ?
              AND status IN ({_placeholders(statuses)})
              AND action_type IN ({_placeholders(action_types)})
              AND datetime(COALESCE(posted_at, reviewed_at, created_at)) >= datetime(?)
              {exclude_sql}
            ORDER BY COALESCE(posted_at, reviewed_at, created_at) DESC, id DESC""",
        params,
    )
    return [dict(row) for row in cursor.fetchall()]


def find_target_conflicts(
    db,
    *,
    target_tweet_id: str | None,
    current_action_id: int | None = None,
    cooldown_hours: int,
    now: datetime | None = None,
) -> list[dict]:
    """Return recent actions against the same target tweet."""
    if not target_tweet_id or cooldown_hours <= 0:
        return []

    statuses = TARGET_BLOCKING_STATUSES
    action_types = COOLDOWN_ACTION_TYPES
    params: list[object] = [
        target_tweet_id,
        *statuses,
        *action_types,
        _cutoff(cooldown_hours, now),
    ]
    exclude_sql = ""
    if current_action_id is not None:
        exclude_sql = "AND id != ?"
        params.append(current_action_id)

    cursor = db.conn.execute(
        f"""SELECT *
            FROM proactive_actions
            WHERE target_tweet_id = ?
              AND status IN ({_placeholders(statuses)})
              AND action_type IN ({_placeholders(action_types)})
              AND datetime(COALESCE(posted_at, reviewed_at, created_at)) >= datetime(?)
              {exclude_sql}
            ORDER BY COALESCE(posted_at, reviewed_at, created_at) DESC, id DESC""",
        params,
    )
    rows = [dict(row) for row in cursor.fetchall()]
    if current_action_id is None:
        return rows

    # Two pending actions on the same target should not both block each other.
    # Keep the older pending row as the survivor and block newer pending rows.
    return [
        row
        for row in rows
        if row.get("status") != "pending" or int(row["id"]) < current_action_id
    ]


def evaluate_proactive_cooldown(
    db,
    action: dict,
    policy: ProactiveCooldownPolicy,
    *,
    now: datetime | None = None,
) -> ProactiveCooldownResult:
    """Evaluate whether a normalized proactive action is blocked by cooldowns."""
    if action.get("action_type") not in COOLDOWN_ACTION_TYPES:
        return ProactiveCooldownResult(blocked=False)

    current_id = action.get("id") if action.get("source") == "presence" else None
    if not isinstance(current_id, int):
        current_id = None

    author_conflicts = find_author_conflicts(
        db,
        target_author_handle=action.get("target_handle"),
        current_action_id=current_id,
        cooldown_hours=max(0, policy.author_cooldown_hours),
        now=now,
    )
    target_conflicts = find_target_conflicts(
        db,
        target_tweet_id=action.get("target_tweet_id"),
        current_action_id=current_id,
        cooldown_hours=max(0, policy.target_cooldown_hours),
        now=now,
    )

    reasons: list[str] = []
    if author_conflicts:
        handle = (action.get("target_handle") or "").lstrip("@")
        count = len(author_conflicts)
        reasons.append(
            f"@{handle} has {count} recent posted/approved proactive action"
            f"{'s' if count != 1 else ''} in the last "
            f"{policy.author_cooldown_hours} hours"
        )

    if target_conflicts:
        count = len(target_conflicts)
        reasons.append(
            f"target tweet has {count} recent proactive action"
            f"{'s' if count != 1 else ''} in the last "
            f"{policy.target_cooldown_hours} hours"
        )

    return ProactiveCooldownResult(
        blocked=bool(reasons),
        reasons=tuple(reasons),
        author_conflicts=tuple(author_conflicts),
        target_conflicts=tuple(target_conflicts),
    )
