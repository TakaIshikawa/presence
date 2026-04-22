"""Shared guard for avoiding repeated X API calls while credentials are blocked."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from .publish_errors import classify_publish_error


BLOCKED_UNTIL_KEY = "x_api_blocked_until"
BLOCK_REASON_KEY = "x_api_block_reason"
DEFAULT_BLOCK_HOURS = 12


def is_x_api_block_error(error: object) -> bool:
    """Return True for account-level X failures that should stop all jobs."""
    text = str(error or "").lower()
    return any(
        marker in text
        for marker in (
            "402",
            "payment required",
            "does not have any credits",
            "no credits",
            "unauthorized",
            "invalid or expired token",
        )
    )


def get_x_api_block_reason(db, now: Optional[datetime] = None) -> Optional[str]:
    """Return the active block reason, or None when X calls are allowed."""
    raw_until = db.get_meta(BLOCKED_UNTIL_KEY)
    if not raw_until:
        return None
    if not isinstance(raw_until, str):
        return None

    now = now or datetime.now(timezone.utc)
    try:
        blocked_until = datetime.fromisoformat(raw_until)
        if blocked_until.tzinfo is None:
            blocked_until = blocked_until.replace(tzinfo=timezone.utc)
    except ValueError:
        return None

    if blocked_until <= now:
        return None

    reason = db.get_meta(BLOCK_REASON_KEY) or "X API temporarily blocked"
    return f"{reason} (until {blocked_until.isoformat()})"


def mark_x_api_blocked(
    db,
    error: object,
    hours: int = DEFAULT_BLOCK_HOURS,
    now: Optional[datetime] = None,
) -> str:
    """Persist a temporary X API block and return the blocked-until timestamp."""
    now = now or datetime.now(timezone.utc)
    blocked_until = now + timedelta(hours=hours)
    reason = str(error or "X API unavailable")
    db.set_meta(BLOCKED_UNTIL_KEY, blocked_until.isoformat())
    db.set_meta(BLOCK_REASON_KEY, reason[:500])
    return blocked_until.isoformat()


def mark_x_api_blocked_if_needed(
    db,
    error: object,
    hours: int = DEFAULT_BLOCK_HOURS,
) -> Optional[str]:
    """Mark the circuit breaker only for account-level X failures."""
    if not is_x_api_block_error(error):
        return None
    return mark_x_api_blocked(db, error, hours=hours)
