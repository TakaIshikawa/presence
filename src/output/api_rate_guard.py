"""Shared helpers for avoiding optional API polling near rate-limit exhaustion."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


RATE_LIMIT_META_PREFIX = "api_rate_limit"


@dataclass(frozen=True)
class RateLimitState:
    service: str
    remaining: int
    reset_at: Optional[datetime] = None


def _parse_int(value: object) -> Optional[int]:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _parse_datetime(value: object) -> Optional[datetime]:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _service_key(service: str, suffix: str | None = None) -> str:
    key = f"{RATE_LIMIT_META_PREFIX}:{service}"
    return f"{key}:{suffix}" if suffix else key


def get_stored_rate_limit_state(db, service: str) -> Optional[RateLimitState]:
    """Read stored rate-limit state from DB meta.

    Supports either a JSON payload at ``api_rate_limit:<service>`` with
    ``remaining`` and optional ``reset_at`` fields, or split keys:
    ``api_rate_limit:<service>:remaining`` and ``...:reset_at``.
    """
    raw_payload = db.get_meta(_service_key(service))
    if isinstance(raw_payload, str) and raw_payload:
        try:
            payload = json.loads(raw_payload)
        except json.JSONDecodeError:
            payload = None
        if isinstance(payload, dict):
            remaining = _parse_int(payload.get("remaining"))
            if remaining is not None:
                return RateLimitState(
                    service=service,
                    remaining=remaining,
                    reset_at=_parse_datetime(payload.get("reset_at")),
                )

    remaining = _parse_int(db.get_meta(_service_key(service, "remaining")))
    if remaining is None:
        return None

    return RateLimitState(
        service=service,
        remaining=remaining,
        reset_at=_parse_datetime(db.get_meta(_service_key(service, "reset_at"))),
    )


def optional_api_skip_reason(
    config,
    db,
    service: str,
    *,
    operation: str = "optional polling",
    now: Optional[datetime] = None,
) -> Optional[str]:
    """Return a clear skip reason when optional calls should be deferred."""
    rate_limits = getattr(config, "rate_limits", None)
    threshold = _parse_int(getattr(rate_limits, f"{service}_min_remaining", None))
    if threshold is None or threshold <= 0:
        return None

    state = get_stored_rate_limit_state(db, service)
    if state is None:
        return None

    now = now or datetime.now(timezone.utc)
    if state.reset_at and state.reset_at <= now:
        return None

    if state.remaining > threshold:
        return None

    reset = f"; resets at {state.reset_at.isoformat()}" if state.reset_at else ""
    return (
        f"{service} API remaining budget {state.remaining} is at or below "
        f"configured optional-call threshold {threshold}{reset}; skipping {operation}"
    )


def should_skip_optional_api_call(
    config,
    db,
    service: str,
    *,
    operation: str = "optional polling",
    logger=None,
) -> bool:
    """Log and return True when an optional API call should be skipped."""
    reason = optional_api_skip_reason(config, db, service, operation=operation)
    if reason:
        if logger is not None:
            logger.warning(reason)
        return True
    return False
