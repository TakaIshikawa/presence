"""Helpers for stable publication failure categories."""

from __future__ import annotations

from typing import Literal


PublishErrorCategory = Literal[
    "auth",
    "rate_limit",
    "duplicate",
    "media",
    "network",
    "unknown",
]

KNOWN_ERROR_CATEGORIES: tuple[PublishErrorCategory, ...] = (
    "auth",
    "rate_limit",
    "duplicate",
    "media",
    "network",
    "unknown",
)


def normalize_error_category(category: object) -> PublishErrorCategory:
    """Return a known publication error category."""
    if isinstance(category, str) and category in KNOWN_ERROR_CATEGORIES:
        return category  # type: ignore[return-value]
    return "unknown"


def classify_publish_error(
    error: object,
    platform: str | None = None,
) -> PublishErrorCategory:
    """Classify a publish failure into a stable category for persistence."""
    text = str(error or "").lower()
    if not text:
        return "unknown"

    if _contains_any(
        text,
        (
            "429",
            "too many requests",
            "rate limit",
            "ratelimit",
            "rate-limit",
            "throttl",
        ),
    ):
        return "rate_limit"

    if _contains_any(
        text,
        (
            "duplicate",
            "already exists",
            "already posted",
            "status is a duplicate",
            "tweet needs to be a bit more unique",
        ),
    ):
        return "duplicate"

    if _contains_any(
        text,
        (
            "media",
            "image",
            "video",
            "upload",
            "alt text",
            "unsupported file",
            "file size",
            "invalid file",
        ),
    ):
        return "media"

    if _contains_any(
        text,
        (
            "401",
            "402",
            "403",
            "unauthorized",
            "forbidden",
            "authentication",
            "authenticate",
            "authorization",
            "invalid token",
            "expired token",
            "invalid credentials",
            "bad password",
            "app password",
            "not configured",
            "does not have any credits",
            "no credits",
            "payment required",
        ),
    ):
        return "auth"

    if _contains_any(
        text,
        (
            "timeout",
            "timed out",
            "connection",
            "network",
            "dns",
            "ssl",
            "temporarily unavailable",
            "service unavailable",
            "bad gateway",
            "gateway timeout",
            "502",
            "503",
            "504",
        ),
    ):
        return "network"

    return "unknown"


def _contains_any(text: str, markers: tuple[str, ...]) -> bool:
    return any(marker in text for marker in markers)
