"""Shared utility functions for review scripts."""

import json
import sys


def truncate(text: str, max_len: int) -> str:
    """Truncate text with ellipsis.

    Args:
        text: The text to truncate
        max_len: Maximum length before truncation

    Returns:
        Truncated string with "..." suffix if longer than max_len, otherwise original text
    """
    if not text:
        return ""
    if len(text) <= max_len:
        return text
    return text[:max_len - 3] + "..."


def read_char() -> str:
    """Read a single character from stdin without requiring Enter.

    Returns:
        Single character string from user input
    """
    try:
        import tty
        import termios
        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        try:
            tty.setraw(fd)
            ch = sys.stdin.read(1)
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        return ch
    except (ImportError, termios.error, AttributeError, OSError):
        # Fallback for non-terminal environments
        return input().strip()[:1] if True else ""


def format_relationship_context(relationship_context_json: str | None) -> str | None:
    """Parse relationship context JSON and format for display.

    Args:
        relationship_context_json: JSON string containing relationship context fields

    Returns:
        Formatted string showing engagement stage, dunbar tier, and relationship strength,
        or None if no valid context available
    """
    if not relationship_context_json:
        return None
    try:
        ctx = json.loads(relationship_context_json)
    except (json.JSONDecodeError, TypeError):
        return None
    parts = []
    if ctx.get("engagement_stage") is not None:
        parts.append(f"{ctx.get('stage_name', '?')} (stage {ctx['engagement_stage']})")
    if ctx.get("dunbar_tier") is not None:
        parts.append(f"{ctx.get('tier_name', '?')} (tier {ctx['dunbar_tier']})")
    if ctx.get("relationship_strength") is not None:
        parts.append(f"strength: {ctx['relationship_strength']:.2f}")
    return " | ".join(parts) if parts else None
