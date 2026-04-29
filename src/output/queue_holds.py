"""Review and resolve held publish queue items."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


PREVIEW_WIDTH = 96


def parse_iso_timestamp(value: str) -> str:
    """Validate and normalize an ISO timestamp."""
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"invalid ISO timestamp: {value}") from exc
    return parsed.isoformat()


def content_preview(content: str | None, width: int = PREVIEW_WIDTH) -> str:
    """Return a compact single-line content preview."""
    text = " ".join((content or "").split())
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


def _parse_db_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        try:
            parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def age_label(created_at: str | None, *, now: datetime | None = None) -> str:
    """Return a stable coarse age label for a queue item."""
    created = _parse_db_timestamp(created_at)
    if created is None:
        return "unknown"
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    seconds = max(0, int((current.astimezone(timezone.utc) - created).total_seconds()))
    days, remainder = divmod(seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes = remainder // 60
    if days:
        return f"{days}d {hours}h"
    if hours:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def held_item_record(row: dict[str, Any], *, now: datetime | None = None) -> dict[str, Any]:
    """Convert a database row to the public review shape."""
    return {
        "queue_id": row["id"],
        "platform": row["platform"],
        "scheduled_at": row["scheduled_at"],
        "hold_reason": row.get("hold_reason"),
        "content_preview": content_preview(row.get("content")),
        "age": age_label(row.get("created_at"), now=now),
    }


def report_held_items(
    db: Any,
    *,
    before: str | None = None,
    reason_match: str | None = None,
    platform: str | None = None,
    limit: int = 50,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Return held queue items with stable report keys."""
    rows = db.get_held_publish_queue_items(
        before=before,
        reason_match=reason_match,
        platform=platform,
        limit=limit,
    )
    return [held_item_record(row, now=now) for row in rows]


def format_held_items(records: list[dict[str, Any]]) -> str:
    """Format held queue records as a compact operator table."""
    if not records:
        return "No held publish queue items found."

    columns = [
        ("queue_id", "ID", 5),
        ("platform", "PLATFORM", 8),
        ("scheduled_at", "SCHEDULED", 25),
        ("age", "AGE", 10),
        ("hold_reason", "HOLD_REASON", 28),
        ("content_preview", "CONTENT_PREVIEW", 60),
    ]
    lines = [
        "  ".join(label.ljust(width) for _, label, width in columns),
        "  ".join("-" * width for _, _, width in columns),
    ]
    for record in records:
        lines.append(
            "  ".join(
                _shorten(record.get(key), width).ljust(width)
                for key, _, width in columns
            )
        )
    return "\n".join(lines)


def _shorten(value: Any, width: int) -> str:
    if value is None:
        return "-"
    text = str(value).replace("\n", " ")
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."
