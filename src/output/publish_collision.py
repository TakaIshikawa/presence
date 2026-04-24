"""Detect near-duplicate publish slots in the queued publish queue."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from typing import Any

SUPPORTED_PLATFORMS = ("x", "bluesky")
DEFAULT_WINDOW_MINUTES = 15
DEFAULT_DAYS_AHEAD = 7
PREVIEW_LENGTH = 96


@dataclass(frozen=True)
class PublishQueueItem:
    """Queued publish row with generated content context."""

    queue_id: int
    content_id: int
    scheduled_at: str
    scheduled_at_dt: datetime
    platform: str
    content_type: str | None = None
    content_preview: str | None = None
    eval_score: float | None = None
    content_format: str | None = None

    def to_dict(self) -> dict[str, Any]:
        data = {
            "queue_id": self.queue_id,
            "content_id": self.content_id,
            "scheduled_at": self.scheduled_at,
            "platform": self.platform,
        }
        metadata = {
            "content_type": self.content_type,
            "content_preview": self.content_preview,
            "eval_score": self.eval_score,
            "content_format": self.content_format,
        }
        data["generated_content"] = {
            key: value for key, value in metadata.items() if value is not None
        }
        return data


@dataclass(frozen=True)
class PublishCollision:
    """A platform-specific cluster of queued items scheduled too close together."""

    platform: str
    window_minutes: int
    item_count: int
    span_minutes: float
    earliest_scheduled_at: str
    latest_scheduled_at: str
    items: tuple[PublishQueueItem, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "platform": self.platform,
            "window_minutes": self.window_minutes,
            "item_count": self.item_count,
            "span_minutes": self.span_minutes,
            "earliest_scheduled_at": self.earliest_scheduled_at,
            "latest_scheduled_at": self.latest_scheduled_at,
            "items": [item.to_dict() for item in self.items],
        }


def scan_publish_collisions(
    db,
    *,
    window_minutes: int = DEFAULT_WINDOW_MINUTES,
    days_ahead: int = DEFAULT_DAYS_AHEAD,
    platform: str = "all",
    now: datetime | None = None,
) -> list[PublishCollision]:
    """Return queued publish groups with platform-effective scheduling collisions."""
    if window_minutes <= 0:
        raise ValueError("window_minutes must be positive")
    if days_ahead < 0:
        raise ValueError("days_ahead must be non-negative")
    if platform not in {"all", *SUPPORTED_PLATFORMS}:
        raise ValueError(f"unsupported platform: {platform}")

    now = _normalize_datetime(now or datetime.now(timezone.utc))
    until = now + timedelta(days=days_ahead)
    items = _load_queued_items(db, now, until)
    target_platforms = SUPPORTED_PLATFORMS if platform == "all" else (platform,)

    collisions: list[PublishCollision] = []
    for target_platform in target_platforms:
        platform_items = sorted(
            (
                item
                for item in items
                if target_platform in _effective_platforms(item.platform)
            ),
            key=lambda item: (item.scheduled_at_dt, item.queue_id),
        )
        collisions.extend(
            _find_platform_collisions(
                target_platform,
                platform_items,
                window_minutes=window_minutes,
            )
        )

    return sorted(
        collisions,
        key=lambda collision: (
            collision.earliest_scheduled_at,
            SUPPORTED_PLATFORMS.index(collision.platform),
            collision.items[0].queue_id,
        ),
    )


def collisions_to_dicts(
    collisions: list[PublishCollision],
) -> list[dict[str, Any]]:
    """Serialize collision objects into JSON-friendly dictionaries."""
    return [collision.to_dict() for collision in collisions]


def collisions_to_json(collisions: list[PublishCollision]) -> str:
    """Serialize collision findings as stable JSON."""
    return json.dumps(collisions_to_dicts(collisions), indent=2, sort_keys=True)


def format_text_collisions(collisions: list[PublishCollision]) -> str:
    """Format collision findings for operator review."""
    if not collisions:
        return "No publish queue collisions found."

    lines = [f"Publish queue collisions: {len(collisions)}"]
    for index, collision in enumerate(collisions, 1):
        lines.append("")
        lines.append(
            f"{index}. {collision.platform}: {collision.item_count} queued items "
            f"within {collision.window_minutes} minutes "
            f"({collision.earliest_scheduled_at} to {collision.latest_scheduled_at})"
        )
        for item in collision.items:
            metadata = item.to_dict()["generated_content"]
            details = []
            if metadata.get("content_type"):
                details.append(metadata["content_type"])
            if metadata.get("content_format"):
                details.append(metadata["content_format"])
            if metadata.get("eval_score") is not None:
                details.append(f"score {metadata['eval_score']}")
            suffix = f" [{' | '.join(details)}]" if details else ""
            preview = metadata.get("content_preview")
            lines.append(
                f"  - queue {item.queue_id} content {item.content_id} "
                f"{item.scheduled_at} platform={item.platform}{suffix}"
            )
            if preview:
                lines.append(f"    {preview}")

    return "\n".join(lines)


def _load_queued_items(
    db,
    now: datetime,
    until: datetime,
) -> list[PublishQueueItem]:
    cursor = db.conn.execute(
        """SELECT pq.id AS queue_id,
                  pq.content_id,
                  pq.scheduled_at,
                  pq.platform,
                  gc.content,
                  gc.content_type,
                  gc.eval_score,
                  gc.content_format
           FROM publish_queue pq
           INNER JOIN generated_content gc ON gc.id = pq.content_id
           WHERE pq.status = 'queued'
             AND pq.scheduled_at >= ?
             AND pq.scheduled_at <= ?
           ORDER BY pq.scheduled_at ASC, pq.id ASC""",
        (now.isoformat(), until.isoformat()),
    )
    rows = [dict(row) for row in cursor.fetchall()]
    return [_row_to_item(row) for row in rows]


def _row_to_item(row: dict[str, Any]) -> PublishQueueItem:
    scheduled_at = str(row["scheduled_at"])
    return PublishQueueItem(
        queue_id=int(row["queue_id"]),
        content_id=int(row["content_id"]),
        scheduled_at=scheduled_at,
        scheduled_at_dt=_parse_datetime(scheduled_at),
        platform=row.get("platform") or "all",
        content_type=row.get("content_type"),
        content_preview=_preview(row.get("content")),
        eval_score=row.get("eval_score"),
        content_format=row.get("content_format"),
    )


def _find_platform_collisions(
    platform: str,
    items: list[PublishQueueItem],
    *,
    window_minutes: int,
) -> list[PublishCollision]:
    if len(items) < 2:
        return []

    window = timedelta(minutes=window_minutes)
    collisions: list[PublishCollision] = []
    group: list[PublishQueueItem] = []

    for item in items:
        if not group:
            group = [item]
            continue
        if item.scheduled_at_dt - group[-1].scheduled_at_dt <= window:
            group.append(item)
            continue
        if len(group) > 1:
            collisions.append(_collision_from_group(platform, group, window_minutes))
        group = [item]

    if len(group) > 1:
        collisions.append(_collision_from_group(platform, group, window_minutes))

    return collisions


def _collision_from_group(
    platform: str,
    group: list[PublishQueueItem],
    window_minutes: int,
) -> PublishCollision:
    span = group[-1].scheduled_at_dt - group[0].scheduled_at_dt
    return PublishCollision(
        platform=platform,
        window_minutes=window_minutes,
        item_count=len(group),
        span_minutes=round(span.total_seconds() / 60, 2),
        earliest_scheduled_at=group[0].scheduled_at,
        latest_scheduled_at=group[-1].scheduled_at,
        items=tuple(group),
    )


def _effective_platforms(platform: str) -> tuple[str, ...]:
    if platform == "all":
        return SUPPORTED_PLATFORMS
    return (platform,)


def _parse_datetime(value: str) -> datetime:
    return _normalize_datetime(datetime.fromisoformat(value))


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _preview(content: str | None) -> str | None:
    if not content:
        return None
    text = " ".join(str(content).split())
    if len(text) <= PREVIEW_LENGTH:
        return text
    return text[: PREVIEW_LENGTH - 3] + "..."
