"""Recovery recommendations for failed publication attempts."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Literal

from .publish_errors import PublishErrorCategory, classify_publish_error, normalize_error_category


RecoveryAction = Literal[
    "retry_now",
    "wait_for_backoff",
    "refresh_auth",
    "edit_duplicate",
    "inspect_media",
    "cancel",
]

RecoveryStatus = Literal["queued", "failed", "held", "cancelled"]

DEFAULT_MAX_ATTEMPTS = 3
RECOVERY_STATUSES: tuple[RecoveryStatus, ...] = (
    "queued",
    "failed",
    "held",
    "cancelled",
)
RECOVERY_PLATFORMS = ("x", "bluesky")


def get_publish_recovery_recommendations(
    conn: sqlite3.Connection,
    *,
    platform: str | None = None,
    status: str | None = None,
    limit: int = 50,
    now: datetime | None = None,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
) -> list[dict]:
    """Return grouped recovery recommendations for queued publication work."""
    if platform is not None and platform not in RECOVERY_PLATFORMS:
        raise ValueError(f"invalid recovery platform: {platform}")
    if status is not None and status not in RECOVERY_STATUSES:
        raise ValueError(f"invalid recovery status: {status}")
    if limit <= 0:
        raise ValueError("limit must be positive")

    now = _coerce_now(now)
    rows = _fetch_recovery_rows(conn, platform=platform, status=status)
    groups: dict[tuple[str, int, str | None, str, str], dict] = {}

    for row in rows:
        item = _recovery_item(dict(row))
        category = item["error_category"]
        attempt_count = item["attempt_count"]
        next_retry_at = item["next_retry_at"]
        action = recommend_recovery_action(
            category=category,
            status=item["status"],
            attempt_count=attempt_count,
            next_retry_at=next_retry_at,
            now=now,
            max_attempts=max_attempts,
        )
        group_key = (
            category,
            attempt_count,
            next_retry_at,
            item["platform"],
            action,
        )
        if group_key not in groups:
            groups[group_key] = {
                "action": action,
                "error_category": category,
                "attempt_count": attempt_count,
                "next_retry_at": next_retry_at,
                "platform": item["platform"],
                "status": item["status"],
                "count": 0,
                "items": [],
            }
        group = groups[group_key]
        group["items"].append(item)
        group["count"] += 1

    ordered = sorted(
        groups.values(),
        key=lambda group: (
            _action_rank(group["action"]),
            group["next_retry_at"] or "",
            group["platform"],
            group["error_category"],
            group["attempt_count"],
        ),
    )
    for group in ordered:
        group["items"].sort(
            key=lambda item: (
                item["scheduled_at"] or "",
                item["content_id"],
                item["platform"],
            )
        )
    return ordered[:limit]


def recommend_recovery_action(
    *,
    category: PublishErrorCategory,
    status: str,
    attempt_count: int,
    next_retry_at: str | None,
    now: datetime | None = None,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
) -> RecoveryAction:
    """Map failure state to a deterministic operator action."""
    now = _coerce_now(now)
    category = normalize_error_category(category)

    if status == "held":
        return "cancel"
    if category == "auth":
        return "refresh_auth"
    if category == "duplicate":
        return "edit_duplicate"
    if category == "media":
        return "inspect_media"
    if attempt_count >= max_attempts:
        return "cancel"
    if next_retry_at and _parse_iso(next_retry_at) > now:
        return "wait_for_backoff"
    if category in {"rate_limit", "network", "unknown"}:
        return "retry_now"
    return "cancel"


def _fetch_recovery_rows(
    conn: sqlite3.Connection,
    *,
    platform: str | None,
    status: str | None,
) -> list[sqlite3.Row]:
    filters = [
        """COALESCE(
               CASE WHEN lq.queue_status = 'held' THEN 'held' ELSE cp.status END,
               lq.queue_status
           ) IN ('queued', 'failed', 'held', 'cancelled')""",
        """NOT (
               targets.platform = 'x'
               AND COALESCE(gc.published, 0) = 1
           )""",
        """NOT (
               targets.platform = 'bluesky'
               AND gc.bluesky_uri IS NOT NULL
           )""",
    ]
    params: list[object] = []
    if platform is not None:
        filters.append("targets.platform = ?")
        params.append(platform)
    if status is not None:
        filters.append(
            """COALESCE(
                   CASE WHEN lq.queue_status = 'held' THEN 'held' ELSE cp.status END,
                   lq.queue_status
               ) = ?"""
        )
        params.append(status)

    cursor = conn.execute(
        f"""WITH queue_targets AS (
               SELECT
                   pq.id AS queue_id,
                   pq.content_id,
                   'x' AS platform,
                   pq.platform AS queue_platform,
                   pq.status AS queue_status,
                   pq.error AS queue_error,
                   pq.error_category AS queue_error_category,
                   pq.hold_reason AS queue_hold_reason,
                   pq.scheduled_at,
                   pq.created_at AS queue_created_at
               FROM publish_queue pq
               WHERE pq.platform IN ('x', 'all')
               UNION ALL
               SELECT
                   pq.id AS queue_id,
                   pq.content_id,
                   'bluesky' AS platform,
                   pq.platform AS queue_platform,
                   pq.status AS queue_status,
                   pq.error AS queue_error,
                   pq.error_category AS queue_error_category,
                   pq.hold_reason AS queue_hold_reason,
                   pq.scheduled_at,
                   pq.created_at AS queue_created_at
               FROM publish_queue pq
               WHERE pq.platform IN ('bluesky', 'all')
           ),
           latest_queue AS (
               SELECT *
               FROM (
                   SELECT
                       qt.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY qt.content_id, qt.platform
                           ORDER BY qt.scheduled_at DESC, qt.queue_id DESC
                       ) AS rn
                   FROM queue_targets qt
               )
               WHERE rn = 1
           ),
           targets AS (
               SELECT content_id, platform FROM latest_queue
               UNION
               SELECT content_id, platform
               FROM content_publications
               WHERE status IN ('queued', 'failed', 'cancelled')
           )
           SELECT
               gc.id AS content_id,
               gc.content_type,
               gc.content,
               targets.platform,
               lq.queue_id,
               lq.queue_platform,
               lq.queue_status,
               lq.queue_error,
               lq.queue_error_category,
               lq.queue_hold_reason,
               lq.scheduled_at,
               cp.id AS publication_id,
               cp.status AS publication_status,
               cp.error AS publication_error,
               cp.error_category AS publication_error_category,
               cp.attempt_count,
               cp.next_retry_at,
               cp.last_error_at,
               cp.updated_at AS publication_updated_at,
               COALESCE(
                   CASE WHEN lq.queue_status = 'held' THEN 'held' ELSE cp.status END,
                   lq.queue_status
               ) AS status
           FROM targets
           INNER JOIN generated_content gc ON gc.id = targets.content_id
           LEFT JOIN latest_queue lq
             ON lq.content_id = targets.content_id
            AND lq.platform = targets.platform
           LEFT JOIN content_publications cp
             ON cp.content_id = targets.content_id
            AND cp.platform = targets.platform
           WHERE {" AND ".join(filters)}
           ORDER BY
               targets.platform ASC,
               COALESCE(cp.next_retry_at, lq.scheduled_at, cp.updated_at, lq.queue_created_at) ASC,
               gc.id ASC""",
        params,
    )
    return cursor.fetchall()


def _recovery_item(row: dict) -> dict:
    error = row.get("publication_error") or row.get("queue_error")
    raw_category = row.get("publication_error_category") or row.get("queue_error_category")
    category = (
        normalize_error_category(raw_category)
        if raw_category is not None
        else classify_publish_error(error, platform=row.get("platform"))
    )
    attempt_count = int(row.get("attempt_count") or 0)
    return {
        "content_id": row["content_id"],
        "content_type": row["content_type"],
        "content": row["content"],
        "platform": row["platform"],
        "status": row["status"],
        "error": error,
        "error_category": category,
        "attempt_count": attempt_count,
        "next_retry_at": row.get("next_retry_at"),
        "scheduled_at": row.get("scheduled_at"),
        "queue_id": row.get("queue_id"),
        "queue_platform": row.get("queue_platform"),
        "queue_status": row.get("queue_status"),
        "hold_reason": row.get("queue_hold_reason"),
        "publication_id": row.get("publication_id"),
        "publication_status": row.get("publication_status"),
        "last_error_at": row.get("last_error_at"),
        "publication_updated_at": row.get("publication_updated_at"),
    }


def _coerce_now(now: datetime | None) -> datetime:
    value = now or datetime.now(timezone.utc)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_iso(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _action_rank(action: str) -> int:
    ranks = {
        "refresh_auth": 0,
        "inspect_media": 1,
        "edit_duplicate": 2,
        "retry_now": 3,
        "wait_for_backoff": 4,
        "cancel": 5,
    }
    return ranks.get(action, 99)
