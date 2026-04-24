"""Aging report for queued publish queue items."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

AGE_BUCKETS: tuple[tuple[str, float | None, float | None], ...] = (
    ("future", None, 0.0),
    ("0-1h", 0.0, 1.0),
    ("1-6h", 1.0, 6.0),
    ("6-24h", 6.0, 24.0),
    ("24-72h", 24.0, 72.0),
    ("72h+", 72.0, None),
)

ACTIVE_STATUSES = ("queued", "failed")
VALID_PLATFORMS = {"x", "bluesky", "all"}


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _age_hours(scheduled_at: str, now: datetime) -> float:
    scheduled = _parse_timestamp(scheduled_at)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return (now - scheduled).total_seconds() / 3600


def age_bucket(age_hours: float) -> str:
    """Return the report bucket name for an age in hours."""
    for label, minimum, maximum in AGE_BUCKETS:
        if minimum is None and age_hours < maximum:
            return label
        if maximum is None and age_hours >= minimum:
            return label
        if minimum is not None and maximum is not None and minimum <= age_hours < maximum:
            return label
    return "72h+"


def _target_platforms(platform: str) -> list[str]:
    if platform == "all":
        return ["x", "bluesky"]
    return [platform]


def _empty_bucket_counts() -> dict[str, int]:
    return {label: 0 for label, _, _ in AGE_BUCKETS}


def _retry_states(db, content_ids: list[int]) -> dict[int, dict[str, dict[str, Any]]]:
    if not content_ids:
        return {}
    placeholders = ",".join("?" for _ in content_ids)
    cursor = db.conn.execute(
        f"""SELECT content_id, platform, status, attempt_count, next_retry_at,
                  last_error_at, error, error_category
           FROM content_publications
           WHERE content_id IN ({placeholders})""",
        content_ids,
    )
    states: dict[int, dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in cursor.fetchall():
        states[row["content_id"]][row["platform"]] = {
            "status": row["status"],
            "attempt_count": row["attempt_count"],
            "next_retry_at": row["next_retry_at"],
            "last_error_at": row["last_error_at"],
            "error": row["error"],
            "error_category": row["error_category"],
        }
    return states


def _review_item(row: dict, age_hours_value: float, retry_states: dict[str, dict]) -> dict:
    return {
        "queue_id": row["id"],
        "content_id": row["content_id"],
        "content_type": row["content_type"],
        "platform": row["platform"],
        "scheduled_at": row["scheduled_at"],
        "status": row["status"],
        "retry_state": retry_states,
        "age_hours": round(age_hours_value, 2),
    }


def build_publish_queue_age_report(
    db,
    *,
    stale_after_hours: float = 24.0,
    platform: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Compute age buckets, oldest item, counts, and stale queue items."""
    if stale_after_hours < 0:
        raise ValueError("stale_after_hours must be non-negative")
    if platform is not None and platform not in VALID_PLATFORMS:
        raise ValueError(f"invalid publish queue platform: {platform}")

    now = now or datetime.now(timezone.utc)
    filters = ["pq.status IN ('queued', 'failed')"]
    params: list[Any] = []
    if platform is not None:
        filters.append("pq.platform = ?")
        params.append(platform)

    cursor = db.conn.execute(
        f"""SELECT pq.id, pq.content_id, pq.scheduled_at, pq.platform, pq.status,
                  pq.error, pq.error_category, gc.content_type
           FROM publish_queue pq
           INNER JOIN generated_content gc ON gc.id = pq.content_id
           WHERE {' AND '.join(filters)}
           ORDER BY pq.scheduled_at ASC, pq.id ASC""",
        params,
    )
    rows = [dict(row) for row in cursor.fetchall()]
    retry_by_content = _retry_states(db, sorted({row["content_id"] for row in rows}))

    platforms: dict[str, dict[str, Any]] = {}
    stale_items: list[dict] = []
    oldest_item: dict | None = None

    for row in rows:
        age = _age_hours(row["scheduled_at"], now)
        bucket = age_bucket(age)
        platform_report = platforms.setdefault(
            row["platform"],
            {
                "total": 0,
                "statuses": {status: 0 for status in ACTIVE_STATUSES},
                "age_buckets": _empty_bucket_counts(),
                "oldest_item": None,
            },
        )
        platform_report["total"] += 1
        platform_report["statuses"][row["status"]] += 1
        platform_report["age_buckets"][bucket] += 1

        retry_states = {
            target: retry_by_content.get(row["content_id"], {}).get(target, {})
            for target in _target_platforms(row["platform"])
        }
        item = _review_item(row, age, retry_states)

        if age >= stale_after_hours:
            stale_items.append(item)

        if platform_report["oldest_item"] is None or age > platform_report["oldest_item"]["age_hours"]:
            platform_report["oldest_item"] = item
        if oldest_item is None or age > oldest_item["age_hours"]:
            oldest_item = item

    return {
        "generated_at": now.isoformat(),
        "stale_after_hours": stale_after_hours,
        "total": len(rows),
        "platforms": platforms,
        "oldest_item": oldest_item,
        "stale_items": stale_items,
    }
