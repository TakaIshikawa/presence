"""Read-only triage report for blocked publish queue items."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from .publish_errors import classify_publish_error, normalize_error_category


DEFAULT_STATUSES = ("failed", "held")
VALID_STATUSES = {"failed", "held"}
VALID_PLATFORMS = {"all", "x", "bluesky"}
RETRYABLE_CATEGORIES = {"network", "rate_limit"}


ACTION_DETAILS = {
    "retry_later": {
        "retryable": True,
        "requires_manual_action": False,
        "recommendation": "Wait for the retry window or restore/reschedule the queue item.",
    },
    "fix_credentials": {
        "retryable": False,
        "requires_manual_action": True,
        "recommendation": "Fix platform credentials or access, then restore the queue item.",
    },
    "fix_media": {
        "retryable": False,
        "requires_manual_action": True,
        "recommendation": "Fix attached media or alt text, then restore the queue item.",
    },
    "cancel_duplicate": {
        "retryable": False,
        "requires_manual_action": True,
        "recommendation": "Confirm the content is already published, then cancel or restore intentionally.",
    },
    "inspect_error": {
        "retryable": False,
        "requires_manual_action": True,
        "recommendation": "Inspect the error details and decide whether to restore, reschedule, or cancel.",
    },
    "review_hold": {
        "retryable": False,
        "requires_manual_action": True,
        "recommendation": "Review the hold reason, then release or cancel the queue item.",
    },
}


def build_publish_failure_triage(
    db_or_conn: Any,
    *,
    days: int = 30,
    platform: str = "all",
    status: str | None = None,
    include_content: bool = False,
    now: datetime | None = None,
) -> dict:
    """Build a JSON-serializable report without mutating queue rows."""
    if days <= 0:
        raise ValueError("days must be positive")
    if platform not in VALID_PLATFORMS:
        raise ValueError(f"invalid platform: {platform}")
    if status is not None and status not in VALID_STATUSES:
        raise ValueError(f"invalid status: {status}")

    conn = getattr(db_or_conn, "conn", db_or_conn)
    current = now or datetime.now(timezone.utc)
    cutoff = current - timedelta(days=days)
    statuses = (status,) if status else DEFAULT_STATUSES

    queue_rows = _fetch_queue_rows(conn, statuses)
    publication_rows = _fetch_publication_rows(conn, queue_rows)
    items = []
    for queue_row in queue_rows:
        for target_platform in _target_platforms(queue_row["queue_platform"]):
            if platform != "all" and target_platform != platform:
                continue
            publication = publication_rows.get(
                (queue_row["content_id"], target_platform)
            )
            if not _within_window(queue_row, publication, cutoff):
                continue
            items.append(
                _triage_item(
                    queue_row,
                    target_platform=target_platform,
                    publication=publication,
                    include_content=include_content,
                )
            )

    items.sort(
        key=lambda item: (
            item["platform"],
            item["category"],
            item["recommended_action"],
            item["queue_id"],
            item["content_id"],
        )
    )
    groups = _group_items(items)
    return {
        "generated_at": current.isoformat(),
        "days": days,
        "platform": platform,
        "status": status,
        "include_content": include_content,
        "total_items": len(items),
        "groups": groups,
        "items": items,
    }


def _fetch_queue_rows(conn: Any, statuses: tuple[str, ...]) -> list[dict]:
    placeholders = ", ".join("?" for _ in statuses)
    rows = conn.execute(
        f"""SELECT pq.id AS queue_id,
                  pq.content_id,
                  pq.scheduled_at,
                  pq.platform AS queue_platform,
                  pq.status AS queue_status,
                  pq.published_at AS queue_published_at,
                  pq.error AS queue_error,
                  pq.error_category AS queue_error_category,
                  pq.hold_reason,
                  pq.created_at AS queue_created_at,
                  gc.content_type,
                  gc.content
           FROM publish_queue pq
           INNER JOIN generated_content gc ON gc.id = pq.content_id
           WHERE pq.status IN ({placeholders})
           ORDER BY pq.scheduled_at ASC, pq.id ASC""",
        statuses,
    ).fetchall()
    return [dict(row) for row in rows]


def _fetch_publication_rows(
    conn: Any,
    queue_rows: list[dict],
) -> dict[tuple[int, str], dict]:
    content_ids = sorted({row["content_id"] for row in queue_rows})
    if not content_ids:
        return {}

    placeholders = ", ".join("?" for _ in content_ids)
    rows = conn.execute(
        f"""SELECT id AS publication_id,
                  content_id,
                  platform,
                  status AS publication_status,
                  error AS publication_error,
                  error_category AS publication_error_category,
                  attempt_count,
                  next_retry_at,
                  last_error_at,
                  updated_at AS publication_updated_at
           FROM content_publications
           WHERE content_id IN ({placeholders})""",
        content_ids,
    ).fetchall()
    return {(row["content_id"], row["platform"]): dict(row) for row in rows}


def _target_platforms(queue_platform: str) -> tuple[str, ...]:
    if queue_platform == "all":
        return ("x", "bluesky")
    if queue_platform in {"x", "bluesky"}:
        return (queue_platform,)
    return ("unknown",)


def _within_window(
    queue_row: dict,
    publication: dict | None,
    cutoff: datetime,
) -> bool:
    candidates = [
        queue_row.get("queue_created_at"),
        queue_row.get("scheduled_at"),
    ]
    if publication:
        candidates.extend(
            [
                publication.get("last_error_at"),
                publication.get("publication_updated_at"),
                publication.get("next_retry_at"),
            ]
        )

    parsed = [_parse_timestamp(value) for value in candidates]
    parsed = [value for value in parsed if value is not None]
    if not parsed:
        return True
    return max(parsed) >= cutoff


def _parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _triage_item(
    queue_row: dict,
    *,
    target_platform: str,
    publication: dict | None,
    include_content: bool,
) -> dict:
    error = _first_present(
        publication.get("publication_error") if publication else None,
        queue_row.get("queue_error"),
        queue_row.get("hold_reason"),
    )
    category = _category_for(queue_row, publication, error)
    action = _recommended_action(queue_row["queue_status"], category)
    details = ACTION_DETAILS[action]
    item = {
        "queue_id": queue_row["queue_id"],
        "content_id": queue_row["content_id"],
        "publication_id": publication.get("publication_id") if publication else None,
        "platform": target_platform,
        "queue_platform": queue_row["queue_platform"],
        "status": queue_row["queue_status"],
        "category": category,
        "recommended_action": action,
        "retryable": details["retryable"],
        "requires_manual_action": details["requires_manual_action"],
        "recommendation": details["recommendation"],
        "scheduled_at": queue_row["scheduled_at"],
        "created_at": queue_row["queue_created_at"],
        "hold_reason": queue_row["hold_reason"],
        "error": error,
        "content_type": queue_row["content_type"],
        "attempt_count": publication.get("attempt_count") if publication else None,
        "next_retry_at": publication.get("next_retry_at") if publication else None,
        "last_error_at": publication.get("last_error_at") if publication else None,
    }
    if include_content:
        item["content"] = queue_row["content"]
    return item


def _category_for(
    queue_row: dict,
    publication: dict | None,
    error: str | None,
) -> str:
    category = None
    if publication:
        category = publication.get("publication_error_category")
    if category is None:
        category = queue_row.get("queue_error_category")
    normalized = normalize_error_category(category)
    if normalized != "unknown":
        return normalized
    return classify_publish_error(error, platform=queue_row.get("queue_platform"))


def _recommended_action(status: str, category: str) -> str:
    if status == "held":
        return "review_hold"
    if category in RETRYABLE_CATEGORIES:
        return "retry_later"
    if category == "auth":
        return "fix_credentials"
    if category == "media":
        return "fix_media"
    if category == "duplicate":
        return "cancel_duplicate"
    return "inspect_error"


def _group_items(items: list[dict]) -> list[dict]:
    grouped: dict[tuple[str, str, str], dict] = {}
    for item in items:
        key = (item["platform"], item["category"], item["recommended_action"])
        group = grouped.setdefault(
            key,
            {
                "platform": item["platform"],
                "category": item["category"],
                "recommended_action": item["recommended_action"],
                "retryable": item["retryable"],
                "requires_manual_action": item["requires_manual_action"],
                "recommendation": item["recommendation"],
                "count": 0,
                "queue_ids": [],
                "content_ids": [],
                "sample_errors": [],
            },
        )
        group["count"] += 1
        group["queue_ids"].append(item["queue_id"])
        group["content_ids"].append(item["content_id"])
        if item.get("error") and item["error"] not in group["sample_errors"]:
            group["sample_errors"].append(item["error"])

    groups = list(grouped.values())
    for group in groups:
        group["queue_ids"] = sorted(set(group["queue_ids"]))
        group["content_ids"] = sorted(set(group["content_ids"]))
        group["sample_errors"] = group["sample_errors"][:3]
    groups.sort(
        key=lambda group: (
            group["platform"],
            group["category"],
            group["recommended_action"],
        )
    )
    return groups


def _first_present(*values: Any) -> str | None:
    for value in values:
        if value:
            return str(value)
    return None
