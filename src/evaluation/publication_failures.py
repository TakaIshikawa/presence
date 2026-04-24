"""Publication failure digest for operator remediation."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any

from output.publish_errors import normalize_error_category


PLATFORMS = ("x", "bluesky")

RECOMMENDATIONS = {
    "auth": "Refresh platform credentials and rerun publishing.",
    "rate_limit": "Wait for the rate limit window, then retry.",
    "duplicate": "Inspect duplicate copy and revise before retrying.",
    "media": "Fix media attachment, alt text, or file format before retrying.",
    "network": "Retry after confirming network and platform availability.",
    "unknown": "Inspect the stored error and publisher logs before retrying.",
}


def build_publication_failure_digest(
    db_or_conn: Any,
    days: int = 7,
    platform: str = "all",
    include_queued: bool = False,
    now: datetime | None = None,
    representative_limit: int = 3,
) -> dict:
    """Summarize failed and retrying publication rows from queue and ledger tables."""
    if days <= 0:
        raise ValueError("days must be positive")
    if platform not in {"all", *PLATFORMS}:
        raise ValueError(f"invalid platform: {platform}")
    if representative_limit <= 0:
        raise ValueError("representative_limit must be positive")

    conn = _connection(db_or_conn)
    now = _aware(now or datetime.now(timezone.utc))
    cutoff = (now - timedelta(days=days)).isoformat()
    rows = [
        _normalize_row(row, now)
        for row in (
            _queue_failure_rows(conn, cutoff, platform, include_queued)
            + _publication_failure_rows(conn, cutoff, platform, include_queued)
        )
    ]
    rows = sorted(
        rows,
        key=lambda row: (
            row["platform"],
            row["error_category"],
            row["source"],
            row["content_id"],
            row.get("queue_id") or 0,
            row.get("publication_id") or 0,
        ),
    )

    buckets_by_key: dict[tuple, dict] = {}
    for row in rows:
        normalized = row
        key = (
            normalized["platform"],
            normalized["error_category"],
            normalized["retry_age_bucket"],
            normalized["attempt_count"],
            normalized["next_retry_at"],
        )
        if key not in buckets_by_key:
            buckets_by_key[key] = {
                "platform": normalized["platform"],
                "error_category": normalized["error_category"],
                "count": 0,
                "retry_age_bucket": normalized["retry_age_bucket"],
                "attempt_count": normalized["attempt_count"],
                "next_retry_at": normalized["next_retry_at"],
                "recommendation": _recommendation(normalized["error_category"]),
                "representative_failures": [],
            }
        bucket = buckets_by_key[key]
        bucket["count"] += 1
        if len(bucket["representative_failures"]) < representative_limit:
            bucket["representative_failures"].append(_representative_failure(normalized))

    buckets = sorted(
        buckets_by_key.values(),
        key=lambda bucket: (
            bucket["platform"],
            bucket["error_category"],
            bucket["retry_age_bucket"],
            bucket["attempt_count"],
            bucket["next_retry_at"] or "",
        ),
    )

    totals_by_platform: dict[str, int] = {name: 0 for name in PLATFORMS}
    totals_by_category = {name: 0 for name in RECOMMENDATIONS}
    for row in rows:
        totals_by_platform[row["platform"]] = totals_by_platform.get(row["platform"], 0) + 1
        totals_by_category[row["error_category"]] = totals_by_category.get(row["error_category"], 0) + 1

    return {
        "generated_at": now.isoformat(),
        "window_days": days,
        "platform": platform,
        "include_queued": include_queued,
        "totals": {
            "failures": len(rows),
            "by_platform": {
                key: value
                for key, value in sorted(totals_by_platform.items())
                if value or platform in {"all", key}
            },
            "by_error_category": {
                key: value
                for key, value in sorted(totals_by_category.items())
                if value
            },
        },
        "buckets": buckets,
    }


def format_publication_failure_digest(summary: dict) -> str:
    """Render a publication failure digest for terminal output."""
    if summary["totals"]["failures"] == 0:
        return "No failed or retrying publications found."

    lines = [
        "Publication failure digest",
        f"Generated: {summary['generated_at']}",
        f"Window: {summary['window_days']} days",
        f"Total failures: {summary['totals']['failures']}",
        "",
        "Buckets:",
    ]
    for bucket in summary["buckets"]:
        lines.append(
            "- "
            f"{bucket['platform']} / {bucket['error_category']}: "
            f"{bucket['count']} "
            f"(retry_age={bucket['retry_age_bucket']}, "
            f"attempt_count={bucket['attempt_count']}, "
            f"next_retry_at={bucket['next_retry_at'] or '-'})"
        )
        lines.append(f"  next_action: {bucket['recommendation']}")
        for failure in bucket["representative_failures"]:
            identifiers = [
                f"content={failure['content_id']}",
                f"source={failure['source']}",
            ]
            if failure.get("queue_id") is not None:
                identifiers.append(f"queue={failure['queue_id']}")
            if failure.get("publication_id") is not None:
                identifiers.append(f"publication={failure['publication_id']}")
            lines.append(f"  example: {', '.join(identifiers)}")

    return "\n".join(lines)


def _queue_failure_rows(
    conn: sqlite3.Connection,
    cutoff: str,
    platform: str,
    include_queued: bool,
) -> list[dict]:
    statuses = "('failed', 'queued')" if include_queued else "('failed')"
    platform_filter = ""
    params: list[object] = [cutoff, cutoff]
    if platform != "all":
        platform_filter = "AND qt.platform = ?"
        params.append(platform)

    cursor = conn.execute(
        f"""WITH queue_targets AS (
               SELECT
                   pq.id AS queue_id,
                   pq.content_id,
                   'x' AS platform,
                   pq.status,
                   pq.error,
                   pq.error_category,
                   pq.scheduled_at,
                   pq.created_at
               FROM publish_queue pq
               WHERE pq.platform IN ('x', 'all')
               UNION ALL
               SELECT
                   pq.id AS queue_id,
                   pq.content_id,
                   'bluesky' AS platform,
                   pq.status,
                   pq.error,
                   pq.error_category,
                   pq.scheduled_at,
                   pq.created_at
               FROM publish_queue pq
               WHERE pq.platform IN ('bluesky', 'all')
           )
           SELECT
               'publish_queue' AS source,
               qt.queue_id,
               NULL AS publication_id,
               qt.content_id,
               qt.platform,
               qt.status,
               qt.error,
               qt.error_category,
               0 AS attempt_count,
               NULL AS next_retry_at,
               COALESCE(qt.scheduled_at, qt.created_at) AS failure_at
           FROM queue_targets qt
           INNER JOIN publish_queue pq ON pq.id = qt.queue_id
           WHERE pq.status IN {statuses}
             AND (pq.scheduled_at >= ? OR pq.created_at >= ?)
             {platform_filter}
           ORDER BY qt.platform ASC, qt.queue_id ASC""",
        params,
    )
    return [_row_dict(row) for row in cursor.fetchall()]


def _publication_failure_rows(
    conn: sqlite3.Connection,
    cutoff: str,
    platform: str,
    include_queued: bool,
) -> list[dict]:
    statuses = "('failed', 'queued')" if include_queued else "('failed')"
    filters = [f"cp.status IN {statuses}", "(cp.last_error_at >= ? OR cp.updated_at >= ?)"]
    params: list[object] = [cutoff, cutoff]
    if platform != "all":
        filters.append("cp.platform = ?")
        params.append(platform)

    cursor = conn.execute(
        f"""SELECT
               'content_publications' AS source,
               NULL AS queue_id,
               cp.id AS publication_id,
               cp.content_id,
               cp.platform,
               cp.status,
               cp.error,
               cp.error_category,
               COALESCE(cp.attempt_count, 0) AS attempt_count,
               cp.next_retry_at,
               COALESCE(cp.last_error_at, cp.updated_at) AS failure_at
           FROM content_publications cp
           WHERE {' AND '.join(filters)}
           ORDER BY cp.platform ASC, cp.id ASC""",
        params,
    )
    return [_row_dict(row) for row in cursor.fetchall()]


def _normalize_row(row: dict, now: datetime) -> dict:
    category = normalize_error_category(row.get("error_category"))
    normalized = dict(row)
    normalized["error_category"] = category
    normalized["attempt_count"] = int(normalized.get("attempt_count") or 0)
    normalized["retry_age_bucket"] = _retry_age_bucket(now, normalized.get("failure_at"))
    return normalized


def _representative_failure(row: dict) -> dict:
    return {
        "source": row["source"],
        "content_id": row["content_id"],
        "queue_id": row.get("queue_id"),
        "publication_id": row.get("publication_id"),
        "status": row["status"],
        "error": row.get("error"),
        "failure_at": row.get("failure_at"),
    }


def _recommendation(category: str) -> str:
    return RECOMMENDATIONS.get(category, RECOMMENDATIONS["unknown"])


def _retry_age_bucket(now: datetime, timestamp: str | None) -> str:
    if not timestamp:
        return "unknown_age"
    try:
        age = now - _parse_timestamp(timestamp)
    except ValueError:
        return "unknown_age"
    if age < timedelta(hours=1):
        return "under_1h"
    if age < timedelta(hours=6):
        return "1h_to_6h"
    if age < timedelta(hours=24):
        return "6h_to_24h"
    return "over_24h"


def _parse_timestamp(value: str) -> datetime:
    text = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    return _aware(parsed)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _row_dict(row: sqlite3.Row) -> dict:
    return dict(row)
