"""Plan and apply retry policy for failed publication attempts."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from .publish_errors import (
    KNOWN_ERROR_CATEGORIES,
    PublishErrorCategory,
    classify_publish_error,
    normalize_error_category,
)


DEFAULT_DAYS = 7
DEFAULT_MAX_ATTEMPTS = 3
RETRY_POLICY_PLATFORMS = ("x", "bluesky")
TERMINAL_ERROR_CATEGORIES = {"auth", "duplicate", "media", "validation"}


RetryPolicyAction = Literal["retry", "wait", "terminal"]
RetryPolicySource = Literal["content_publications", "publish_queue"]


@dataclass(frozen=True)
class BackoffRule:
    """Backoff settings for one platform/error category pair."""

    base_minutes: int
    max_minutes: int
    retryable: bool = True


DEFAULT_BACKOFF_RULES: dict[str, dict[str, BackoffRule]] = {
    "default": {
        "rate_limit": BackoffRule(base_minutes=60, max_minutes=360),
        "network": BackoffRule(base_minutes=5, max_minutes=120),
        "unknown": BackoffRule(base_minutes=15, max_minutes=180),
        "auth": BackoffRule(base_minutes=0, max_minutes=0, retryable=False),
        "duplicate": BackoffRule(base_minutes=0, max_minutes=0, retryable=False),
        "media": BackoffRule(base_minutes=0, max_minutes=0, retryable=False),
        "validation": BackoffRule(base_minutes=0, max_minutes=0, retryable=False),
    },
    "x": {
        "rate_limit": BackoffRule(base_minutes=90, max_minutes=480),
    },
    "bluesky": {
        "rate_limit": BackoffRule(base_minutes=30, max_minutes=240),
    },
}


def build_retry_policy_plan(
    db_or_conn: Any,
    *,
    platform: str = "all",
    days: int = DEFAULT_DAYS,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    apply: bool = False,
    now: datetime | None = None,
    backoff_rules: dict[str, dict[str, BackoffRule]] | None = None,
) -> dict[str, Any]:
    """Return a grouped retry policy plan and optionally apply it."""
    if platform not in {"all", *RETRY_POLICY_PLATFORMS}:
        raise ValueError(f"invalid platform: {platform}")
    if days <= 0:
        raise ValueError("days must be positive")
    if max_attempts <= 0:
        raise ValueError("max_attempts must be positive")

    conn = _connection(db_or_conn)
    now_dt = _aware(now or datetime.now(timezone.utc))
    cutoff = (now_dt - timedelta(days=days)).isoformat()
    rules = backoff_rules or DEFAULT_BACKOFF_RULES

    rows = [
        _plan_item(row, now=now_dt, max_attempts=max_attempts, backoff_rules=rules)
        for row in _fetch_policy_rows(conn, cutoff=cutoff, platform=platform)
    ]
    rows.sort(
        key=lambda row: (
            _action_rank(row["action"]),
            row["platform"],
            row["error_category"],
            row["attempt_count"],
            row["content_id"],
            row["source"],
            row.get("publication_id") or row.get("queue_id") or 0,
        )
    )

    if apply and rows:
        _apply_policy_rows(conn, rows, applied_at=now_dt.isoformat())
        for row in rows:
            row["applied"] = True
    else:
        for row in rows:
            row["applied"] = False

    groups = _group_policy_rows(rows)
    return {
        "generated_at": now_dt.isoformat(),
        "window_days": days,
        "platform": platform,
        "max_attempts": max_attempts,
        "applied": apply,
        "totals": {
            "failures": len(rows),
            "retryable": sum(1 for row in rows if row["action"] in {"retry", "wait"}),
            "terminal": sum(1 for row in rows if row["action"] == "terminal"),
            "updates": sum(
                1
                for row in rows
                if row["proposed_next_retry_at"] != row["current_retry_at"]
            ),
        },
        "groups": groups,
        "items": rows,
    }


def calculate_next_retry_at(
    *,
    platform: str,
    error_category: str,
    attempt_count: int,
    failure_at: datetime,
    now: datetime,
    backoff_rules: dict[str, dict[str, BackoffRule]] | None = None,
) -> datetime | None:
    """Calculate the next retry time for a failed publication."""
    category = normalize_error_category(error_category)
    rule = _backoff_rule(platform, category, backoff_rules or DEFAULT_BACKOFF_RULES)
    if not rule.retryable:
        return None
    delay_minutes = min(
        rule.max_minutes,
        rule.base_minutes * (2 ** max(0, attempt_count - 1)),
    )
    proposed = failure_at + timedelta(minutes=delay_minutes)
    return max(_aware(proposed), _aware(now))


def format_retry_policy_plan_json(plan: dict[str, Any]) -> str:
    """Format a retry policy plan as stable JSON."""
    return json.dumps(plan, indent=2, sort_keys=True)


def format_retry_policy_plan_text(plan: dict[str, Any]) -> str:
    """Render a retry policy plan for terminal output."""
    if plan["totals"]["failures"] == 0:
        return "No failed publication retries found."

    mode = "applied" if plan["applied"] else "dry run"
    lines = [
        "Publication retry policy plan",
        f"Generated: {plan['generated_at']}",
        f"Mode: {mode}",
        f"Window: {plan['window_days']} days",
        f"Total failures: {plan['totals']['failures']}",
        f"Retryable: {plan['totals']['retryable']}",
        f"Terminal: {plan['totals']['terminal']}",
        "",
        "Groups:",
    ]
    for group in plan["groups"]:
        lines.append(
            "- "
            f"{group['platform']} / {group['error_category']} / "
            f"attempts={group['attempt_count']}: {group['count']} "
            f"action={group['action']} "
            f"next_retry_at={group['proposed_next_retry_at'] or '-'}"
        )
        for item in group["items"][:3]:
            identifiers = [
                f"content={item['content_id']}",
                f"source={item['source']}",
            ]
            if item.get("publication_id") is not None:
                identifiers.append(f"publication={item['publication_id']}")
            if item.get("queue_id") is not None:
                identifiers.append(f"queue={item['queue_id']}")
            lines.append(
                "  - "
                + ", ".join(identifiers)
                + f", current={item['current_retry_at'] or '-'}"
                + f", proposed={item['proposed_next_retry_at'] or '-'}"
            )
    return "\n".join(lines)


def _fetch_policy_rows(
    conn: sqlite3.Connection,
    *,
    cutoff: str,
    platform: str,
) -> list[sqlite3.Row]:
    retry_timestamp = (
        "COALESCE(latest.failure_at, cp.last_error_at, cp.updated_at, "
        "lq.created_at, lq.scheduled_at)"
    )
    filters = [f"{retry_timestamp} >= ?"]
    params: list[object] = [cutoff, cutoff, cutoff, cutoff]
    if platform != "all":
        filters.append("targets.platform = ?")
        params.append(platform)

    cursor = conn.execute(
        f"""WITH failed_attempts AS (
               SELECT
                   pa.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY pa.content_id, pa.platform
                       ORDER BY pa.attempted_at DESC, pa.id DESC
                   ) AS rn,
                   COUNT(*) OVER (
                       PARTITION BY pa.content_id, pa.platform
                   ) AS failed_attempt_count
               FROM publication_attempts pa
               WHERE pa.success = 0
           ),
           latest AS (
               SELECT
                   fa.id AS attempt_id,
                   fa.queue_id AS attempt_queue_id,
                   fa.content_id,
                   fa.platform,
                   fa.attempted_at AS failure_at,
                   fa.error AS attempt_error,
                   fa.error_category AS attempt_error_category,
                   fa.failed_attempt_count
               FROM failed_attempts fa
               WHERE fa.rn = 1
           ),
           queue_targets AS (
               SELECT
                   pq.id AS queue_id,
                   pq.content_id,
                   'x' AS platform,
                   pq.platform AS queue_platform,
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
                   pq.platform AS queue_platform,
                   pq.status,
                   pq.error,
                   pq.error_category,
                   pq.scheduled_at,
                   pq.created_at
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
                   WHERE qt.status = 'failed'
               )
               WHERE rn = 1
           ),
           targets AS (
               SELECT cp.content_id, cp.platform
               FROM content_publications cp
               WHERE cp.status = 'failed'
                 AND COALESCE(cp.last_error_at, cp.updated_at) >= ?
               UNION
               SELECT latest.content_id, latest.platform
               FROM latest
               WHERE latest.failure_at >= ?
               UNION
               SELECT lq.content_id, lq.platform
               FROM latest_queue lq
               WHERE COALESCE(lq.created_at, lq.scheduled_at) >= ?
           )
           SELECT
               latest.attempt_id,
               latest.attempt_queue_id,
               targets.content_id,
               targets.platform,
               latest.failure_at,
               latest.attempt_error,
               latest.attempt_error_category,
               latest.failed_attempt_count,
               cp.id AS publication_id,
               cp.status AS publication_status,
               cp.error AS publication_error,
               cp.error_category AS publication_error_category,
               cp.attempt_count,
               cp.next_retry_at,
               cp.last_error_at,
               cp.updated_at AS publication_updated_at,
               lq.queue_id,
               lq.queue_platform,
               lq.status AS queue_status,
               lq.error AS queue_error,
               lq.error_category AS queue_error_category,
               lq.scheduled_at,
               lq.created_at AS queue_created_at
           FROM targets
           LEFT JOIN latest
             ON latest.content_id = targets.content_id
            AND latest.platform = targets.platform
           LEFT JOIN content_publications cp
             ON cp.content_id = targets.content_id
            AND cp.platform = targets.platform
            AND cp.status = 'failed'
           LEFT JOIN latest_queue lq
             ON lq.content_id = targets.content_id
            AND lq.platform = targets.platform
           WHERE {" AND ".join(filters)}
             AND (cp.id IS NOT NULL OR lq.queue_id IS NOT NULL)
           ORDER BY targets.platform ASC,
                    {retry_timestamp} ASC,
                    targets.content_id ASC""",
        params,
    )
    return cursor.fetchall()


def _plan_item(
    row: sqlite3.Row,
    *,
    now: datetime,
    max_attempts: int,
    backoff_rules: dict[str, dict[str, BackoffRule]],
) -> dict[str, Any]:
    data = dict(row)
    source: RetryPolicySource = (
        "content_publications" if data.get("publication_id") is not None else "publish_queue"
    )
    error = (
        data.get("publication_error")
        or data.get("queue_error")
        or data.get("attempt_error")
    )
    raw_category = (
        data.get("publication_error_category")
        or data.get("queue_error_category")
        or data.get("attempt_error_category")
    )
    category = (
        normalize_error_category(raw_category)
        if raw_category is not None
        else classify_publish_error(error, platform=data["platform"])
    )
    attempt_count = max(
        int(data.get("attempt_count") or 0),
        int(data.get("failed_attempt_count") or 0),
    )
    failure_at_raw = (
        data.get("failure_at")
        or data.get("last_error_at")
        or data.get("publication_updated_at")
        or data.get("queue_created_at")
        or data.get("scheduled_at")
    )
    failure_at = _parse_timestamp(failure_at_raw)
    proposed = calculate_next_retry_at(
        platform=data["platform"],
        error_category=category,
        attempt_count=attempt_count,
        failure_at=failure_at,
        now=now,
        backoff_rules=backoff_rules,
    )
    terminal_reason = None
    if attempt_count >= max_attempts:
        terminal_reason = "max_attempts"
    elif category in TERMINAL_ERROR_CATEGORIES:
        terminal_reason = "non_retryable_error"

    if terminal_reason is not None:
        action: RetryPolicyAction = "terminal"
        proposed_text = None
    else:
        proposed_text = proposed.isoformat() if proposed else None
        current = (
            data.get("next_retry_at")
            if source == "content_publications"
            else data.get("scheduled_at")
        )
        action = "wait" if current == proposed_text else "retry"

    return {
        "source": source,
        "content_id": data["content_id"],
        "platform": data["platform"],
        "publication_id": data.get("publication_id"),
        "queue_id": data.get("queue_id") or data.get("attempt_queue_id"),
        "queue_platform": data.get("queue_platform"),
        "attempt_id": data.get("attempt_id"),
        "failure_at": failure_at.isoformat(),
        "error": error,
        "error_category": category,
        "attempt_count": attempt_count,
        "current_retry_at": (
            data.get("next_retry_at")
            if source == "content_publications"
            else data.get("scheduled_at")
        ),
        "proposed_next_retry_at": proposed_text,
        "action": action,
        "terminal_reason": terminal_reason,
    }


def _apply_policy_rows(
    conn: sqlite3.Connection,
    rows: list[dict[str, Any]],
    *,
    applied_at: str,
) -> None:
    for row in rows:
        if row["source"] == "content_publications":
            if row["action"] == "terminal":
                conn.execute(
                    """UPDATE content_publications
                       SET status = 'cancelled',
                           next_retry_at = NULL,
                           updated_at = ?
                       WHERE id = ? AND status = 'failed'""",
                    (applied_at, row["publication_id"]),
                )
            else:
                conn.execute(
                    """UPDATE content_publications
                       SET next_retry_at = ?,
                           updated_at = ?
                       WHERE id = ? AND status = 'failed'""",
                    (row["proposed_next_retry_at"], applied_at, row["publication_id"]),
                )
        elif row.get("queue_id") is not None:
            if row["action"] == "terminal":
                conn.execute(
                    """UPDATE publish_queue
                       SET status = 'cancelled',
                           hold_reason = NULL
                       WHERE id = ? AND status = 'failed'""",
                    (row["queue_id"],),
                )
            else:
                conn.execute(
                    """UPDATE publish_queue
                       SET scheduled_at = ?
                       WHERE id = ? AND status = 'failed'""",
                    (row["proposed_next_retry_at"], row["queue_id"]),
                )
    conn.commit()


def _group_policy_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups_by_key: dict[tuple[str, str, int, str, str | None], dict[str, Any]] = {}
    for row in rows:
        key = (
            row["platform"],
            row["error_category"],
            row["attempt_count"],
            row["action"],
            row["proposed_next_retry_at"],
        )
        if key not in groups_by_key:
            groups_by_key[key] = {
                "platform": row["platform"],
                "error_category": row["error_category"],
                "attempt_count": row["attempt_count"],
                "action": row["action"],
                "proposed_next_retry_at": row["proposed_next_retry_at"],
                "terminal_reason": row["terminal_reason"],
                "count": 0,
                "items": [],
            }
        group = groups_by_key[key]
        group["count"] += 1
        group["items"].append(row)
    return sorted(
        groups_by_key.values(),
        key=lambda group: (
            _action_rank(group["action"]),
            group["platform"],
            group["error_category"],
            group["attempt_count"],
            group["proposed_next_retry_at"] or "",
        ),
    )


def _backoff_rule(
    platform: str,
    category: PublishErrorCategory,
    rules: dict[str, dict[str, BackoffRule]],
) -> BackoffRule:
    platform_rules = rules.get(platform, {})
    default_rules = rules.get("default", {})
    return platform_rules.get(
        category,
        default_rules.get(
            category,
            BackoffRule(
                base_minutes=15,
                max_minutes=180,
                retryable=category not in TERMINAL_ERROR_CATEGORIES,
            ),
        ),
    )


def _parse_timestamp(value: str) -> datetime:
    text = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    return _aware(parsed)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _action_rank(action: str) -> int:
    ranks = {"retry": 0, "wait": 1, "terminal": 2}
    return ranks.get(action, 99)


__all__ = [
    "BackoffRule",
    "DEFAULT_BACKOFF_RULES",
    "DEFAULT_DAYS",
    "DEFAULT_MAX_ATTEMPTS",
    "KNOWN_ERROR_CATEGORIES",
    "build_retry_policy_plan",
    "calculate_next_retry_at",
    "format_retry_policy_plan_json",
    "format_retry_policy_plan_text",
]
