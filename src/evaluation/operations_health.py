"""Operational health summary for background automation."""

from __future__ import annotations

import sqlite3
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any


@dataclass
class OperationsHealthThresholds:
    max_poll_age_minutes: int = 30
    max_reply_state_age_hours: int = 6
    max_platform_reply_state_age_hours: int = 6
    max_failed_queue_items: int = 0
    pipeline_window_hours: int = 24
    min_pipeline_runs_for_rejection_rate: int = 3
    max_pipeline_rejection_rate: float = 0.5
    max_engagement_fetch_age_hours: int = 36
    max_newsletter_weekly_unsubscribes: int = 5
    max_newsletter_churn_rate: float = 0.05
    max_api_rate_limit_snapshot_age_hours: int = 24
    api_rate_limit_min_remaining: dict[str, int] = field(default_factory=dict)
    max_model_cost_24h: float = 5.0
    max_single_run_model_cost: float = 1.0


def thresholds_from_config(config: Any) -> OperationsHealthThresholds:
    """Build health thresholds from the loaded app config."""
    source = getattr(config, "operations_health", None)
    if source is None:
        health = OperationsHealthThresholds()
    else:
        health = OperationsHealthThresholds(
            max_poll_age_minutes=source.max_poll_age_minutes,
            max_reply_state_age_hours=source.max_reply_state_age_hours,
            max_platform_reply_state_age_hours=source.max_platform_reply_state_age_hours,
            max_failed_queue_items=source.max_failed_queue_items,
            pipeline_window_hours=source.pipeline_window_hours,
            min_pipeline_runs_for_rejection_rate=source.min_pipeline_runs_for_rejection_rate,
            max_pipeline_rejection_rate=source.max_pipeline_rejection_rate,
            max_engagement_fetch_age_hours=source.max_engagement_fetch_age_hours,
            max_newsletter_weekly_unsubscribes=source.max_newsletter_weekly_unsubscribes,
            max_newsletter_churn_rate=source.max_newsletter_churn_rate,
            max_api_rate_limit_snapshot_age_hours=getattr(
                source,
                "max_api_rate_limit_snapshot_age_hours",
                24,
            ),
            max_model_cost_24h=getattr(source, "max_model_cost_24h", 5.0),
            max_single_run_model_cost=getattr(
                source,
                "max_single_run_model_cost",
                1.0,
            ),
        )
    rate_limits = getattr(config, "rate_limits", None)
    health.api_rate_limit_min_remaining = {
        provider: threshold
        for provider, threshold in {
            "x": _optional_int(getattr(rate_limits, "x_min_remaining", None)),
            "bluesky": _optional_int(getattr(rate_limits, "bluesky_min_remaining", None)),
            "anthropic": _optional_int(getattr(rate_limits, "anthropic_min_remaining", None)),
            "github": _optional_int(getattr(rate_limits, "github_min_remaining", None)),
        }.items()
        if threshold is not None and threshold > 0
    }
    return health


def summarize_operations_health(
    db_or_conn: Any,
    thresholds: OperationsHealthThresholds,
    now: datetime | None = None,
) -> dict:
    """Return a JSON-serializable operations health summary."""
    conn = _connection(db_or_conn)
    now = _aware(now or datetime.now(timezone.utc))

    checks = {
        "poll_state": _poll_state(conn, thresholds, now),
        "reply_state": _reply_state(conn, thresholds, now),
        "platform_reply_state": _platform_reply_state(conn, thresholds, now),
        "publish_queue": _publish_queue(conn, thresholds),
        "pipeline_runs": _pipeline_runs(conn, thresholds, now),
        "model_usage": _model_usage(conn, thresholds, now),
        "engagement_fetches": _engagement_fetches(conn, thresholds, now),
        "newsletter_audience": _newsletter_audience(conn, thresholds),
        "api_rate_limits": _api_rate_limits(conn, thresholds, now),
    }
    warnings = [
        message
        for check in checks.values()
        for message in check.get("warnings", [])
    ]

    return {
        "status": "warning" if warnings else "ok",
        "generated_at": now.isoformat(),
        "thresholds": asdict(thresholds),
        "checks": checks,
        "warnings": warnings,
    }


def format_operations_health(summary: dict) -> str:
    """Format a health summary for terminal output."""
    lines = [
        "=" * 70,
        "OPERATIONS HEALTH",
        "=" * 70,
        f"Status: {summary['status'].upper()}",
        f"Generated at: {summary['generated_at']}",
        "",
    ]

    checks = summary["checks"]
    poll = checks["poll_state"]
    lines.append(f"Poll state: {poll['status']}")
    lines.append(f"  Last poll: {poll.get('last_poll_time') or 'none'}")
    if poll.get("age_minutes") is not None:
        lines.append(f"  Age: {poll['age_minutes']:.1f} minutes")

    reply = checks["reply_state"]
    lines.append(f"Reply state: {reply['status']}")
    lines.append(f"  Updated at: {reply.get('updated_at') or 'none'}")

    platform_reply = checks["platform_reply_state"]
    lines.append(f"Platform reply state: {platform_reply['status']}")
    for platform, data in sorted(platform_reply.get("platforms", {}).items()):
        age = data.get("age_hours")
        age_text = f", age {age:.1f}h" if age is not None else ""
        lines.append(f"  {platform}: {data.get('updated_at') or 'none'}{age_text}")

    queue = checks["publish_queue"]
    lines.append(f"Publish queue: {queue['status']}")
    lines.append(f"  Failed items: {queue['failed_count']}")

    pipeline = checks["pipeline_runs"]
    lines.append(f"Pipeline runs: {pipeline['status']}")
    lines.append(
        f"  Recent runs: {pipeline['total_runs']} "
        f"(rejected {pipeline['rejected_runs']}, "
        f"rate {pipeline['rejection_rate'] * 100:.1f}%)"
    )

    model_usage = checks["model_usage"]
    lines.append(f"Model usage: {model_usage['status']}")
    lines.append(f"  24h cost: ${model_usage['total_cost_24h']:.4f}")
    max_run = model_usage.get("max_run") or {}
    lines.append(
        f"  Max run cost: ${model_usage['max_run_cost']:.4f}"
        + (f" ({max_run.get('batch_id')})" if max_run.get("batch_id") else "")
    )
    if model_usage.get("top_operations"):
        lines.append("  Top operations:")
        for operation in model_usage["top_operations"]:
            lines.append(
                f"    {operation['operation_name']}: "
                f"${operation['estimated_cost']:.4f}"
            )

    engagement = checks["engagement_fetches"]
    lines.append(f"Engagement fetches: {engagement['status']}")
    for platform, data in sorted(engagement.get("platforms", {}).items()):
        last = data.get("last_fetched_at") or "none"
        age = data.get("age_hours")
        age_text = f", age {age:.1f}h" if age is not None else ""
        lines.append(
            f"  {platform}: tracked {data['tracked_posts']}, "
            f"missing {data['missing_fetches']}, last {last}{age_text}"
        )

    newsletter = checks["newsletter_audience"]
    if newsletter.get("latest_fetched_at"):
        lines.append(f"Newsletter audience: {newsletter['status']}")
        lines.append(f"  Latest fetched: {newsletter['latest_fetched_at']}")
        lines.append(
            f"  Subscribers: {newsletter['subscriber_count']} "
            f"(active {newsletter.get('active_subscriber_count') or 'unknown'})"
        )
        lines.append(
            f"  Weekly unsubscribes: "
            f"{newsletter.get('weekly_unsubscribes') or 0}"
        )
        churn_rate = newsletter.get("churn_rate")
        churn_text = f"{churn_rate * 100:.2f}%" if churn_rate is not None else "unknown"
        lines.append(f"  Churn rate: {churn_text}")

    api_limits = checks["api_rate_limits"]
    lines.append(f"API rate limits: {api_limits['status']}")
    for key, data in sorted(api_limits.get("snapshots", {}).items()):
        age = data.get("age_hours")
        age_text = f", age {age:.1f}h" if age is not None else ""
        limit = data.get("limit")
        budget = f"{data['remaining']}/{limit}" if limit is not None else str(data["remaining"])
        lines.append(f"  {key}: remaining {budget}{age_text}")

    if summary["warnings"]:
        lines.extend(["", "Warnings:"])
        lines.extend(f"  - {warning}" for warning in summary["warnings"])

    return "\n".join(lines)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _poll_state(
    conn: sqlite3.Connection,
    thresholds: OperationsHealthThresholds,
    now: datetime,
) -> dict:
    row = _one(conn, "SELECT last_poll_time FROM poll_state WHERE id = 1")
    if row is None:
        return _warning("poll_state row is missing", last_poll_time=None, age_minutes=None)

    timestamp = row["last_poll_time"]
    age_minutes = _age(now, timestamp).total_seconds() / 60
    warnings = []
    if age_minutes > thresholds.max_poll_age_minutes:
        warnings.append(
            f"poll_state is stale: {age_minutes:.1f}m "
            f"> {thresholds.max_poll_age_minutes}m"
        )
    return {
        "status": _status(warnings),
        "last_poll_time": timestamp,
        "age_minutes": round(age_minutes, 2),
        "warnings": warnings,
    }


def _reply_state(
    conn: sqlite3.Connection,
    thresholds: OperationsHealthThresholds,
    now: datetime,
) -> dict:
    row = _one(conn, "SELECT last_mention_id, updated_at FROM reply_state WHERE id = 1")
    if row is None:
        return _warning("reply_state row is missing", updated_at=None, age_hours=None)

    age_hours = _age(now, row["updated_at"]).total_seconds() / 3600
    warnings = []
    if age_hours > thresholds.max_reply_state_age_hours:
        warnings.append(
            f"reply_state is stale: {age_hours:.1f}h "
            f"> {thresholds.max_reply_state_age_hours}h"
        )
    return {
        "status": _status(warnings),
        "last_mention_id": row["last_mention_id"],
        "updated_at": row["updated_at"],
        "age_hours": round(age_hours, 2),
        "warnings": warnings,
    }


def _platform_reply_state(
    conn: sqlite3.Connection,
    thresholds: OperationsHealthThresholds,
    now: datetime,
) -> dict:
    rows = _all(
        conn,
        "SELECT platform, cursor, updated_at FROM platform_reply_state ORDER BY platform",
    )
    warnings = []
    platforms = {}
    for row in rows:
        age_hours = _age(now, row["updated_at"]).total_seconds() / 3600
        platform = row["platform"]
        platforms[platform] = {
            "cursor": row["cursor"],
            "updated_at": row["updated_at"],
            "age_hours": round(age_hours, 2),
        }
        if age_hours > thresholds.max_platform_reply_state_age_hours:
            warnings.append(
                f"platform_reply_state[{platform}] is stale: {age_hours:.1f}h "
                f"> {thresholds.max_platform_reply_state_age_hours}h"
            )
    if not rows:
        warnings.append("platform_reply_state has no platform rows")
    return {"status": _status(warnings), "platforms": platforms, "warnings": warnings}


def _publish_queue(
    conn: sqlite3.Connection,
    thresholds: OperationsHealthThresholds,
) -> dict:
    rows = _all(
        conn,
        """SELECT platform, COUNT(*) AS count
           FROM publish_queue
           WHERE status = 'failed'
           GROUP BY platform""",
    )
    by_platform = {row["platform"]: row["count"] for row in rows}
    failed_count = sum(by_platform.values())
    warnings = []
    if failed_count > thresholds.max_failed_queue_items:
        warnings.append(
            f"publish_queue has {failed_count} failed items "
            f"> {thresholds.max_failed_queue_items}"
        )
    return {
        "status": _status(warnings),
        "failed_count": failed_count,
        "failed_by_platform": by_platform,
        "warnings": warnings,
    }


def _pipeline_runs(
    conn: sqlite3.Connection,
    thresholds: OperationsHealthThresholds,
    now: datetime,
) -> dict:
    since = (now - timedelta(hours=thresholds.pipeline_window_hours)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    rows = _all(
        conn,
        """SELECT outcome, COUNT(*) AS count
           FROM pipeline_runs
           WHERE created_at >= ?
           GROUP BY outcome""",
        (since,),
    )
    outcomes = {row["outcome"] or "unknown": row["count"] for row in rows}
    total_runs = sum(outcomes.values())
    rejected_runs = sum(
        count
        for outcome, count in outcomes.items()
        if outcome not in {"published", "dry_run"}
    )
    rejection_rate = rejected_runs / total_runs if total_runs else 0.0
    warnings = []
    if (
        total_runs >= thresholds.min_pipeline_runs_for_rejection_rate
        and rejection_rate > thresholds.max_pipeline_rejection_rate
    ):
        warnings.append(
            f"pipeline rejection rate is high: {rejection_rate * 100:.1f}% "
            f"> {thresholds.max_pipeline_rejection_rate * 100:.1f}%"
        )
    return {
        "status": _status(warnings),
        "window_hours": thresholds.pipeline_window_hours,
        "total_runs": total_runs,
        "rejected_runs": rejected_runs,
        "rejection_rate": round(rejection_rate, 4),
        "outcomes": outcomes,
        "warnings": warnings,
    }


def _model_usage(
    conn: sqlite3.Connection,
    thresholds: OperationsHealthThresholds,
    now: datetime,
) -> dict:
    if not _has_table(conn, "model_usage"):
        return _model_usage_empty()

    since = (now - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
    total_row = _one(
        conn,
        """SELECT COALESCE(SUM(estimated_cost), 0) AS total_cost,
                  COUNT(*) AS usage_events
           FROM model_usage
           WHERE datetime(created_at) >= datetime(?)""",
        (since,),
    )
    total_cost = float(total_row["total_cost"] or 0.0)
    usage_events = int(total_row["usage_events"] or 0)

    max_run_row = _one(
        conn,
        """SELECT m.pipeline_run_id, pr.batch_id, pr.content_type, pr.outcome,
                  COALESCE(SUM(m.estimated_cost), 0) AS estimated_cost
           FROM model_usage m
           JOIN pipeline_runs pr ON pr.id = m.pipeline_run_id
           WHERE datetime(m.created_at) >= datetime(?)
           GROUP BY m.pipeline_run_id, pr.batch_id, pr.content_type, pr.outcome
           ORDER BY estimated_cost DESC
           LIMIT 1""",
        (since,),
    )
    max_run_cost = float(max_run_row["estimated_cost"] or 0.0) if max_run_row else 0.0
    max_run = (
        {
            "pipeline_run_id": max_run_row["pipeline_run_id"],
            "batch_id": max_run_row["batch_id"],
            "content_type": max_run_row["content_type"],
            "outcome": max_run_row["outcome"],
            "estimated_cost": round(max_run_cost, 6),
        }
        if max_run_row
        else None
    )

    operation_rows = _all(
        conn,
        """SELECT operation_name,
                  COALESCE(SUM(estimated_cost), 0) AS estimated_cost,
                  COUNT(*) AS usage_events
           FROM model_usage
           WHERE datetime(created_at) >= datetime(?)
           GROUP BY operation_name
           ORDER BY estimated_cost DESC, operation_name
           LIMIT 5""",
        (since,),
    )
    top_operations = [
        {
            "operation_name": row["operation_name"],
            "estimated_cost": round(float(row["estimated_cost"] or 0.0), 6),
            "usage_events": row["usage_events"],
        }
        for row in operation_rows
    ]

    warnings = []
    if total_cost > thresholds.max_model_cost_24h:
        warnings.append(
            f"model usage cost is high: ${total_cost:.4f} "
            f"> ${thresholds.max_model_cost_24h:.4f} in 24h"
        )
    if max_run_cost > thresholds.max_single_run_model_cost:
        run_label = (
            max_run["batch_id"] if max_run and max_run.get("batch_id") else "unknown"
        )
        warnings.append(
            f"single pipeline run model cost is high for {run_label}: "
            f"${max_run_cost:.4f} > ${thresholds.max_single_run_model_cost:.4f}"
        )

    return {
        "status": _status(warnings),
        "window_hours": 24,
        "total_cost_24h": round(total_cost, 6),
        "total_estimated_cost_24h": round(total_cost, 6),
        "usage_events": usage_events,
        "max_run_cost": round(max_run_cost, 6),
        "max_run_estimated_cost": round(max_run_cost, 6),
        "max_run": max_run,
        "top_operations": top_operations,
        "top_operation_costs": top_operations,
        "warnings": warnings,
    }


def _model_usage_empty() -> dict:
    return {
        "status": "ok",
        "window_hours": 24,
        "total_cost_24h": 0.0,
        "total_estimated_cost_24h": 0.0,
        "usage_events": 0,
        "max_run_cost": 0.0,
        "max_run_estimated_cost": 0.0,
        "max_run": None,
        "top_operations": [],
        "top_operation_costs": [],
        "warnings": [],
    }


def _engagement_fetches(
    conn: sqlite3.Connection,
    thresholds: OperationsHealthThresholds,
    now: datetime,
) -> dict:
    platforms = {
        "x": _engagement_platform(
            conn=conn,
            platform="x",
            id_column="tweet_id",
            engagement_table="post_engagement",
            thresholds=thresholds,
            now=now,
        )
    }
    if _has_column(conn, "generated_content", "bluesky_uri"):
        platforms["bluesky"] = _engagement_platform(
            conn=conn,
            platform="bluesky",
            id_column="bluesky_uri",
            engagement_table="bluesky_engagement",
            thresholds=thresholds,
            now=now,
        )

    warnings = [
        warning
        for data in platforms.values()
        for warning in data.get("warnings", [])
    ]
    return {"status": _status(warnings), "platforms": platforms, "warnings": warnings}


def _newsletter_audience(
    conn: sqlite3.Connection,
    thresholds: OperationsHealthThresholds,
) -> dict:
    if not _has_table(conn, "newsletter_subscriber_metrics"):
        return _newsletter_audience_empty()

    row = _one(
        conn,
        """SELECT subscriber_count, active_subscriber_count, unsubscribes,
                  churn_rate, new_subscribers, net_subscriber_change, fetched_at
           FROM newsletter_subscriber_metrics
           ORDER BY fetched_at DESC, id DESC
           LIMIT 1""",
    )
    if row is None:
        return _newsletter_audience_empty()

    weekly_unsubscribes = row["unsubscribes"]
    churn_rate = row["churn_rate"]
    warnings = []
    if (
        weekly_unsubscribes is not None
        and weekly_unsubscribes > thresholds.max_newsletter_weekly_unsubscribes
    ):
        warnings.append(
            f"newsletter weekly unsubscribes are high: {weekly_unsubscribes} "
            f"> {thresholds.max_newsletter_weekly_unsubscribes}"
        )
    if churn_rate is not None and churn_rate > thresholds.max_newsletter_churn_rate:
        warnings.append(
            f"newsletter churn rate is high: {churn_rate * 100:.2f}% "
            f"> {thresholds.max_newsletter_churn_rate * 100:.2f}%"
        )

    return {
        "status": _status(warnings),
        "subscriber_count": row["subscriber_count"],
        "active_subscriber_count": row["active_subscriber_count"],
        "weekly_unsubscribes": weekly_unsubscribes,
        "churn_rate": churn_rate,
        "new_subscribers": row["new_subscribers"],
        "net_subscriber_change": row["net_subscriber_change"],
        "latest_fetched_at": row["fetched_at"],
        "warnings": warnings,
    }


def _newsletter_audience_empty() -> dict:
    return {
        "status": "ok",
        "subscriber_count": None,
        "active_subscriber_count": None,
        "weekly_unsubscribes": None,
        "churn_rate": None,
        "new_subscribers": None,
        "net_subscriber_change": None,
        "latest_fetched_at": None,
        "warnings": [],
    }


def _engagement_platform(
    conn: sqlite3.Connection,
    platform: str,
    id_column: str,
    engagement_table: str,
    thresholds: OperationsHealthThresholds,
    now: datetime,
) -> dict:
    if not _has_table(conn, engagement_table):
        return _warning(
            f"{engagement_table} table is missing",
            tracked_posts=0,
            missing_fetches=0,
            last_fetched_at=None,
            age_hours=None,
        )

    row = _one(
        conn,
        f"""SELECT COUNT(*) AS tracked_posts,
                  SUM(CASE WHEN latest.fetched_at IS NULL THEN 1 ELSE 0 END)
                      AS missing_fetches,
                  MAX(latest.fetched_at) AS last_fetched_at
           FROM generated_content gc
           LEFT JOIN (
               SELECT content_id, MAX(fetched_at) AS fetched_at
               FROM {engagement_table}
               GROUP BY content_id
           ) latest ON latest.content_id = gc.id
           WHERE gc.published = 1
             AND gc.{id_column} IS NOT NULL""",
    )
    tracked_posts = row["tracked_posts"] or 0
    missing_fetches = row["missing_fetches"] or 0
    last_fetched = row["last_fetched_at"]
    age_hours = None
    warnings = []

    if tracked_posts and missing_fetches:
        warnings.append(f"{platform} engagement has {missing_fetches} missing fetches")
    if tracked_posts and last_fetched:
        age_hours = _age(now, last_fetched).total_seconds() / 3600
        if age_hours > thresholds.max_engagement_fetch_age_hours:
            warnings.append(
                f"{platform} engagement fetch is stale: {age_hours:.1f}h "
                f"> {thresholds.max_engagement_fetch_age_hours}h"
            )
    elif tracked_posts:
        warnings.append(f"{platform} engagement has no fetch timestamp")

    return {
        "status": _status(warnings),
        "tracked_posts": tracked_posts,
        "missing_fetches": missing_fetches,
        "last_fetched_at": last_fetched,
        "age_hours": round(age_hours, 2) if age_hours is not None else None,
        "warnings": warnings,
    }


def _api_rate_limits(
    conn: sqlite3.Connection,
    thresholds: OperationsHealthThresholds,
    now: datetime,
) -> dict:
    if not _has_table(conn, "api_rate_limit_snapshots"):
        return {"status": "ok", "snapshots": {}, "warnings": []}

    rows = _all(
        conn,
        """SELECT id, provider, endpoint, remaining, limit_value,
                  reset_at, fetched_at
           FROM (
               SELECT s.*,
                      ROW_NUMBER() OVER (
                          PARTITION BY provider, endpoint
                          ORDER BY fetched_at DESC, id DESC
                      ) AS rn
               FROM api_rate_limit_snapshots s
           )
           WHERE rn = 1
           ORDER BY provider, endpoint""",
    )
    warnings = []
    snapshots = {}
    for row in rows:
        provider = row["provider"]
        endpoint = row["endpoint"] or "default"
        key = f"{provider}:{endpoint}"
        age_hours = _age(now, row["fetched_at"]).total_seconds() / 3600
        remaining = row["remaining"]
        threshold = thresholds.api_rate_limit_min_remaining.get(provider)
        snapshots[key] = {
            "provider": provider,
            "endpoint": endpoint,
            "remaining": remaining,
            "limit": row["limit_value"],
            "reset_at": row["reset_at"],
            "fetched_at": row["fetched_at"],
            "age_hours": round(age_hours, 2),
            "threshold": threshold,
        }
        if threshold is not None and remaining <= threshold:
            warnings.append(
                f"{provider} API rate limit for {endpoint} is low: "
                f"{remaining} <= {threshold}"
            )
        if age_hours > thresholds.max_api_rate_limit_snapshot_age_hours:
            warnings.append(
                f"{provider} API rate limit snapshot for {endpoint} is stale: "
                f"{age_hours:.1f}h > {thresholds.max_api_rate_limit_snapshot_age_hours}h"
            )

    return {"status": _status(warnings), "snapshots": snapshots, "warnings": warnings}


def _status(warnings: list[str]) -> str:
    return "warning" if warnings else "ok"


def _optional_int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _warning(message: str, **fields: Any) -> dict:
    return {"status": "warning", "warnings": [message], **fields}


def _one(
    conn: sqlite3.Connection,
    query: str,
    params: tuple = (),
) -> sqlite3.Row | None:
    previous = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        return conn.execute(query, params).fetchone()
    finally:
        conn.row_factory = previous


def _all(
    conn: sqlite3.Connection,
    query: str,
    params: tuple = (),
) -> list[sqlite3.Row]:
    previous = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        return conn.execute(query, params).fetchall()
    finally:
        conn.row_factory = previous


def _has_table(conn: sqlite3.Connection, table: str) -> bool:
    row = _one(
        conn,
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    )
    return row is not None


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = _all(conn, f"PRAGMA table_info({table})")
    return any(row["name"] == column for row in rows)


def _age(now: datetime, timestamp: str) -> timedelta:
    parsed = _parse_datetime(timestamp)
    if parsed > now:
        return timedelta(0)
    return now - parsed


def _parse_datetime(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    return _aware(parsed)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
