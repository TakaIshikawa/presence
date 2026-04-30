"""Publication latency SLO reporting."""

from __future__ import annotations

import json
import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


PLATFORMS = ("x", "bluesky")
VALID_PLATFORMS = {"all", *PLATFORMS}


@dataclass(frozen=True)
class QueueTarget:
    queue_id: int
    content_id: int
    platform: str
    queue_platform: str
    status: str | None
    created_at: str | None
    scheduled_at: str | None
    published_at: str | None


def build_publication_latency_report(
    db_or_conn: Any,
    *,
    days: int = 7,
    platform: str = "all",
    queued_threshold_minutes: float = 60.0,
    scheduled_threshold_minutes: float = 15.0,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a read-only latency report for queued publication targets."""
    if days <= 0:
        raise ValueError("days must be positive")
    if platform not in VALID_PLATFORMS:
        raise ValueError(f"invalid platform: {platform}")
    if queued_threshold_minutes < 0:
        raise ValueError("queued_threshold_minutes must be non-negative")
    if scheduled_threshold_minutes < 0:
        raise ValueError("scheduled_threshold_minutes must be non-negative")

    conn = _connection(db_or_conn)
    now = _aware(now or datetime.now(timezone.utc))
    cutoff = (now - timedelta(days=days)).isoformat()
    schema = _schema(conn)

    targets = _queue_targets(conn, schema, cutoff, platform)
    success_lookup = _success_lookup(conn, schema, targets)

    platform_reports: dict[str, dict[str, Any]] = {}
    slow_items: list[dict[str, Any]] = []
    missing_success_counts: dict[str, int] = {}

    for target in targets:
        report = platform_reports.setdefault(target.platform, _empty_platform_report())
        report["total"] += 1

        success_at, success_source = success_lookup.get(
            (target.queue_id, target.content_id, target.platform),
            (None, None),
        )
        if success_at is None:
            report["missing_success_count"] += 1
            missing_success_counts[target.platform] = (
                missing_success_counts.get(target.platform, 0) + 1
            )
            continue

        report["success_count"] += 1
        queued_latency = _latency_minutes(target.created_at, success_at)
        scheduled_latency = _latency_minutes(target.scheduled_at, success_at)
        if queued_latency is not None:
            report["_queued_latencies"].append(queued_latency)
        if scheduled_latency is not None:
            report["_scheduled_latencies"].append(scheduled_latency)

        exceeded: list[str] = []
        if queued_latency is not None and queued_latency > queued_threshold_minutes:
            exceeded.append("queued")
        if (
            scheduled_latency is not None
            and scheduled_latency > scheduled_threshold_minutes
        ):
            exceeded.append("scheduled")
        if exceeded:
            slow_items.append(
                {
                    "queue_id": target.queue_id,
                    "content_id": target.content_id,
                    "platform": target.platform,
                    "queue_platform": target.queue_platform,
                    "status": target.status,
                    "created_at": target.created_at,
                    "scheduled_at": target.scheduled_at,
                    "success_at": success_at,
                    "success_source": success_source,
                    "queued_latency_minutes": _round_latency(queued_latency),
                    "scheduled_latency_minutes": _round_latency(scheduled_latency),
                    "exceeded_thresholds": exceeded,
                }
            )

    for target_platform in _selected_platforms(platform):
        platform_reports.setdefault(target_platform, _empty_platform_report())
        missing_success_counts.setdefault(
            target_platform,
            platform_reports[target_platform]["missing_success_count"],
        )

    finalized_platforms = {
        name: _finalize_platform_report(data)
        for name, data in sorted(platform_reports.items())
        if platform == "all" or name == platform
    }

    slow_items.sort(
        key=lambda item: (
            item["platform"],
            -(item["queued_latency_minutes"] or 0),
            -(item["scheduled_latency_minutes"] or 0),
            item["queue_id"],
        )
    )

    return {
        "generated_at": now.isoformat(),
        "window_days": days,
        "platform": platform,
        "thresholds": {
            "queued_to_published_minutes": queued_threshold_minutes,
            "scheduled_to_published_minutes": scheduled_threshold_minutes,
        },
        "platforms": finalized_platforms,
        "missing_success_counts": {
            name: missing_success_counts.get(name, 0)
            for name in sorted(finalized_platforms)
        },
        "slow_items": slow_items,
    }


def format_publication_latency_json(report: dict[str, Any]) -> str:
    """Render a publication latency report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_publication_latency_text(report: dict[str, Any]) -> str:
    """Render a stable human-readable publication latency report."""
    lines = [
        "Publication latency SLO report",
        f"Generated: {report['generated_at']}",
        f"Window: {report['window_days']} days",
        (
            "Thresholds: queued_to_published="
            f"{report['thresholds']['queued_to_published_minutes']}m, "
            "scheduled_to_published="
            f"{report['thresholds']['scheduled_to_published_minutes']}m"
        ),
        "",
    ]

    if not any(data["total"] for data in report["platforms"].values()):
        lines.append("No publish queue items found.")
        return "\n".join(lines)

    columns = [
        ("platform", "PLATFORM", 10),
        ("total", "TOTAL", 5),
        ("success_count", "SUCCESS", 7),
        ("missing_success_count", "MISSING", 7),
        ("queued_p50_minutes", "Q_P50", 7),
        ("queued_p90_minutes", "Q_P90", 7),
        ("queued_max_minutes", "Q_MAX", 7),
        ("scheduled_p50_minutes", "S_P50", 7),
        ("scheduled_p90_minutes", "S_P90", 7),
        ("scheduled_max_minutes", "S_MAX", 7),
    ]
    lines.append("  ".join(label.ljust(width) for _, label, width in columns))
    lines.append("  ".join("-" * width for _, _, width in columns))
    for platform, data in sorted(report["platforms"].items()):
        rendered = {"platform": platform, **data}
        lines.append(
            "  ".join(
                _format_cell(rendered.get(key), width).ljust(width)
                for key, _, width in columns
            )
        )

    lines.extend(["", "Slow publications:"])
    if not report["slow_items"]:
        lines.append("No publications exceeded latency thresholds.")
        return "\n".join(lines)

    for item in report["slow_items"]:
        lines.append(
            "- "
            f"{item['platform']} queue={item['queue_id']} content={item['content_id']} "
            f"queued={_format_value(item['queued_latency_minutes'])}m "
            f"scheduled={_format_value(item['scheduled_latency_minutes'])}m "
            f"thresholds={','.join(item['exceeded_thresholds'])} "
            f"success={item['success_at']}"
        )
    return "\n".join(lines)


def _queue_targets(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    cutoff: str,
    platform: str,
) -> list[QueueTarget]:
    if "publish_queue" not in schema:
        return []

    columns = schema["publish_queue"]
    select_columns = {
        "id": "pq.id",
        "content_id": _column_expr(columns, "content_id"),
        "platform": _column_expr(columns, "platform", "'all'"),
        "status": _column_expr(columns, "status"),
        "created_at": _column_expr(columns, "created_at"),
        "scheduled_at": _column_expr(columns, "scheduled_at"),
        "published_at": _column_expr(columns, "published_at"),
    }
    filters: list[str] = []
    params: list[Any] = []
    if "created_at" in columns and "scheduled_at" in columns:
        filters.append("(pq.created_at >= ? OR pq.scheduled_at >= ?)")
        params.extend([cutoff, cutoff])
    elif "created_at" in columns:
        filters.append("pq.created_at >= ?")
        params.append(cutoff)
    elif "scheduled_at" in columns:
        filters.append("pq.scheduled_at >= ?")
        params.append(cutoff)

    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    rows = conn.execute(
        f"""SELECT
               {select_columns['id']} AS id,
               {select_columns['content_id']} AS content_id,
               {select_columns['platform']} AS platform,
               {select_columns['status']} AS status,
               {select_columns['created_at']} AS created_at,
               {select_columns['scheduled_at']} AS scheduled_at,
               {select_columns['published_at']} AS published_at
           FROM publish_queue pq
           {where_clause}
           ORDER BY pq.id ASC""",
        params,
    ).fetchall()

    targets: list[QueueTarget] = []
    selected = set(_selected_platforms(platform))
    for row in rows:
        queue_platform = row["platform"] or "all"
        for target_platform in _target_platforms(queue_platform):
            if target_platform not in selected:
                continue
            targets.append(
                QueueTarget(
                    queue_id=int(row["id"]),
                    content_id=int(row["content_id"] or 0),
                    platform=target_platform,
                    queue_platform=queue_platform,
                    status=row["status"],
                    created_at=row["created_at"],
                    scheduled_at=row["scheduled_at"],
                    published_at=row["published_at"],
                )
            )
    return targets


def _success_lookup(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    targets: list[QueueTarget],
) -> dict[tuple[int, int, str], tuple[str | None, str | None]]:
    lookup: dict[tuple[int, int, str], tuple[str | None, str | None]] = {}
    for target in targets:
        successes: list[tuple[str, str]] = []
        successes.extend(_attempt_successes(conn, schema, target))
        successes.extend(_publication_successes(conn, schema, target))
        successes.extend(_queue_successes(target))
        parsed = [
            (_parse_timestamp(timestamp), timestamp, source)
            for timestamp, source in successes
            if timestamp and _parse_timestamp(timestamp) is not None
        ]
        if parsed:
            parsed.sort(key=lambda item: item[0])
            lookup[(target.queue_id, target.content_id, target.platform)] = (
                parsed[0][1],
                parsed[0][2],
            )
    return lookup


def _attempt_successes(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    target: QueueTarget,
) -> list[tuple[str, str]]:
    columns = schema.get("publication_attempts")
    if not columns or not {"success", "attempted_at", "platform"}.issubset(columns):
        return []

    filters = ["pa.success = 1", "pa.platform = ?"]
    params: list[Any] = [target.platform]
    if "queue_id" in columns and "content_id" in columns:
        filters.append("(pa.queue_id = ? OR (pa.content_id = ? AND pa.queue_id IS NULL))")
        params.extend([target.queue_id, target.content_id])
    elif "queue_id" in columns:
        filters.append("pa.queue_id = ?")
        params.append(target.queue_id)
    elif "content_id" in columns:
        filters.append("pa.content_id = ?")
        params.append(target.content_id)
    else:
        return []

    rows = conn.execute(
        f"""SELECT pa.attempted_at
            FROM publication_attempts pa
            WHERE {' AND '.join(filters)}""",
        params,
    ).fetchall()
    return [(row["attempted_at"], "publication_attempts") for row in rows]


def _publication_successes(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    target: QueueTarget,
) -> list[tuple[str, str]]:
    columns = schema.get("content_publications")
    if not columns or not {"content_id", "platform", "published_at"}.issubset(columns):
        return []
    filters = ["cp.content_id = ?", "cp.platform = ?", "cp.published_at IS NOT NULL"]
    params: list[Any] = [target.content_id, target.platform]
    if "status" in columns:
        filters.append("(cp.status = 'published' OR cp.status IS NULL)")
    rows = conn.execute(
        f"""SELECT cp.published_at
            FROM content_publications cp
            WHERE {' AND '.join(filters)}""",
        params,
    ).fetchall()
    return [(row["published_at"], "content_publications") for row in rows]


def _queue_successes(target: QueueTarget) -> list[tuple[str, str]]:
    if target.published_at:
        return [(target.published_at, "publish_queue")]
    return []


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    return {
        table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        for table in tables
        if table
    }


def _empty_platform_report() -> dict[str, Any]:
    return {
        "total": 0,
        "success_count": 0,
        "missing_success_count": 0,
        "_queued_latencies": [],
        "_scheduled_latencies": [],
    }


def _finalize_platform_report(data: dict[str, Any]) -> dict[str, Any]:
    queued = data.pop("_queued_latencies")
    scheduled = data.pop("_scheduled_latencies")
    return {
        **data,
        "queued_p50_minutes": _percentile(queued, 50),
        "queued_p90_minutes": _percentile(queued, 90),
        "queued_max_minutes": _round_latency(max(queued)) if queued else None,
        "scheduled_p50_minutes": _percentile(scheduled, 50),
        "scheduled_p90_minutes": _percentile(scheduled, 90),
        "scheduled_max_minutes": _round_latency(max(scheduled)) if scheduled else None,
    }


def _percentile(values: list[float], percentile: int) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil((percentile / 100) * len(ordered)) - 1)
    return _round_latency(ordered[index])


def _latency_minutes(start: str | None, end: str | None) -> float | None:
    start_dt = _parse_timestamp(start)
    end_dt = _parse_timestamp(end)
    if start_dt is None or end_dt is None:
        return None
    return (end_dt - start_dt).total_seconds() / 60


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return _aware(parsed)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _round_latency(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value, 2)


def _selected_platforms(platform: str) -> list[str]:
    return list(PLATFORMS if platform == "all" else (platform,))


def _target_platforms(platform: str) -> list[str]:
    return list(PLATFORMS if platform == "all" else (platform,))


def _column_expr(columns: set[str], column: str, default: str = "NULL") -> str:
    if column in columns:
        return f"pq.{column}"
    return default


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _format_cell(value: Any, width: int) -> str:
    text = _format_value(value)
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


def _format_value(value: Any) -> str:
    if value is None:
        return "-"
    return str(value)
