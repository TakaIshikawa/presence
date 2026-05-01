"""Read-only throughput forecast for publish queue backlog clearance."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import math
import sqlite3
from typing import Any


DEFAULT_LOOKBACK_DAYS = 14
DEFAULT_HORIZON_DAYS = 7
BACKLOG_STATUSES = ("queued", "held")


@dataclass(frozen=True)
class AtRiskPublishQueueItem:
    """One active backlog item scheduled before forecast generation."""

    queue_id: int
    content_id: int | None
    platform: str
    status: str
    scheduled_at: str
    overdue_days: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PublishQueuePlatformForecast:
    """Backlog and clearance forecast for one queue platform."""

    platform: str
    backlog_count: int
    backlog_by_status: dict[str, int]
    recent_success_count: int
    recent_daily_throughput: float
    estimated_clearance_days: float | None
    at_risk_count: int
    recommendation: str
    at_risk_items: tuple[AtRiskPublishQueueItem, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "platform": self.platform,
            "backlog_count": self.backlog_count,
            "backlog_by_status": dict(sorted(self.backlog_by_status.items())),
            "recent_success_count": self.recent_success_count,
            "recent_daily_throughput": self.recent_daily_throughput,
            "estimated_clearance_days": self.estimated_clearance_days,
            "at_risk_count": self.at_risk_count,
            "recommendation": self.recommendation,
            "at_risk_items": [item.to_dict() for item in self.at_risk_items],
        }


@dataclass(frozen=True)
class PublishQueueThroughputForecast:
    """Read-only publish queue backlog throughput forecast."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    platforms: tuple[PublishQueuePlatformForecast, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "filters": self.filters,
            "totals": self.totals,
            "platforms": [platform.to_dict() for platform in self.platforms],
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
        }


def build_publish_queue_throughput_forecast(
    db_or_conn: Any,
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    horizon_days: int = DEFAULT_HORIZON_DAYS,
    platform: str = "all",
    now: datetime | None = None,
) -> PublishQueueThroughputForecast:
    """Estimate when queued and held publish queue backlog will clear."""
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")
    if horizon_days <= 0:
        raise ValueError("horizon_days must be positive")
    selected_platform = str(platform).strip().lower()
    if not selected_platform:
        raise ValueError("platform must be non-empty")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=lookback_days)
    horizon_end = generated_at + timedelta(days=horizon_days)
    conn = _connection(db_or_conn)
    schema = _schema(conn)

    missing_tables: set[str] = set()
    missing_columns: dict[str, tuple[str, ...]] = {}
    backlog_rows = _backlog_rows(
        conn,
        schema,
        selected_platform,
        missing_tables,
        missing_columns,
    )
    successes = _recent_successes(
        conn,
        schema,
        cutoff,
        generated_at,
        selected_platform,
        missing_tables,
        missing_columns,
    )
    platforms = _platform_forecasts(
        backlog_rows,
        successes,
        generated_at,
        lookback_days,
        horizon_days,
    )

    backlog_total = sum(forecast.backlog_count for forecast in platforms)
    at_risk_total = sum(forecast.at_risk_count for forecast in platforms)
    recent_success_total = sum(forecast.recent_success_count for forecast in platforms)
    recommendations = tuple(sorted({forecast.recommendation for forecast in platforms}))

    return PublishQueueThroughputForecast(
        generated_at=generated_at.isoformat(),
        filters={
            "lookback_days": lookback_days,
            "horizon_days": horizon_days,
            "platform": selected_platform,
            "lookback_start": cutoff.isoformat(),
            "lookback_end": generated_at.isoformat(),
            "horizon_end": horizon_end.isoformat(),
        },
        totals={
            "backlog_count": backlog_total,
            "queued_count": sum(
                forecast.backlog_by_status.get("queued", 0) for forecast in platforms
            ),
            "held_count": sum(
                forecast.backlog_by_status.get("held", 0) for forecast in platforms
            ),
            "at_risk_count": at_risk_total,
            "recent_success_count": recent_success_total,
            "recent_daily_throughput": _round_rate(recent_success_total / lookback_days),
            "recommendations": list(recommendations),
            "platform_count": len(platforms),
        },
        platforms=tuple(platforms),
        missing_tables=tuple(sorted(missing_tables)),
        missing_columns=missing_columns,
    )


def format_publish_queue_throughput_forecast_json(
    forecast: PublishQueueThroughputForecast,
) -> str:
    """Serialize a throughput forecast as deterministic JSON."""
    return json.dumps(forecast.to_dict(), indent=2, sort_keys=True)


def format_publish_queue_throughput_forecast_text(
    forecast: PublishQueueThroughputForecast,
) -> str:
    """Format a throughput forecast for terminal review."""
    lines = [
        "Publish Queue Throughput Forecast",
        f"Generated: {forecast.generated_at}",
        (
            f"Lookback: {forecast.filters['lookback_days']} days "
            f"({forecast.filters['lookback_start']} to {forecast.filters['lookback_end']})"
        ),
        (
            f"Horizon: {forecast.filters['horizon_days']} days "
            f"({forecast.filters['horizon_end']})"
        ),
        f"Platform: {forecast.filters['platform']}",
        (
            f"Backlog: {forecast.totals['backlog_count']} "
            f"(queued={forecast.totals['queued_count']}, held={forecast.totals['held_count']})"
        ),
        (
            f"Recent throughput: {forecast.totals['recent_success_count']} successes, "
            f"{forecast.totals['recent_daily_throughput']}/day"
        ),
        f"At risk: {forecast.totals['at_risk_count']}",
        "Recommendations: " + ", ".join(forecast.totals["recommendations"] or ["normal"]),
    ]
    if forecast.missing_tables:
        lines.append("Missing optional tables: " + ", ".join(forecast.missing_tables))
    if forecast.missing_columns:
        details = ", ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(forecast.missing_columns.items())
        )
        lines.append("Missing columns: " + details)

    if not forecast.platforms:
        lines.append("No queued or held backlog found.")
        return "\n".join(lines)

    lines.append("Platform forecasts:")
    for platform in forecast.platforms:
        clearance = (
            "unknown"
            if platform.estimated_clearance_days is None
            else f"{platform.estimated_clearance_days} days"
        )
        status_counts = ", ".join(
            f"{status}={count}" for status, count in sorted(platform.backlog_by_status.items())
        )
        lines.append(
            f"- {platform.platform}: backlog={platform.backlog_count} "
            f"({status_counts}); throughput={platform.recent_daily_throughput}/day; "
            f"clearance={clearance}; at_risk={platform.at_risk_count}; "
            f"recommendation={platform.recommendation}"
        )
        for item in platform.at_risk_items:
            lines.append(
                f"  at_risk queue={item.queue_id} content={item.content_id} "
                f"status={item.status} scheduled_at={item.scheduled_at} "
                f"overdue_days={item.overdue_days}"
            )
    return "\n".join(lines)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


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


def _backlog_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    platform: str,
    missing_tables: set[str],
    missing_columns: dict[str, tuple[str, ...]],
) -> list[dict[str, Any]]:
    required = ("id", "scheduled_at", "platform", "status")
    if "publish_queue" not in schema:
        missing_tables.add("publish_queue")
        return []
    missing = tuple(column for column in required if column not in schema["publish_queue"])
    if missing:
        missing_columns["publish_queue"] = missing
        return []

    select_columns = ["id", "scheduled_at", "platform", "status"]
    if "content_id" in schema["publish_queue"]:
        select_columns.append("content_id")
    filters = ["status IN (?, ?)"]
    params: list[Any] = list(BACKLOG_STATUSES)
    if platform != "all":
        filters.append("LOWER(platform) = ?")
        params.append(platform)
    rows = _fetch_dicts(
        conn,
        f"""SELECT {', '.join(select_columns)}
            FROM publish_queue
            WHERE {' AND '.join(filters)}
            ORDER BY platform ASC, scheduled_at ASC, id ASC""",
        params,
    )
    return rows


def _recent_successes(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    cutoff: datetime,
    now: datetime,
    platform: str,
    missing_tables: set[str],
    missing_columns: dict[str, tuple[str, ...]],
) -> list[dict[str, Any]]:
    successes = _publication_successes(
        conn,
        schema,
        cutoff,
        now,
        platform,
        missing_tables,
        missing_columns,
    )
    successes.extend(
        _queue_successes(
            conn,
            schema,
            cutoff,
            now,
            platform,
            successes,
            missing_tables,
            missing_columns,
        )
    )
    return successes


def _publication_successes(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    cutoff: datetime,
    now: datetime,
    platform: str,
    missing_tables: set[str],
    missing_columns: dict[str, tuple[str, ...]],
) -> list[dict[str, Any]]:
    if "content_publications" not in schema:
        missing_tables.add("content_publications")
        return []
    required = ("content_id", "platform", "published_at")
    missing = tuple(
        column for column in required if column not in schema["content_publications"]
    )
    if missing:
        missing_columns["content_publications"] = missing
        return []

    filters = ["published_at >= ?", "published_at < ?", "published_at IS NOT NULL"]
    params: list[Any] = [cutoff.isoformat(), now.isoformat()]
    if "status" in schema["content_publications"]:
        filters.append("status = 'published'")
    if platform != "all":
        filters.append("LOWER(platform) = ?")
        params.append(platform)
    rows = _fetch_dicts(
        conn,
        f"""SELECT content_id, platform, published_at
            FROM content_publications
            WHERE {' AND '.join(filters)}
            ORDER BY platform ASC, published_at ASC, content_id ASC""",
        params,
    )
    return [
        {
            "content_id": row["content_id"],
            "platform": _normalize_platform(row["platform"]),
            "published_at": row["published_at"],
            "source": "content_publications",
        }
        for row in rows
        if _parse_timestamp(row["published_at"]) is not None
    ]


def _queue_successes(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    cutoff: datetime,
    now: datetime,
    platform: str,
    existing_successes: list[dict[str, Any]],
    missing_tables: set[str],
    missing_columns: dict[str, tuple[str, ...]],
) -> list[dict[str, Any]]:
    if "publish_queue" not in schema:
        missing_tables.add("publish_queue")
        return []
    required = ("id", "platform", "status", "published_at")
    missing = tuple(column for column in required if column not in schema["publish_queue"])
    if missing:
        existing = set(missing_columns.get("publish_queue", ()))
        missing_columns["publish_queue"] = tuple(sorted(existing.union(missing)))
        return []

    select_columns = ["id", "platform", "published_at"]
    if "content_id" in schema["publish_queue"]:
        select_columns.append("content_id")
    filters = [
        "status = 'published'",
        "published_at >= ?",
        "published_at < ?",
        "published_at IS NOT NULL",
    ]
    params: list[Any] = [cutoff.isoformat(), now.isoformat()]
    if platform != "all":
        filters.append("LOWER(platform) = ?")
        params.append(platform)
    rows = _fetch_dicts(
        conn,
        f"""SELECT {', '.join(select_columns)}
            FROM publish_queue
            WHERE {' AND '.join(filters)}
            ORDER BY platform ASC, published_at ASC, id ASC""",
        params,
    )

    seen = {
        (success.get("content_id"), success["platform"])
        for success in existing_successes
        if success.get("content_id") is not None
    }
    successes: list[dict[str, Any]] = []
    for row in rows:
        published_at = row["published_at"]
        if _parse_timestamp(published_at) is None:
            continue
        normalized_platform = _normalize_platform(row["platform"])
        content_id = row.get("content_id")
        if content_id is not None and (content_id, normalized_platform) in seen:
            continue
        successes.append(
            {
                "content_id": content_id,
                "platform": normalized_platform,
                "published_at": published_at,
                "source": "publish_queue",
            }
        )
    return successes


def _platform_forecasts(
    backlog_rows: list[dict[str, Any]],
    successes: list[dict[str, Any]],
    now: datetime,
    lookback_days: int,
    horizon_days: int,
) -> list[PublishQueuePlatformForecast]:
    backlog_by_platform: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in backlog_rows:
        backlog_by_platform[_normalize_platform(row.get("platform"))].append(row)
    successes_by_platform = Counter(_normalize_platform(row.get("platform")) for row in successes)
    platform_names = sorted(set(backlog_by_platform) | set(successes_by_platform))

    drafts: list[dict[str, Any]] = []
    for platform in platform_names:
        rows = sorted(
            backlog_by_platform.get(platform, []),
            key=lambda row: (str(row.get("scheduled_at") or ""), int(row.get("id") or 0)),
        )
        backlog_by_status = Counter(_normalize_status(row.get("status")) for row in rows)
        success_count = successes_by_platform.get(platform, 0)
        throughput = success_count / lookback_days
        estimated = _estimated_clearance_days(len(rows), throughput)
        at_risk_items = tuple(_at_risk_items(rows, now))
        drafts.append(
            {
                "platform": platform,
                "backlog_count": len(rows),
                "backlog_by_status": dict(backlog_by_status),
                "recent_success_count": success_count,
                "recent_daily_throughput": _round_rate(throughput),
                "estimated_clearance_days": estimated,
                "at_risk_items": at_risk_items,
            }
        )

    for draft in drafts:
        draft["recommendation"] = _recommendation(draft, drafts, horizon_days)

    return [
        PublishQueuePlatformForecast(
            platform=draft["platform"],
            backlog_count=draft["backlog_count"],
            backlog_by_status=draft["backlog_by_status"],
            recent_success_count=draft["recent_success_count"],
            recent_daily_throughput=draft["recent_daily_throughput"],
            estimated_clearance_days=draft["estimated_clearance_days"],
            at_risk_count=len(draft["at_risk_items"]),
            recommendation=draft["recommendation"],
            at_risk_items=draft["at_risk_items"],
        )
        for draft in drafts
    ]


def _at_risk_items(
    rows: list[dict[str, Any]],
    now: datetime,
) -> list[AtRiskPublishQueueItem]:
    items: list[AtRiskPublishQueueItem] = []
    for row in rows:
        scheduled_at = _parse_timestamp(row.get("scheduled_at"))
        if scheduled_at is None or scheduled_at >= now:
            continue
        overdue_days = (now - scheduled_at).total_seconds() / 86400
        items.append(
            AtRiskPublishQueueItem(
                queue_id=int(row["id"]),
                content_id=(
                    int(row["content_id"]) if row.get("content_id") is not None else None
                ),
                platform=_normalize_platform(row.get("platform")),
                status=_normalize_status(row.get("status")),
                scheduled_at=str(row["scheduled_at"]),
                overdue_days=round(overdue_days, 2),
            )
        )
    return items


def _recommendation(
    draft: dict[str, Any],
    all_drafts: list[dict[str, Any]],
    horizon_days: int,
) -> str:
    if draft["backlog_count"] == 0:
        return "normal"
    estimated = draft["estimated_clearance_days"]
    if estimated is not None and estimated <= horizon_days and not draft["at_risk_items"]:
        return "normal"
    if _has_rebalance_capacity(draft, all_drafts, horizon_days):
        return "rebalance_platform"
    return "reduce_intake"


def _has_rebalance_capacity(
    draft: dict[str, Any],
    all_drafts: list[dict[str, Any]],
    horizon_days: int,
) -> bool:
    for other in all_drafts:
        if other["platform"] == draft["platform"]:
            continue
        if other["recent_daily_throughput"] <= draft["recent_daily_throughput"]:
            continue
        other_estimated = other["estimated_clearance_days"]
        if other["backlog_count"] == 0 or (
            other_estimated is not None and other_estimated <= horizon_days
        ):
            return True
    return False


def _estimated_clearance_days(backlog_count: int, daily_throughput: float) -> float | None:
    if backlog_count == 0:
        return 0.0
    if daily_throughput <= 0:
        return None
    return round(math.ceil(backlog_count / daily_throughput * 100) / 100, 2)


def _fetch_dicts(
    conn: sqlite3.Connection,
    query: str,
    params: list[Any] | tuple[Any, ...],
) -> list[dict[str, Any]]:
    cursor = conn.execute(query, params)
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _parse_timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return _as_utc(parsed)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _normalize_platform(value: Any) -> str:
    return str(value or "unknown").strip().lower() or "unknown"


def _normalize_status(value: Any) -> str:
    return str(value or "unknown").strip().lower() or "unknown"


def _round_rate(value: float) -> float:
    return round(value, 4)
