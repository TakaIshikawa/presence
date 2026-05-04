"""Report engagement velocity and identify trending interaction patterns."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_WINDOW_DAYS = 7


@dataclass(frozen=True)
class EngagementVelocityRow:
    """One content piece or topic with engagement velocity metrics."""

    content_id: int | None
    topic: str | None
    platform: str
    current_period_engagement: float
    previous_period_engagement: float
    velocity: float  # Change in engagement rate
    acceleration: str  # 'accelerating', 'decelerating', 'stable'
    current_period_posts: int
    previous_period_posts: int
    published_at: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class EngagementVelocityReport:
    """Engagement velocity trend report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    rows: tuple[EngagementVelocityRow, ...]
    high_velocity_topics: dict[str, float]  # topic -> velocity
    platform_summary: dict[str, dict[str, float]]  # platform -> metrics
    missing_tables: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "engagement_velocity",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "high_velocity_topics": dict(sorted(
                self.high_velocity_topics.items(),
                key=lambda x: x[1],
                reverse=True
            )),
            "missing_tables": list(self.missing_tables),
            "platform_summary": {
                platform: dict(sorted(metrics.items()))
                for platform, metrics in sorted(self.platform_summary.items())
            },
            "rows": [row.to_dict() for row in self.rows],
            "totals": dict(sorted(self.totals.items())),
        }


def build_engagement_velocity_report(
    db_or_conn: Any,
    *,
    window_days: int = DEFAULT_WINDOW_DAYS,
    platform: str | None = None,
    topic: str | None = None,
    now: datetime | None = None,
) -> EngagementVelocityReport:
    """Return engagement velocity trends with acceleration/deceleration detection."""
    if window_days <= 0:
        raise ValueError("window_days must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    filters = {
        "window_days": window_days,
        "platform": platform,
        "topic": topic,
    }

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = _missing_tables(schema)
    if missing_tables:
        return _empty_report(
            generated_at=generated_at,
            filters=filters,
            missing_tables=missing_tables,
        )

    # Define time windows
    current_end = generated_at
    current_start = current_end - timedelta(days=window_days)
    previous_end = current_start
    previous_start = previous_end - timedelta(days=window_days)

    # Collect velocity data from all available platforms
    rows_data = _collect_velocity_data(
        conn,
        schema,
        current_start=current_start,
        current_end=current_end,
        previous_start=previous_start,
        previous_end=previous_end,
        platform_filter=platform,
        topic_filter=topic,
    )

    # Build rows with velocity calculation
    rows = tuple(
        sorted(
            (_build_row(row) for row in rows_data),
            key=_sort_key,
            reverse=True,
        )
    )

    # Aggregate high-velocity topics
    high_velocity_topics = _aggregate_by_topic(rows)

    # Platform summary
    platform_summary = _aggregate_by_platform(rows)

    return EngagementVelocityReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "total_items": len(rows),
            "accelerating_count": sum(1 for row in rows if row.acceleration == "accelerating"),
            "decelerating_count": sum(1 for row in rows if row.acceleration == "decelerating"),
            "stable_count": sum(1 for row in rows if row.acceleration == "stable"),
        },
        rows=rows,
        high_velocity_topics=high_velocity_topics,
        platform_summary=platform_summary,
        missing_tables=(),
    )


def format_engagement_velocity_json(
    report: EngagementVelocityReport,
) -> str:
    """Serialize the report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_engagement_velocity_csv(
    report: EngagementVelocityReport,
) -> str:
    """Render the engagement velocity report as CSV."""
    lines = [
        "content_id,topic,platform,current_engagement,previous_engagement,velocity,acceleration,current_posts,previous_posts"
    ]
    for row in report.rows:
        lines.append(
            f"{row.content_id if row.content_id else ''},"
            f"{_csv_escape(row.topic or '')},"
            f"{_csv_escape(row.platform)},"
            f"{row.current_period_engagement:.2f},"
            f"{row.previous_period_engagement:.2f},"
            f"{row.velocity:.2f},"
            f"{row.acceleration},"
            f"{row.current_period_posts},"
            f"{row.previous_period_posts}"
        )
    return "\n".join(lines)


def _collect_velocity_data(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    current_start: datetime,
    current_end: datetime,
    previous_start: datetime,
    previous_end: datetime,
    platform_filter: str | None,
    topic_filter: str | None,
) -> list[dict[str, Any]]:
    """Collect engagement velocity data from all available platforms."""
    velocity_data = []

    # X/Twitter engagement
    if "post_engagement" in schema and "generated_content" in schema:
        velocity_data.extend(_load_platform_velocity(
            conn,
            platform="x",
            engagement_table="post_engagement",
            engagement_columns=["like_count", "retweet_count", "reply_count", "quote_count"],
            current_start=current_start,
            current_end=current_end,
            previous_start=previous_start,
            previous_end=previous_end,
            platform_filter=platform_filter,
            topic_filter=topic_filter,
        ))

    # LinkedIn engagement
    if "linkedin_engagement" in schema and "generated_content" in schema:
        velocity_data.extend(_load_platform_velocity(
            conn,
            platform="linkedin",
            engagement_table="linkedin_engagement",
            engagement_columns=["like_count", "comment_count", "share_count"],
            current_start=current_start,
            current_end=current_end,
            previous_start=previous_start,
            previous_end=previous_end,
            platform_filter=platform_filter,
            topic_filter=topic_filter,
        ))

    # Bluesky engagement
    if "bluesky_engagement" in schema and "generated_content" in schema:
        velocity_data.extend(_load_platform_velocity(
            conn,
            platform="bluesky",
            engagement_table="bluesky_engagement",
            engagement_columns=["like_count", "repost_count", "reply_count", "quote_count"],
            current_start=current_start,
            current_end=current_end,
            previous_start=previous_start,
            previous_end=previous_end,
            platform_filter=platform_filter,
            topic_filter=topic_filter,
        ))

    # Mastodon engagement
    if "mastodon_engagement" in schema and "generated_content" in schema:
        velocity_data.extend(_load_platform_velocity(
            conn,
            platform="mastodon",
            engagement_table="mastodon_engagement",
            engagement_columns=["favourite_count", "boost_count", "reply_count"],
            current_start=current_start,
            current_end=current_end,
            previous_start=previous_start,
            previous_end=previous_end,
            platform_filter=platform_filter,
            topic_filter=topic_filter,
        ))

    return velocity_data


def _load_platform_velocity(
    conn: sqlite3.Connection,
    *,
    platform: str,
    engagement_table: str,
    engagement_columns: list[str],
    current_start: datetime,
    current_end: datetime,
    previous_start: datetime,
    previous_end: datetime,
    platform_filter: str | None,
    topic_filter: str | None,
) -> list[dict[str, Any]]:
    """Load velocity data for a specific platform."""
    if platform_filter and platform != platform_filter:
        return []

    # Build engagement sum expression
    engagement_sum = " + ".join(f"COALESCE({col}, 0)" for col in engagement_columns)

    # Build topic join if filtering
    topic_join = ""
    topic_where = ""
    if topic_filter:
        topic_join = "LEFT JOIN content_topics ct ON gc.id = ct.content_id"
        topic_where = f"AND ct.topic = '{topic_filter}'"

    # Query for current period
    current_query = f"""
        SELECT
            gc.id as content_id,
            '{platform}' as platform,
            AVG({engagement_sum}) as avg_engagement,
            COUNT(DISTINCT gc.id) as post_count,
            MAX(gc.published_at) as published_at
        FROM {engagement_table} e
        JOIN generated_content gc ON e.content_id = gc.id
        {topic_join}
        WHERE e.fetched_at >= ? AND e.fetched_at < ?
            {topic_where}
        GROUP BY gc.id
    """

    # Query for previous period
    previous_query = f"""
        SELECT
            gc.id as content_id,
            AVG({engagement_sum}) as avg_engagement,
            COUNT(DISTINCT gc.id) as post_count
        FROM {engagement_table} e
        JOIN generated_content gc ON e.content_id = gc.id
        {topic_join}
        WHERE e.fetched_at >= ? AND e.fetched_at < ?
            {topic_where}
        GROUP BY gc.id
    """

    current_rows = conn.execute(
        current_query,
        (current_start.isoformat(), current_end.isoformat())
    ).fetchall()

    previous_rows = conn.execute(
        previous_query,
        (previous_start.isoformat(), previous_end.isoformat())
    ).fetchall()

    # Build lookup for previous period
    previous_by_content = {
        row["content_id"]: {
            "avg_engagement": float(row["avg_engagement"] or 0),
            "post_count": int(row["post_count"] or 0),
        }
        for row in previous_rows
    }

    # Combine into velocity data
    results = []
    for row in current_rows:
        content_id = row["content_id"]
        current_engagement = float(row["avg_engagement"] or 0)
        current_posts = int(row["post_count"] or 0)

        previous = previous_by_content.get(content_id, {"avg_engagement": 0.0, "post_count": 0})
        previous_engagement = previous["avg_engagement"]
        previous_posts = previous["post_count"]

        # Get topic if available
        topic = _get_content_topic(conn, content_id)

        results.append({
            "content_id": content_id,
            "topic": topic,
            "platform": platform,
            "current_period_engagement": current_engagement,
            "previous_period_engagement": previous_engagement,
            "current_period_posts": current_posts,
            "previous_period_posts": previous_posts,
            "published_at": row["published_at"],
        })

    return results


def _get_content_topic(conn: sqlite3.Connection, content_id: int) -> str | None:
    """Get the primary topic for a content piece."""
    try:
        row = conn.execute(
            "SELECT topic FROM content_topics WHERE content_id = ? ORDER BY confidence DESC LIMIT 1",
            (content_id,)
        ).fetchone()
        return row["topic"] if row else None
    except sqlite3.OperationalError:
        # content_topics table doesn't exist
        return None


def _build_row(row: dict[str, Any]) -> EngagementVelocityRow:
    """Build a velocity row from raw data."""
    current_eng = float(row["current_period_engagement"])
    previous_eng = float(row["previous_period_engagement"])

    # Calculate velocity (change in engagement)
    velocity = current_eng - previous_eng

    # Determine acceleration status
    # Use a threshold to avoid noise
    threshold = 0.5
    if velocity > threshold:
        acceleration = "accelerating"
    elif velocity < -threshold:
        acceleration = "decelerating"
    else:
        acceleration = "stable"

    return EngagementVelocityRow(
        content_id=row.get("content_id"),
        topic=_optional_value(row.get("topic")),
        platform=str(row["platform"]),
        current_period_engagement=current_eng,
        previous_period_engagement=previous_eng,
        velocity=velocity,
        acceleration=acceleration,
        current_period_posts=int(row["current_period_posts"]),
        previous_period_posts=int(row["previous_period_posts"]),
        published_at=_optional_value(row.get("published_at")),
    )


def _sort_key(row: EngagementVelocityRow) -> tuple[Any, ...]:
    """Sort by absolute velocity descending, then acceleration status."""
    return (
        abs(row.velocity),  # Highest velocity changes first
        row.acceleration == "accelerating",  # Accelerating before decelerating
        row.current_period_engagement,
    )


def _aggregate_by_topic(rows: tuple[EngagementVelocityRow, ...]) -> dict[str, float]:
    """Aggregate velocity by topic, returning top topics by average velocity."""
    topic_velocities: dict[str, list[float]] = {}

    for row in rows:
        if row.topic:
            if row.topic not in topic_velocities:
                topic_velocities[row.topic] = []
            topic_velocities[row.topic].append(row.velocity)

    # Calculate average velocity per topic
    topic_avg_velocity = {
        topic: sum(velocities) / len(velocities)
        for topic, velocities in topic_velocities.items()
    }

    # Return top 10 by velocity
    sorted_topics = sorted(topic_avg_velocity.items(), key=lambda x: x[1], reverse=True)
    return dict(sorted_topics[:10])


def _aggregate_by_platform(rows: tuple[EngagementVelocityRow, ...]) -> dict[str, dict[str, float]]:
    """Aggregate metrics by platform."""
    platform_data: dict[str, dict[str, list[float]]] = {}

    for row in rows:
        if row.platform not in platform_data:
            platform_data[row.platform] = {
                "velocities": [],
                "current_engagements": [],
            }
        platform_data[row.platform]["velocities"].append(row.velocity)
        platform_data[row.platform]["current_engagements"].append(row.current_period_engagement)

    # Calculate averages
    platform_summary = {}
    for platform, data in platform_data.items():
        velocities = data["velocities"]
        engagements = data["current_engagements"]
        platform_summary[platform] = {
            "avg_velocity": sum(velocities) / len(velocities) if velocities else 0.0,
            "avg_engagement": sum(engagements) / len(engagements) if engagements else 0.0,
            "item_count": len(velocities),
        }

    return platform_summary


def _missing_tables(schema: dict[str, set[str]]) -> tuple[str, ...]:
    """Check for missing required tables."""
    # Need at least one engagement table and generated_content
    engagement_tables = {
        "post_engagement", "linkedin_engagement",
        "bluesky_engagement", "mastodon_engagement"
    }
    if "generated_content" not in schema:
        return ("generated_content",)
    if not any(table in schema for table in engagement_tables):
        return ("at least one of: post_engagement, linkedin_engagement, bluesky_engagement, mastodon_engagement",)
    return ()


def _empty_report(
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: tuple[str, ...] = (),
) -> EngagementVelocityReport:
    return EngagementVelocityReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "total_items": 0,
            "accelerating_count": 0,
            "decelerating_count": 0,
            "stable_count": 0,
        },
        rows=(),
        high_velocity_topics={},
        platform_summary={},
        missing_tables=missing_tables,
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
    ).fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = str(row["name"] if isinstance(row, sqlite3.Row) else row[0])
        schema[table] = {
            str(info[1]) for info in conn.execute(f"PRAGMA table_info({table})")
        }
    return schema


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return _ensure_utc(value)
    if value is None:
        raise ValueError("Cannot parse None as datetime")
    text = str(value).strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    return _ensure_utc(datetime.fromisoformat(text))


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _optional_value(value: Any) -> str | None:
    text = str(value or "").strip()
    return text if text else None


def _csv_escape(value: str) -> str:
    """Escape CSV field values."""
    if not value:
        return ""
    if "," in value or '"' in value or "\n" in value:
        escaped = value.replace('"', '""')
        return f'"{escaped}"'
    return value
