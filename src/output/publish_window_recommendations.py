"""Recommend publish windows from historical engagement outcomes."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 90
DEFAULT_MIN_SAMPLES = 3
DEFAULT_LIMIT = 10
PRIOR_SAMPLE_WEIGHT = 3.0
DAY_NAMES = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")


@dataclass(frozen=True)
class PublishSample:
    """One published post with its latest available engagement metrics."""

    content_id: int
    platform: str
    published_at: str
    weekday: int
    hour: int
    engagement_score: float
    likes: int
    replies: int
    reposts: int
    clicks: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PublishWindowRecommendation:
    """Ranked weekday/hour recommendation for future publishing."""

    weekday: int
    day_name: str
    hour: int
    sample_count: int
    confidence: str
    average_engagement_score: float
    normalized_engagement_score: float
    total_likes: int
    total_replies: int
    total_reposts: int
    total_clicks: int
    next_publish_at: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PublishWindowRecommendationReport:
    """Historical engagement report with ranked publish window recommendations."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    recommendations: tuple[PublishWindowRecommendation, ...]
    samples: tuple[PublishSample, ...]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "publish_window_recommendations",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "recommendations": [item.to_dict() for item in self.recommendations],
            "samples": [sample.to_dict() for sample in self.samples],
            "totals": dict(sorted(self.totals.items())),
        }


def build_publish_window_recommendation_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_samples: int = DEFAULT_MIN_SAMPLES,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> PublishWindowRecommendationReport:
    """Rank weekday/hour publish windows by normalized engagement score."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_samples <= 0:
        raise ValueError("min_samples must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    start = generated_at - timedelta(days=days)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables: set[str] = set()
    missing_columns: dict[str, set[str]] = defaultdict(set)

    samples = _load_samples(
        conn,
        schema=schema,
        start=start,
        end=generated_at,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )
    samples.sort(key=lambda sample: (sample.published_at, sample.platform, sample.content_id))
    recommendations, sparse_count = _rank_windows(
        samples,
        min_samples=min_samples,
        limit=limit,
        now=generated_at,
    )

    return PublishWindowRecommendationReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "limit": limit,
            "min_samples": min_samples,
            "window_start": start.isoformat(),
            "window_end": generated_at.isoformat(),
        },
        totals={
            "sample_count": len(samples),
            "recommended_window_count": len(recommendations),
            "sparse_window_count": sparse_count,
            "window_count": len({(sample.weekday, sample.hour) for sample in samples}),
        },
        recommendations=tuple(recommendations),
        samples=tuple(samples),
        missing_tables=tuple(sorted(missing_tables)),
        missing_columns={
            table: tuple(sorted(columns))
            for table, columns in sorted(missing_columns.items())
            if columns
        },
    )


def format_publish_window_recommendations_json(
    report: PublishWindowRecommendationReport,
) -> str:
    """Serialize recommendations as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def _rank_windows(
    samples: list[PublishSample],
    *,
    min_samples: int,
    limit: int,
    now: datetime,
) -> tuple[list[PublishWindowRecommendation], int]:
    if not samples:
        return [], 0

    baseline = sum(sample.engagement_score for sample in samples) / len(samples)
    buckets: dict[tuple[int, int], list[PublishSample]] = defaultdict(list)
    for sample in samples:
        buckets[(sample.weekday, sample.hour)].append(sample)

    recommendations: list[PublishWindowRecommendation] = []
    sparse_count = 0
    for (weekday, hour), bucket in buckets.items():
        sample_count = len(bucket)
        if sample_count < min_samples:
            sparse_count += 1
            continue
        average_score = sum(sample.engagement_score for sample in bucket) / sample_count
        normalized_score = _normalized_score(
            average_score,
            sample_count=sample_count,
            baseline=baseline,
        )
        recommendations.append(
            PublishWindowRecommendation(
                weekday=weekday,
                day_name=DAY_NAMES[weekday],
                hour=hour,
                sample_count=sample_count,
                confidence=_confidence_label(sample_count),
                average_engagement_score=round(average_score, 2),
                normalized_engagement_score=round(normalized_score, 2),
                total_likes=sum(sample.likes for sample in bucket),
                total_replies=sum(sample.replies for sample in bucket),
                total_reposts=sum(sample.reposts for sample in bucket),
                total_clicks=sum(sample.clicks for sample in bucket),
                next_publish_at=_next_publish_at(now, weekday=weekday, hour=hour).isoformat(),
            )
        )

    recommendations.sort(
        key=lambda item: (
            -item.normalized_engagement_score,
            -item.sample_count,
            -item.average_engagement_score,
            item.weekday,
            item.hour,
        )
    )
    return recommendations[:limit], sparse_count


def _load_samples(
    conn: sqlite3.Connection,
    *,
    schema: dict[str, set[str]],
    start: datetime,
    end: datetime,
    missing_tables: set[str],
    missing_columns: dict[str, set[str]],
) -> list[PublishSample]:
    required = {
        "content_publications": ("content_id", "platform", "status", "published_at"),
        "post_engagement": (
            "content_id",
            "like_count",
            "retweet_count",
            "reply_count",
            "engagement_score",
            "fetched_at",
        ),
        "bluesky_engagement": (
            "content_id",
            "like_count",
            "repost_count",
            "reply_count",
            "engagement_score",
            "fetched_at",
        ),
    }
    for table, columns in required.items():
        if table not in schema:
            missing_tables.add(table)
            return []
        missing = tuple(column for column in columns if column not in schema[table])
        if missing:
            missing_columns[table].update(missing)
            return []

    click_expr = "0"
    click_join = ""
    if "newsletter_link_clicks" in schema:
        click_columns = schema["newsletter_link_clicks"]
        if "content_id" in click_columns and "fetched_at" in click_columns:
            click_column = "unique_clicks" if "unique_clicks" in click_columns else "clicks"
            if click_column in click_columns:
                click_expr = "COALESCE(clicks.clicks, 0)"
                click_join = f"""
               LEFT JOIN (
                   SELECT content_id, SUM(COALESCE({click_column}, 0)) AS clicks
                   FROM newsletter_link_clicks
                   WHERE content_id IS NOT NULL
                     AND fetched_at >= ?
                     AND fetched_at <= ?
                   GROUP BY content_id
               ) clicks ON clicks.content_id = cp.content_id"""

    params: list[Any] = [start.isoformat(), end.isoformat()]
    if click_join:
        params.extend([start.isoformat(), end.isoformat()])

    rows = _fetch_dicts(
        conn,
        f"""WITH latest_x AS (
               SELECT content_id, like_count, retweet_count, reply_count, engagement_score
               FROM (
                   SELECT content_id, like_count, retweet_count, reply_count, engagement_score,
                          ROW_NUMBER() OVER (
                              PARTITION BY content_id ORDER BY fetched_at DESC, id DESC
                          ) AS rn
                   FROM post_engagement
                   WHERE engagement_score IS NOT NULL
               )
               WHERE rn = 1
           ),
           latest_bluesky AS (
               SELECT content_id, like_count, repost_count, reply_count, engagement_score
               FROM (
                   SELECT content_id, like_count, repost_count, reply_count, engagement_score,
                          ROW_NUMBER() OVER (
                              PARTITION BY content_id ORDER BY fetched_at DESC, id DESC
                          ) AS rn
                   FROM bluesky_engagement
                   WHERE engagement_score IS NOT NULL
               )
               WHERE rn = 1
           )
           SELECT cp.content_id,
                  LOWER(cp.platform) AS platform,
                  cp.published_at,
                  CASE
                      WHEN LOWER(cp.platform) = 'x' THEN latest_x.engagement_score
                      WHEN LOWER(cp.platform) = 'bluesky' THEN latest_bluesky.engagement_score
                  END AS engagement_score,
                  CASE
                      WHEN LOWER(cp.platform) = 'x' THEN latest_x.like_count
                      WHEN LOWER(cp.platform) = 'bluesky' THEN latest_bluesky.like_count
                  END AS likes,
                  CASE
                      WHEN LOWER(cp.platform) = 'x' THEN latest_x.reply_count
                      WHEN LOWER(cp.platform) = 'bluesky' THEN latest_bluesky.reply_count
                  END AS replies,
                  CASE
                      WHEN LOWER(cp.platform) = 'x' THEN latest_x.retweet_count
                      WHEN LOWER(cp.platform) = 'bluesky' THEN latest_bluesky.repost_count
                  END AS reposts,
                  {click_expr} AS clicks
           FROM content_publications cp
           LEFT JOIN latest_x ON latest_x.content_id = cp.content_id
           LEFT JOIN latest_bluesky ON latest_bluesky.content_id = cp.content_id
           {click_join}
           WHERE LOWER(cp.status) = 'published'
             AND cp.published_at IS NOT NULL
             AND cp.published_at >= ?
             AND cp.published_at <= ?
             AND LOWER(cp.platform) IN ('x', 'bluesky')
             AND CASE
                     WHEN LOWER(cp.platform) = 'x' THEN latest_x.engagement_score
                     WHEN LOWER(cp.platform) = 'bluesky' THEN latest_bluesky.engagement_score
                 END IS NOT NULL
           ORDER BY cp.published_at ASC, cp.content_id ASC, cp.platform ASC""",
        params,
    )

    samples: list[PublishSample] = []
    for row in rows:
        published_at = _parse_timestamp(row.get("published_at"))
        if published_at is None:
            continue
        samples.append(
            PublishSample(
                content_id=int(row["content_id"]),
                platform=str(row["platform"]),
                published_at=published_at.isoformat(),
                weekday=published_at.weekday(),
                hour=published_at.hour,
                engagement_score=float(row["engagement_score"]),
                likes=_int(row.get("likes")),
                replies=_int(row.get("replies")),
                reposts=_int(row.get("reposts")),
                clicks=_int(row.get("clicks")),
            )
        )
    return samples


def _normalized_score(average_score: float, *, sample_count: int, baseline: float) -> float:
    posterior = (
        (average_score * sample_count) + (baseline * PRIOR_SAMPLE_WEIGHT)
    ) / (sample_count + PRIOR_SAMPLE_WEIGHT)
    confidence_weight = sample_count / (sample_count + PRIOR_SAMPLE_WEIGHT)
    return posterior * confidence_weight


def _confidence_label(sample_count: int) -> str:
    if sample_count >= 10:
        return "high"
    if sample_count >= 3:
        return "medium"
    return "low"


def _next_publish_at(now: datetime, *, weekday: int, hour: int) -> datetime:
    candidate = now.replace(hour=hour, minute=0, second=0, microsecond=0)
    days_ahead = (weekday - candidate.weekday()) % 7
    candidate = candidate + timedelta(days=days_ahead)
    if candidate <= now:
        candidate = candidate + timedelta(days=7)
    return candidate


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        str(row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    return {
        table: {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
        for table in tables
        if table
    }


def _fetch_dicts(
    conn: sqlite3.Connection,
    sql: str,
    params: list[Any],
) -> list[dict[str, Any]]:
    cursor = conn.execute(sql, params)
    return [dict(row) for row in cursor.fetchall()]


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return _as_utc(parsed)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0
