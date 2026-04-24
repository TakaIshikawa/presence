"""Weekly audience growth insights from profile metrics and publications."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from storage.db import Database


SUPPORTED_PLATFORMS = ("x", "bluesky")


@dataclass
class AssociatedPost:
    """Published post associated with a weekly growth window."""

    content_id: int
    platform: str
    published_at: datetime
    content_preview: str
    engagement_score: float
    engagement_to_growth_ratio: float | None


@dataclass
class WeeklyAudienceGrowth:
    """Audience growth and publication context for one platform/week."""

    platform: str
    week_start: datetime
    week_end: datetime
    start_followers: int | None
    end_followers: int | None
    follower_delta: int | None
    growth_rate_pct: float | None
    published_count: int
    total_engagement_score: float
    avg_engagement_score: float
    engagement_to_growth_ratio: float | None
    top_posts: list[AssociatedPost]


@dataclass
class QuietPeriod:
    """Weekly period with little publishing and flat or negative growth."""

    platform: str
    week_start: datetime
    week_end: datetime
    follower_delta: int | None
    published_count: int
    reason: str


@dataclass
class AudienceGrowthInsightsReport:
    """Audience growth insight report across one or more platforms."""

    weeks: int
    period_start: datetime
    period_end: datetime
    platforms: dict[str, list[WeeklyAudienceGrowth]]
    quiet_periods: list[QuietPeriod]


class AudienceGrowthInsights:
    """Correlate weekly audience growth with recent published content."""

    def __init__(
        self,
        db: Database,
        *,
        now: datetime | None = None,
        attribution_days: int = 3,
        top_post_limit: int = 3,
        quiet_publish_threshold: int = 1,
    ) -> None:
        self.db = db
        self.now = _ensure_aware(now or datetime.now(timezone.utc))
        self.attribution_days = attribution_days
        self.top_post_limit = top_post_limit
        self.quiet_publish_threshold = quiet_publish_threshold

    def generate(
        self,
        *,
        weeks: int = 4,
        platform: str = "all",
    ) -> AudienceGrowthInsightsReport:
        """Generate weekly audience growth insights."""
        if weeks <= 0:
            raise ValueError("weeks must be greater than 0")
        platforms = _resolve_platforms(platform)
        period_end = self.now
        period_start = period_end - timedelta(days=weeks * 7)

        platform_windows: dict[str, list[WeeklyAudienceGrowth]] = {}
        quiet_periods: list[QuietPeriod] = []
        for platform_name in platforms:
            windows = [
                self._weekly_growth(platform_name, week_start, week_start + timedelta(days=7))
                for week_start in _weekly_starts(period_start, weeks)
            ]
            platform_windows[platform_name] = windows
            quiet_periods.extend(
                self._quiet_period(window)
                for window in windows
                if self._is_quiet_period(window)
            )

        return AudienceGrowthInsightsReport(
            weeks=weeks,
            period_start=period_start,
            period_end=period_end,
            platforms=platform_windows,
            quiet_periods=quiet_periods,
        )

    def _weekly_growth(
        self,
        platform: str,
        week_start: datetime,
        week_end: datetime,
    ) -> WeeklyAudienceGrowth:
        start_metric, end_metric = self._metric_bounds(platform, week_start, week_end)
        start_followers = start_metric["follower_count"] if start_metric else None
        end_followers = end_metric["follower_count"] if end_metric else None
        follower_delta = (
            end_followers - start_followers
            if start_followers is not None and end_followers is not None
            else None
        )
        growth_rate_pct = (
            round((follower_delta / start_followers) * 100, 2)
            if follower_delta is not None and start_followers
            else None
        )

        posts = self._associated_posts(platform, week_start, week_end, follower_delta)
        window_posts = [
            post for post in posts
            if week_start <= post.published_at < week_end
        ]
        engagement_scores = [post.engagement_score for post in window_posts]
        total_engagement = round(sum(engagement_scores), 2)
        published_count = len(window_posts)
        avg_engagement = (
            round(total_engagement / published_count, 2)
            if published_count
            else 0.0
        )

        return WeeklyAudienceGrowth(
            platform=platform,
            week_start=week_start,
            week_end=week_end,
            start_followers=start_followers,
            end_followers=end_followers,
            follower_delta=follower_delta,
            growth_rate_pct=growth_rate_pct,
            published_count=published_count,
            total_engagement_score=total_engagement,
            avg_engagement_score=avg_engagement,
            engagement_to_growth_ratio=_ratio(total_engagement, follower_delta),
            top_posts=posts[: self.top_post_limit],
        )

    def _metric_bounds(
        self,
        platform: str,
        week_start: datetime,
        week_end: datetime,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        rows = self.db.conn.execute(
            """SELECT follower_count, following_count, tweet_count, fetched_at
               FROM profile_metrics
               WHERE platform = ?
                 AND fetched_at <= ?
               ORDER BY fetched_at ASC""",
            (platform, week_end.isoformat()),
        ).fetchall()
        metrics = [dict(row) for row in rows]
        if not metrics:
            return None, None

        start_metric = _latest_at_or_before(metrics, week_start)
        if start_metric is None:
            start_metric = _earliest_between(metrics, week_start, week_end)
        end_metric = _latest_at_or_before(metrics, week_end)
        if start_metric == end_metric:
            end_metric = _latest_between(metrics, week_start, week_end)
        return start_metric, end_metric

    def _associated_posts(
        self,
        platform: str,
        week_start: datetime,
        week_end: datetime,
        follower_delta: int | None,
    ) -> list[AssociatedPost]:
        engagement_table = _engagement_table(platform)
        id_column = "tweet_id" if platform == "x" else "bluesky_uri"
        attribution_start = week_start - timedelta(days=self.attribution_days)
        rows = self.db.conn.execute(
            f"""SELECT cp.content_id, cp.platform, cp.published_at, gc.content,
                      COALESCE(latest.engagement_score, 0) AS engagement_score
                FROM content_publications cp
                INNER JOIN generated_content gc ON gc.id = cp.content_id
                LEFT JOIN (
                    SELECT content_id, engagement_score,
                           ROW_NUMBER() OVER (
                               PARTITION BY content_id ORDER BY fetched_at DESC, id DESC
                           ) AS rn
                    FROM {engagement_table}
                    WHERE {id_column} IS NOT NULL
                ) latest ON latest.content_id = cp.content_id AND latest.rn = 1
                WHERE cp.platform = ?
                  AND cp.status = 'published'
                  AND cp.published_at >= ?
                  AND cp.published_at < ?
                ORDER BY engagement_score DESC, cp.published_at ASC, cp.content_id ASC""",
            (platform, attribution_start.isoformat(), week_end.isoformat()),
        ).fetchall()
        return [
            AssociatedPost(
                content_id=row["content_id"],
                platform=row["platform"],
                published_at=_parse_datetime(row["published_at"]),
                content_preview=_preview(row["content"]),
                engagement_score=round(float(row["engagement_score"] or 0), 2),
                engagement_to_growth_ratio=_ratio(float(row["engagement_score"] or 0), follower_delta),
            )
            for row in rows
        ]

    def _is_quiet_period(self, window: WeeklyAudienceGrowth) -> bool:
        return (
            window.published_count <= self.quiet_publish_threshold
            and window.follower_delta is not None
            and window.follower_delta <= 0
        )

    def _quiet_period(self, window: WeeklyAudienceGrowth) -> QuietPeriod:
        if window.published_count == 0:
            reason = "No published posts and flat or negative follower growth."
        else:
            reason = "Low publishing volume and flat or negative follower growth."
        return QuietPeriod(
            platform=window.platform,
            week_start=window.week_start,
            week_end=window.week_end,
            follower_delta=window.follower_delta,
            published_count=window.published_count,
            reason=reason,
        )


def _resolve_platforms(platform: str) -> list[str]:
    if platform == "all":
        return list(SUPPORTED_PLATFORMS)
    if platform in SUPPORTED_PLATFORMS:
        return [platform]
    raise ValueError("platform must be one of: all, x, bluesky")


def _weekly_starts(period_start: datetime, weeks: int) -> list[datetime]:
    return [period_start + timedelta(days=index * 7) for index in range(weeks)]


def _engagement_table(platform: str) -> str:
    if platform == "x":
        return "post_engagement"
    if platform == "bluesky":
        return "bluesky_engagement"
    raise ValueError("platform must be one of: x, bluesky")


def _latest_at_or_before(metrics: list[dict[str, Any]], target: datetime) -> dict[str, Any] | None:
    candidates = [
        metric for metric in metrics
        if _parse_datetime(metric["fetched_at"]) <= target
    ]
    return candidates[-1] if candidates else None


def _earliest_between(
    metrics: list[dict[str, Any]],
    start: datetime,
    end: datetime,
) -> dict[str, Any] | None:
    for metric in metrics:
        fetched_at = _parse_datetime(metric["fetched_at"])
        if start <= fetched_at <= end:
            return metric
    return None


def _latest_between(
    metrics: list[dict[str, Any]],
    start: datetime,
    end: datetime,
) -> dict[str, Any] | None:
    candidates = [
        metric for metric in metrics
        if start <= _parse_datetime(metric["fetched_at"]) <= end
    ]
    return candidates[-1] if candidates else None


def _ratio(numerator: float, follower_delta: int | None) -> float | None:
    if follower_delta is None or follower_delta <= 0:
        return None
    return round(numerator / follower_delta, 2)


def _preview(content: str, width: int = 120) -> str:
    text = " ".join(str(content).split())
    if len(text) <= width:
        return text
    return text[: width - 3] + "..."


def _parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return _ensure_aware(parsed)


def _ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value
