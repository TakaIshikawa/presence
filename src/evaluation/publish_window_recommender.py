"""Rank concrete upcoming publish windows by history and daily cap pressure."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Mapping

from evaluation.posting_windows import PostingWindow, PostingWindowRecommender
from output.publish_caps import DEFAULT_DAILY_PLATFORM_LIMITS, utc_day_bounds
from storage.db import Database


VALID_RECOMMENDATION_PLATFORMS = {"all", "x", "bluesky"}


@dataclass(frozen=True)
class PublishWindowRecommendation:
    """A concrete platform-specific window that can be scheduled."""

    platform: str
    start_time: datetime
    score: float
    available: bool
    reasons: list[str]
    historical_score: float
    historical_avg_engagement: float
    historical_sample_size: int
    historical_confidence: str
    cap_limit: int | None
    cap_published_count: int
    cap_queued_count: int
    cap_pressure: float | None
    content_type: str | None = None

    @property
    def start_time_iso(self) -> str:
        return self.start_time.isoformat()


class PublishWindowRecommender:
    """Recommend the next best platform-specific publish windows."""

    def __init__(
        self,
        db: Database,
        daily_limits: Mapping[str, int] | None = None,
        *,
        history_days: int = 90,
    ) -> None:
        self.db = db
        self.daily_limits = dict(
            DEFAULT_DAILY_PLATFORM_LIMITS if daily_limits is None else daily_limits
        )
        self.history_days = max(1, int(history_days))
        self.window_recommender = PostingWindowRecommender(db)

    def recommend(
        self,
        *,
        platform: str = "all",
        days: int = 7,
        limit: int | None = 10,
        content_type: str | None = None,
        now: datetime | None = None,
    ) -> list[PublishWindowRecommendation]:
        """Return ranked upcoming publish windows.

        ``days`` is the look-ahead horizon. Historical engagement uses
        ``history_days`` from the recommender instance.
        """
        platform = _normalize_platform(platform)
        horizon_days = max(1, int(days))
        current_time = _ensure_utc(now or datetime.now(timezone.utc))
        platforms = ("x", "bluesky") if platform == "all" else (platform,)

        recommendations: list[PublishWindowRecommendation] = []
        for platform_name in platforms:
            historical_windows = self.window_recommender.recommend(
                days=self.history_days,
                platform=platform_name,
                limit=None,
                content_type=content_type,
            )
            for window in historical_windows:
                for start_time in _upcoming_start_times(
                    window,
                    now=current_time,
                    days=horizon_days,
                ):
                    recommendations.append(
                        self._recommendation_for_window(
                            platform=platform_name,
                            window=window,
                            start_time=start_time,
                            content_type=content_type,
                        )
                    )

        recommendations.sort(
            key=lambda item: (
                item.available,
                item.score,
                item.historical_sample_size,
                -item.start_time.timestamp(),
            ),
            reverse=True,
        )
        if limit is not None:
            recommendations = recommendations[: max(0, int(limit))]
        return recommendations

    def _recommendation_for_window(
        self,
        *,
        platform: str,
        window: PostingWindow,
        start_time: datetime,
        content_type: str | None,
    ) -> PublishWindowRecommendation:
        cap = self._cap_status_for_day(platform, start_time)
        pressure = (
            None
            if cap["limit"] is None
            else cap["effective_count"] / max(1, cap["limit"])
        )
        available = cap["limit"] is None or cap["effective_count"] < cap["limit"]
        score = self._score(window.normalized_engagement, pressure, available)
        reasons = self._reasons(
            window=window,
            cap=cap,
            pressure=pressure,
            available=available,
        )
        return PublishWindowRecommendation(
            platform=platform,
            start_time=start_time,
            score=round(score, 2),
            available=available,
            reasons=reasons,
            historical_score=window.normalized_engagement,
            historical_avg_engagement=window.avg_engagement,
            historical_sample_size=window.sample_size,
            historical_confidence=window.confidence_label,
            cap_limit=cap["limit"],
            cap_published_count=cap["published_count"],
            cap_queued_count=cap["queued_count"],
            cap_pressure=None if pressure is None else round(pressure, 2),
            content_type=content_type,
        )

    def _cap_status_for_day(self, platform: str, start_time: datetime) -> dict:
        day_start, day_end = utc_day_bounds(start_time)
        published_count = _safe_count(
            self.db.conn.execute(
                """SELECT COUNT(*) AS count
                   FROM content_publications
                   WHERE platform = ?
                     AND status = 'published'
                     AND published_at IS NOT NULL
                     AND published_at >= ?
                     AND published_at < ?""",
                (platform, day_start.isoformat(), day_end.isoformat()),
            ).fetchone()["count"]
        )
        queued_count = 0
        if hasattr(self.db, "count_platform_queue_items_between"):
            queued_count = _safe_count(
                self.db.count_platform_queue_items_between(
                    platform,
                    day_start.isoformat(),
                    day_end.isoformat(),
                )
            )
        limit = self.daily_limits.get(platform)
        return {
            "limit": limit,
            "published_count": published_count,
            "queued_count": queued_count,
            "effective_count": published_count + queued_count,
        }

    @staticmethod
    def _score(
        historical_score: float,
        pressure: float | None,
        available: bool,
    ) -> float:
        if not available:
            return historical_score - 1000.0
        if pressure is None:
            return historical_score
        return historical_score * (1.0 - min(0.5, pressure * 0.25))

    @staticmethod
    def _reasons(
        *,
        window: PostingWindow,
        cap: dict,
        pressure: float | None,
        available: bool,
    ) -> list[str]:
        reasons = [
            (
                f"Historical {window.platform} engagement for "
                f"{window.day_name} {window.hour_utc:02d}:00 UTC is "
                f"{window.normalized_engagement:.2f} from {window.sample_size} posts "
                f"({window.confidence_label} confidence)."
            )
        ]
        if cap["limit"] is None:
            reasons.append("No configured daily cap for this platform.")
        else:
            reasons.append(
                "Daily cap pressure is "
                f"{cap['effective_count']}/{cap['limit']} "
                f"({cap['published_count']} published, {cap['queued_count']} queued)."
            )
            if not available:
                reasons.append("Unavailable because this window would exceed the daily cap.")
            elif pressure and pressure >= 0.67:
                reasons.append("Ranked lower because the daily cap is already under pressure.")
            else:
                reasons.append("Daily cap has room for another publish.")
        return reasons


def recommend_publish_windows(
    db: Database,
    *,
    daily_limits: Mapping[str, int] | None = None,
    platform: str = "all",
    days: int = 7,
    limit: int | None = 10,
    content_type: str | None = None,
    now: datetime | None = None,
) -> list[PublishWindowRecommendation]:
    """Convenience wrapper for ``PublishWindowRecommender.recommend``."""
    return PublishWindowRecommender(db, daily_limits=daily_limits).recommend(
        platform=platform,
        days=days,
        limit=limit,
        content_type=content_type,
        now=now,
    )


def recommendations_to_dicts(
    recommendations: Iterable[PublishWindowRecommendation],
) -> list[dict]:
    """Serialize recommendations for stable JSON automation output."""
    return [
        {
            "platform": item.platform,
            "start_time": item.start_time.isoformat(),
            "score": item.score,
            "available": item.available,
            "reasons": list(item.reasons),
            "historical_signal": {
                "score": item.historical_score,
                "avg_engagement": item.historical_avg_engagement,
                "sample_size": item.historical_sample_size,
                "confidence": item.historical_confidence,
            },
            "cap_pressure": {
                "limit": item.cap_limit,
                "published_count": item.cap_published_count,
                "queued_count": item.cap_queued_count,
                "pressure": item.cap_pressure,
            },
            "content_type": item.content_type,
        }
        for item in recommendations
    ]


def _normalize_platform(platform: str) -> str:
    normalized = str(platform).strip().lower()
    if normalized not in VALID_RECOMMENDATION_PLATFORMS:
        raise ValueError("platform must be one of: all, x, bluesky")
    return normalized


def _upcoming_start_times(
    window: PostingWindow,
    *,
    now: datetime,
    days: int,
) -> list[datetime]:
    horizon_end = now + timedelta(days=days)
    days_ahead = (window.day_of_week - now.weekday()) % 7
    candidate = now.replace(
        hour=window.hour_utc,
        minute=0,
        second=0,
        microsecond=0,
    ) + timedelta(days=days_ahead)
    if candidate < now:
        candidate += timedelta(days=7)

    starts: list[datetime] = []
    while candidate <= horizon_end:
        starts.append(candidate)
        candidate += timedelta(days=7)
    return starts


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _safe_count(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return 0
    return int(value)
