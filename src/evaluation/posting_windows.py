"""Engagement-based posting window recommendations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable

from storage.db import Database


VALID_PLATFORMS = {"all", "x", "bluesky"}
DAY_NAMES = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")


@dataclass
class PostingWindow:
    """Ranked weekday/hour posting recommendation."""

    day_of_week: int
    day_name: str
    hour_utc: int
    sample_size: int
    avg_engagement: float
    normalized_engagement: float
    confidence: float
    confidence_label: str
    platform: str = "all"

    @property
    def weekday(self) -> int:
        """Alias for callers that use weekday terminology."""
        return self.day_of_week

    @property
    def hour(self) -> int:
        """Alias for callers that do not need the UTC suffix."""
        return self.hour_utc

    @property
    def normalized_score(self) -> float:
        """Alias for the reliability-adjusted engagement score."""
        return self.normalized_engagement

    @property
    def score(self) -> float:
        """Alias for ranking score."""
        return self.normalized_engagement


class PostingWindowRecommender:
    """Recommend posting windows from X and Bluesky engagement history."""

    def __init__(self, db: Database, prior_weight: float = 3.0) -> None:
        self.db = db
        self.prior_weight = max(0.0, float(prior_weight))

    def recommend(
        self,
        days: int = 90,
        platform: str = "all",
        limit: int | None = 10,
    ) -> list[PostingWindow]:
        """Return ranked posting windows for the requested platform.

        Sparse buckets are normalized with empirical-Bayes shrinkage and a
        reliability penalty. This keeps one-off spikes from outranking
        consistently good windows.
        """
        platform = _normalize_platform(platform)
        days = max(1, int(days))
        rows = self._fetch_rows(days=days, platform=platform)
        if not rows:
            return []

        scores = [row["engagement_score"] for row in rows]
        baseline = sum(scores) / len(scores)
        buckets: dict[tuple[int, int], list[float]] = {}

        for row in rows:
            published_at = row["published_at"]
            key = (published_at.weekday(), published_at.hour)
            buckets.setdefault(key, []).append(row["engagement_score"])

        windows: list[PostingWindow] = []
        for (day_of_week, hour_utc), bucket_scores in buckets.items():
            sample_size = len(bucket_scores)
            avg_engagement = sum(bucket_scores) / sample_size
            normalized = self._normalize_score(avg_engagement, sample_size, baseline)
            confidence = self._confidence(sample_size)
            windows.append(
                PostingWindow(
                    day_of_week=day_of_week,
                    day_name=DAY_NAMES[day_of_week],
                    hour_utc=hour_utc,
                    sample_size=sample_size,
                    avg_engagement=round(avg_engagement, 2),
                    normalized_engagement=round(normalized, 2),
                    confidence=round(confidence, 2),
                    confidence_label=self._confidence_label(sample_size),
                    platform=platform,
                )
            )

        windows.sort(
            key=lambda window: (
                window.normalized_engagement,
                window.confidence,
                window.avg_engagement,
                window.sample_size,
            ),
            reverse=True,
        )
        if limit is not None:
            windows = windows[: max(0, int(limit))]
        return windows

    def recommend_windows(
        self,
        days: int = 90,
        platform: str = "all",
        limit: int | None = 10,
    ) -> list[PostingWindow]:
        """Alias for ``recommend``."""
        return self.recommend(days=days, platform=platform, limit=limit)

    def _fetch_rows(self, days: int, platform: str) -> list[dict]:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        platforms = ("x", "bluesky") if platform == "all" else (platform,)
        rows: list[dict] = []

        for platform_name in platforms:
            for raw_row in self._fetch_platform_rows(platform_name):
                published_at = _parse_datetime(raw_row["published_at"])
                if published_at is None or published_at < cutoff:
                    continue
                engagement_score = raw_row["engagement_score"]
                if engagement_score is None:
                    continue
                rows.append(
                    {
                        "platform": platform_name,
                        "published_at": published_at,
                        "engagement_score": float(engagement_score),
                    }
                )

        return rows

    def _fetch_platform_rows(self, platform: str) -> list[dict]:
        if platform == "x":
            return self._fetch_x_rows()
        if platform == "bluesky":
            return self._fetch_bluesky_rows()
        raise ValueError("platform must be one of: all, x, bluesky")

    def _fetch_x_rows(self) -> list[dict]:
        cursor = self.db.conn.execute(
            """WITH latest AS (
                   SELECT content_id, engagement_score,
                          ROW_NUMBER() OVER (
                              PARTITION BY content_id ORDER BY fetched_at DESC, id DESC
                          ) AS rn
                   FROM post_engagement
                   WHERE engagement_score IS NOT NULL
               )
               SELECT COALESCE(gc.published_at, cp.published_at) AS published_at,
                      latest.engagement_score AS engagement_score
               FROM content_publications cp
               INNER JOIN generated_content gc ON gc.id = cp.content_id
               INNER JOIN latest ON latest.content_id = gc.id AND latest.rn = 1
               WHERE cp.platform = 'x'
                 AND cp.status = 'published'
                 AND cp.published_at IS NOT NULL
               UNION ALL
               SELECT gc.published_at AS published_at,
                      latest.engagement_score AS engagement_score
               FROM generated_content gc
               INNER JOIN latest ON latest.content_id = gc.id AND latest.rn = 1
               WHERE gc.published = 1
                 AND gc.published_at IS NOT NULL
                 AND NOT EXISTS (
                     SELECT 1
                     FROM content_publications cp
                     WHERE cp.content_id = gc.id
                       AND cp.platform = 'x'
                       AND cp.status = 'published'
                 )"""
        )
        return [dict(row) for row in cursor.fetchall()]

    def _fetch_bluesky_rows(self) -> list[dict]:
        cursor = self.db.conn.execute(
            """WITH latest AS (
                   SELECT content_id, engagement_score,
                          ROW_NUMBER() OVER (
                              PARTITION BY content_id ORDER BY fetched_at DESC, id DESC
                          ) AS rn
                   FROM bluesky_engagement
                   WHERE engagement_score IS NOT NULL
               )
               SELECT cp.published_at AS published_at,
                      latest.engagement_score AS engagement_score
               FROM content_publications cp
               INNER JOIN generated_content gc ON gc.id = cp.content_id
               INNER JOIN latest ON latest.content_id = gc.id AND latest.rn = 1
               WHERE cp.platform = 'bluesky'
                 AND cp.status = 'published'
                 AND cp.published_at IS NOT NULL"""
        )
        return [dict(row) for row in cursor.fetchall()]

    def _normalize_score(
        self,
        avg_engagement: float,
        sample_size: int,
        baseline: float,
    ) -> float:
        if self.prior_weight == 0:
            return avg_engagement
        posterior = (
            (avg_engagement * sample_size) + (baseline * self.prior_weight)
        ) / (sample_size + self.prior_weight)
        return posterior * self._confidence(sample_size)

    def _confidence(self, sample_size: int) -> float:
        return sample_size / (sample_size + self.prior_weight) if sample_size > 0 else 0.0

    @staticmethod
    def _confidence_label(sample_size: int) -> str:
        if sample_size >= 10:
            return "high"
        if sample_size >= 3:
            return "medium"
        return "low"


def recommend_posting_windows(
    db: Database,
    days: int = 90,
    platform: str = "all",
    limit: int | None = 10,
) -> list[PostingWindow]:
    """Convenience wrapper for ``PostingWindowRecommender.recommend``."""
    return PostingWindowRecommender(db).recommend(days=days, platform=platform, limit=limit)


def _normalize_platform(platform: str) -> str:
    normalized = str(platform).strip().lower()
    if normalized not in VALID_PLATFORMS:
        raise ValueError("platform must be one of: all, x, bluesky")
    return normalized


def _parse_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            try:
                parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def windows_to_dicts(windows: Iterable[PostingWindow]) -> list[dict]:
    """Serialize posting windows for JSON callers."""
    return [
        {
            "platform": window.platform,
            "day_of_week": window.day_of_week,
            "day_name": window.day_name,
            "hour_utc": window.hour_utc,
            "sample_size": window.sample_size,
            "avg_engagement": window.avg_engagement,
            "normalized_engagement": window.normalized_engagement,
            "confidence": window.confidence,
            "confidence_label": window.confidence_label,
        }
        for window in windows
    ]
