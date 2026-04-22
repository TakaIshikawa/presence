"""Detect engagement anomalies against recent platform/format baselines."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from statistics import median
from typing import Any

from storage.db import Database


MAD_NORMAL_SCALE = 1.4826


@dataclass
class EngagementPost:
    """Latest engagement snapshot for one post on one platform."""

    content_id: int
    platform: str
    content_format: str
    content_type: str
    content_preview: str
    published_at: str
    engagement_score: float
    fetched_at: str


@dataclass
class EngagementBaseline:
    """Robust baseline for a platform/content_format group."""

    platform: str
    content_format: str
    sample_count: int
    median_score: float
    mad: float


@dataclass
class EngagementAnomaly:
    """Post whose engagement is unusually high or low versus its peer baseline."""

    content_id: int
    platform: str
    content_format: str
    content_type: str
    content_preview: str
    published_at: str
    engagement_score: float
    baseline_median: float
    baseline_mad: float
    baseline_sample_count: int
    score_delta: float
    robust_z_score: float
    direction: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class EngagementAnomalyDetector:
    """Find engagement outliers by platform and content format."""

    MIN_BASELINE_SAMPLES = 5
    ROBUST_Z_THRESHOLD = 3.5

    def __init__(self, db: Database):
        self.db = db

    def detect_anomalies(
        self,
        days: int = 30,
        platform: str | None = None,
        min_score_delta: float = 5.0,
        min_baseline_samples: int = MIN_BASELINE_SAMPLES,
        z_threshold: float = ROBUST_Z_THRESHOLD,
    ) -> list[EngagementAnomaly]:
        """Return posts with unusually high or low engagement.

        Baselines are computed from recent posts on the same platform and
        content_format, excluding the candidate post being evaluated.
        """
        posts = self.get_recent_engagement_posts(days=days, platform=platform)
        anomalies: list[EngagementAnomaly] = []

        for post in posts:
            peers = [
                peer.engagement_score
                for peer in posts
                if peer.platform == post.platform
                and peer.content_format == post.content_format
                and peer.content_id != post.content_id
            ]
            if len(peers) < min_baseline_samples:
                continue

            baseline = self._compute_baseline(
                platform=post.platform,
                content_format=post.content_format,
                scores=peers,
            )
            score_delta = post.engagement_score - baseline.median_score
            if abs(score_delta) < min_score_delta:
                continue

            robust_z = self._robust_z_score(score_delta, baseline.mad)
            if abs(robust_z) < z_threshold:
                continue

            anomalies.append(
                EngagementAnomaly(
                    content_id=post.content_id,
                    platform=post.platform,
                    content_format=post.content_format,
                    content_type=post.content_type,
                    content_preview=post.content_preview,
                    published_at=post.published_at,
                    engagement_score=post.engagement_score,
                    baseline_median=baseline.median_score,
                    baseline_mad=baseline.mad,
                    baseline_sample_count=baseline.sample_count,
                    score_delta=score_delta,
                    robust_z_score=robust_z,
                    direction="high" if score_delta > 0 else "low",
                )
            )

        anomalies.sort(key=lambda item: abs(item.robust_z_score), reverse=True)
        return anomalies

    def get_recent_engagement_posts(
        self,
        days: int = 30,
        platform: str | None = None,
    ) -> list[EngagementPost]:
        """Load latest engagement snapshots for recent X and/or Bluesky posts."""
        normalized_platform = platform.lower() if platform else None
        if normalized_platform not in {None, "x", "bluesky"}:
            raise ValueError("platform must be 'x', 'bluesky', or None")

        rows: list[dict[str, Any]] = []
        if normalized_platform in {None, "x"}:
            rows.extend(self._query_x_posts(days))
        if normalized_platform in {None, "bluesky"}:
            rows.extend(self._query_bluesky_posts(days))

        return [
            EngagementPost(
                content_id=row["content_id"],
                platform=row["platform"],
                content_format=row["content_format"] or "unknown",
                content_type=row["content_type"],
                content_preview=row["content_preview"],
                published_at=row["published_at"],
                engagement_score=float(row["engagement_score"]),
                fetched_at=row["fetched_at"],
            )
            for row in rows
        ]

    def _query_x_posts(self, days: int) -> list[dict[str, Any]]:
        cursor = self.db.conn.execute(
            """SELECT gc.id AS content_id,
                      'x' AS platform,
                      COALESCE(gc.content_format, 'unknown') AS content_format,
                      gc.content_type,
                      SUBSTR(gc.content, 1, 120) AS content_preview,
                      gc.published_at,
                      pe.engagement_score,
                      pe.fetched_at
               FROM generated_content gc
               INNER JOIN post_engagement pe ON pe.id = (
                   SELECT latest.id
                   FROM post_engagement latest
                   WHERE latest.content_id = gc.id
                   ORDER BY latest.fetched_at DESC, latest.id DESC
                   LIMIT 1
               )
               WHERE gc.published = 1
                 AND gc.published_at IS NOT NULL
                 AND gc.published_at >= datetime('now', ?)
                 AND pe.engagement_score IS NOT NULL""",
            (f"-{days} days",),
        )
        return [dict(row) for row in cursor.fetchall()]

    def _query_bluesky_posts(self, days: int) -> list[dict[str, Any]]:
        cursor = self.db.conn.execute(
            """SELECT gc.id AS content_id,
                      'bluesky' AS platform,
                      COALESCE(gc.content_format, 'unknown') AS content_format,
                      gc.content_type,
                      SUBSTR(gc.content, 1, 120) AS content_preview,
                      gc.published_at,
                      be.engagement_score,
                      be.fetched_at
               FROM generated_content gc
               INNER JOIN bluesky_engagement be ON be.id = (
                   SELECT latest.id
                   FROM bluesky_engagement latest
                   WHERE latest.content_id = gc.id
                   ORDER BY latest.fetched_at DESC, latest.id DESC
                   LIMIT 1
               )
               WHERE gc.bluesky_uri IS NOT NULL
                 AND gc.published_at IS NOT NULL
                 AND gc.published_at >= datetime('now', ?)
                 AND be.engagement_score IS NOT NULL""",
            (f"-{days} days",),
        )
        return [dict(row) for row in cursor.fetchall()]

    def _compute_baseline(
        self,
        platform: str,
        content_format: str,
        scores: list[float],
    ) -> EngagementBaseline:
        median_score = float(median(scores))
        deviations = [abs(score - median_score) for score in scores]
        mad = float(median(deviations))
        return EngagementBaseline(
            platform=platform,
            content_format=content_format,
            sample_count=len(scores),
            median_score=median_score,
            mad=mad,
        )

    def _robust_z_score(self, score_delta: float, mad: float) -> float:
        if mad == 0:
            return score_delta
        return score_delta / (MAD_NORMAL_SCALE * mad)
