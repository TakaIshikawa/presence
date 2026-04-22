"""Pipeline analytics module for aggregating pipeline health metrics.

Analyzes pipeline_runs, generated_content, and post_engagement tables to provide
comprehensive insights into pipeline performance, filter effectiveness, and
score-engagement correlation.
"""

import json
import logging
import statistics
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from storage.db import Database

logger = logging.getLogger(__name__)


@dataclass
class PipelineHealthReport:
    """Comprehensive pipeline health metrics for a given period."""

    period_start: datetime
    period_end: datetime
    total_runs: int
    outcomes: dict[str, int]  # {'published': N, 'below_threshold': N, 'all_filtered': N}
    conversion_rate: float  # published / total_runs
    avg_final_score: float
    avg_candidates_per_run: float
    filter_breakdown: dict[str, int]  # {'repetition_rejected': N, 'stale_pattern_rejected': N, ...}
    score_distribution: dict[str, int]  # {'0-3': N, '3-5': N, '5-7': N, '7-9': N, '9-10': N}
    refinement_stats: dict  # {'total_refined': N, 'picked_refined': N, 'picked_original': N}
    avg_engagement_by_score_band: dict[str, float]  # engagement correlation


@dataclass
class CrossPlatformReport:
    """Cross-platform engagement comparison report."""

    period_days: int
    avg_x_score: float
    avg_bluesky_score: float
    correlation: Optional[float]  # Pearson correlation between X and Bluesky scores
    x_only_count: int
    bluesky_only_count: int
    both_count: int
    platform_winner: dict[int, str]  # content_id -> 'x', 'bluesky', or 'tie'


@dataclass
class PlatformGrowthStats:
    """Profile growth and posting/engagement summary for one platform."""

    platform: str
    start_followers: Optional[int]
    end_followers: Optional[int]
    follower_delta: Optional[int]
    follower_delta_pct: Optional[float]
    start_following: Optional[int]
    end_following: Optional[int]
    following_delta: Optional[int]
    start_post_count: Optional[int]
    end_post_count: Optional[int]
    profile_post_delta: Optional[int]
    posting_volume: int
    engagement_count: int
    avg_engagement_score: float
    min_engagement_score: float
    max_engagement_score: float
    total_engagement_score: float


@dataclass
class ProfileGrowthReport:
    """Profile growth report across one or more platforms."""

    period_days: int
    period_start: datetime
    period_end: datetime
    platforms: dict[str, PlatformGrowthStats]


@dataclass
class CampaignPerformanceReport:
    """Campaign-level topic, content, and engagement summary."""

    campaign: dict
    period_days: int
    period_start: datetime
    period_end: datetime
    topic_counts: dict[str, int]
    avg_eval_score: float
    per_platform_engagement: dict[str, dict]
    top_content: list[dict]
    gaps: list[dict]


@dataclass
class CampaignRetrospectiveReport:
    """Retrospective campaign outcome summary."""

    campaign: dict
    planned_topics: int
    generated_topics: int
    published_items: int
    avg_engagement_score: float
    top_content: list[dict]
    missed_planned_topics: list[dict]
    platform_split: dict[str, dict]


class PipelineAnalytics:
    """Analytics engine for pipeline health and performance metrics."""

    def __init__(self, db: Database):
        self.db = db

    def health_report(
        self,
        content_type: str = 'x_thread',
        days: int = 30
    ) -> Optional[PipelineHealthReport]:
        """Generate comprehensive pipeline health report.

        Args:
            content_type: Content type to analyze ('x_post', 'x_thread')
            days: Number of days to look back

        Returns:
            PipelineHealthReport or None if no data available
        """
        # Get pipeline runs for the period
        runs = self.db.get_pipeline_runs(content_type, since_days=days)

        if not runs:
            return None

        # Calculate period bounds
        period_end = datetime.now(timezone.utc)
        period_start = datetime.fromisoformat(runs[-1]['created_at'])
        if period_start.tzinfo is None:
            period_start = period_start.replace(tzinfo=timezone.utc)

        total_runs = len(runs)

        # Aggregate outcomes
        outcomes = {}
        for run in runs:
            outcome = run.get('outcome') or 'unknown'
            outcomes[outcome] = outcomes.get(outcome, 0) + 1

        published_count = outcomes.get('published', 0)
        conversion_rate = published_count / total_runs if total_runs > 0 else 0.0

        # Calculate average final score (excluding None values)
        scores = [r['final_score'] for r in runs if r.get('final_score') is not None]
        avg_final_score = sum(scores) / len(scores) if scores else 0.0

        # Calculate average candidates per run
        candidates = [r['candidates_generated'] for r in runs if r.get('candidates_generated') is not None]
        avg_candidates_per_run = sum(candidates) / len(candidates) if candidates else 0.0

        # Aggregate filter stats from JSON
        filter_breakdown = {}
        for run in runs:
            if run.get('filter_stats'):
                try:
                    stats = json.loads(run['filter_stats'])
                    for key, value in stats.items():
                        if isinstance(value, (int, float)) and value > 0:
                            filter_breakdown[key] = filter_breakdown.get(key, 0) + int(value)
                except (json.JSONDecodeError, TypeError) as e:
                    logger.debug(f"Skipping malformed filter_stats JSON in pipeline run: {e}")
                    continue

        # Calculate score distribution
        score_distribution = {
            '0-3': 0,
            '3-5': 0,
            '5-7': 0,
            '7-9': 0,
            '9-10': 0,
        }
        for score in scores:
            if score < 3:
                score_distribution['0-3'] += 1
            elif score < 5:
                score_distribution['3-5'] += 1
            elif score < 7:
                score_distribution['5-7'] += 1
            elif score < 9:
                score_distribution['7-9'] += 1
            else:
                score_distribution['9-10'] += 1

        # Calculate refinement stats
        total_refined = sum(1 for r in runs if r.get('refinement_picked') is not None)
        picked_refined = sum(1 for r in runs if r.get('refinement_picked') == 'REFINED')
        picked_original = sum(1 for r in runs if r.get('refinement_picked') == 'ORIGINAL')

        refinement_stats = {
            'total_refined': total_refined,
            'picked_refined': picked_refined,
            'picked_original': picked_original,
        }

        # Calculate engagement by score band
        avg_engagement_by_score_band = self._calculate_engagement_by_score_band(
            content_type, days
        )

        return PipelineHealthReport(
            period_start=period_start,
            period_end=period_end,
            total_runs=total_runs,
            outcomes=outcomes,
            conversion_rate=conversion_rate,
            avg_final_score=avg_final_score,
            avg_candidates_per_run=avg_candidates_per_run,
            filter_breakdown=filter_breakdown,
            score_distribution=score_distribution,
            refinement_stats=refinement_stats,
            avg_engagement_by_score_band=avg_engagement_by_score_band,
        )

    def _calculate_engagement_by_score_band(
        self,
        content_type: str,
        days: int
    ) -> dict[str, float]:
        """Calculate average engagement scores grouped by eval score bands."""
        cursor = self.db.conn.execute(
            """SELECT gc.eval_score, COALESCE(pe.engagement_score, 0) AS engagement_score
               FROM generated_content gc
               LEFT JOIN (
                   SELECT content_id, engagement_score,
                          ROW_NUMBER() OVER (
                              PARTITION BY content_id ORDER BY fetched_at DESC
                          ) AS rn
                   FROM post_engagement
               ) pe ON pe.content_id = gc.id AND pe.rn = 1
               WHERE gc.content_type = ?
                 AND gc.published = 1
                 AND gc.created_at >= datetime('now', ?)
                 AND gc.eval_score IS NOT NULL""",
            (content_type, f'-{days} days')
        )

        # Group by score bands
        bands = {
            '0-3': [],
            '3-5': [],
            '5-7': [],
            '7-9': [],
            '9-10': [],
        }

        for row in cursor.fetchall():
            eval_score = row[0]
            engagement = row[1]

            if eval_score < 3:
                bands['0-3'].append(engagement)
            elif eval_score < 5:
                bands['3-5'].append(engagement)
            elif eval_score < 7:
                bands['5-7'].append(engagement)
            elif eval_score < 9:
                bands['7-9'].append(engagement)
            else:
                bands['9-10'].append(engagement)

        # Calculate averages
        return {
            band: (sum(scores) / len(scores) if scores else 0.0)
            for band, scores in bands.items()
        }

    def filter_effectiveness(self, days: int = 30) -> dict:
        """Analyze which filters reject the most candidates.

        Args:
            days: Number of days to look back

        Returns:
            Dict with filter names as keys and rejection stats as values
        """
        cursor = self.db.conn.execute(
            """SELECT filter_stats, candidates_generated
               FROM pipeline_runs
               WHERE created_at >= datetime('now', ?)
                 AND filter_stats IS NOT NULL""",
            (f'-{days} days',)
        )

        filter_totals = {}
        total_candidates = 0

        for row in cursor.fetchall():
            try:
                stats = json.loads(row[0])
                candidates = row[1] or 0
                total_candidates += candidates

                for key, value in stats.items():
                    if isinstance(value, (int, float)) and value > 0:
                        filter_totals[key] = filter_totals.get(key, 0) + int(value)
            except (json.JSONDecodeError, TypeError) as e:
                logger.debug(f"Skipping malformed filter_stats row: {e}")
                continue

        # Calculate percentages
        result = {}
        for filter_name, count in filter_totals.items():
            percentage = (count / total_candidates * 100) if total_candidates > 0 else 0.0
            result[filter_name] = {
                'count': count,
                'percentage': round(percentage, 1),
            }

        return result

    def score_engagement_correlation(
        self,
        content_type: str = 'x_thread'
    ) -> list[dict]:
        """Return eval_score vs actual engagement_score pairs for calibration analysis.

        Args:
            content_type: Content type to analyze

        Returns:
            List of dicts with eval_score, engagement_score, content_type, published_at
        """
        cursor = self.db.conn.execute(
            """SELECT gc.id, gc.eval_score, pe.engagement_score,
                      gc.content_type, gc.published_at
               FROM generated_content gc
               INNER JOIN (
                   SELECT content_id, engagement_score,
                          ROW_NUMBER() OVER (
                              PARTITION BY content_id ORDER BY fetched_at DESC
                          ) AS rn
                   FROM post_engagement
               ) pe ON pe.content_id = gc.id AND pe.rn = 1
               WHERE gc.content_type = ?
                 AND gc.published = 1
                 AND gc.eval_score IS NOT NULL
                 AND pe.engagement_score IS NOT NULL
               ORDER BY gc.published_at DESC""",
            (content_type,)
        )

        return [
            {
                'content_id': row[0],
                'eval_score': row[1],
                'engagement_score': row[2],
                'content_type': row[3],
                'published_at': row[4],
            }
            for row in cursor.fetchall()
        ]

    def trend(
        self,
        content_type: str = 'x_thread',
        weeks: int = 8
    ) -> list[dict]:
        """Weekly trend data: runs, conversion rate, avg score, avg engagement.

        Args:
            content_type: Content type to analyze
            weeks: Number of weeks to look back

        Returns:
            List of weekly aggregates sorted by week
        """
        cursor = self.db.conn.execute(
            """SELECT
                   strftime('%Y-%W', pr.created_at) AS week,
                   COUNT(*) AS runs,
                   SUM(CASE WHEN pr.outcome = 'published' THEN 1 ELSE 0 END) AS published,
                   AVG(CASE WHEN pr.final_score IS NOT NULL THEN pr.final_score ELSE NULL END) AS avg_score
               FROM pipeline_runs pr
               WHERE pr.content_type = ?
                 AND pr.created_at >= datetime('now', ?)
               GROUP BY week
               ORDER BY week""",
            (content_type, f'-{weeks * 7} days')
        )

        weeks_data = []
        for row in cursor.fetchall():
            week = row[0]
            runs = row[1]
            published = row[2]
            avg_score = row[3]

            # Get average engagement for this week's published content
            eng_cursor = self.db.conn.execute(
                """SELECT AVG(pe.engagement_score) AS avg_engagement
                   FROM generated_content gc
                   INNER JOIN (
                       SELECT content_id, engagement_score,
                              ROW_NUMBER() OVER (
                                  PARTITION BY content_id ORDER BY fetched_at DESC
                              ) AS rn
                       FROM post_engagement
                   ) pe ON pe.content_id = gc.id AND pe.rn = 1
                   WHERE gc.content_type = ?
                     AND gc.published = 1
                     AND strftime('%Y-%W', gc.created_at) = ?""",
                (content_type, week)
            )
            eng_row = eng_cursor.fetchone()
            avg_engagement = eng_row[0] if eng_row and eng_row[0] is not None else 0.0

            conversion_rate = (published / runs * 100) if runs > 0 else 0.0

            weeks_data.append({
                'week': week,
                'runs': runs,
                'published': published,
                'conversion_rate': round(conversion_rate, 1),
                'avg_score': round(avg_score, 1) if avg_score is not None else 0.0,
                'avg_engagement': round(avg_engagement, 1),
            })

        return weeks_data

    def cross_platform_comparison(self, days: int = 30) -> CrossPlatformReport:
        """Compare engagement across X and Bluesky platforms.

        Args:
            days: Number of days to look back

        Returns:
            CrossPlatformReport with comparative metrics
        """
        # Get content posted to both platforms with latest engagement scores
        cursor = self.db.conn.execute(
            """SELECT gc.id,
                      pe.engagement_score AS x_score,
                      be.engagement_score AS bluesky_score
               FROM generated_content gc
               LEFT JOIN (
                   SELECT content_id, engagement_score,
                          ROW_NUMBER() OVER (
                              PARTITION BY content_id ORDER BY fetched_at DESC
                          ) AS rn
                   FROM post_engagement
               ) pe ON pe.content_id = gc.id AND pe.rn = 1
               LEFT JOIN (
                   SELECT content_id, engagement_score,
                          ROW_NUMBER() OVER (
                              PARTITION BY content_id ORDER BY fetched_at DESC
                          ) AS rn
                   FROM bluesky_engagement
               ) be ON be.content_id = gc.id AND be.rn = 1
               WHERE gc.published = 1
                 AND gc.published_at >= datetime('now', ?)
                 AND (pe.engagement_score IS NOT NULL OR be.engagement_score IS NOT NULL)""",
            (f'-{days} days',)
        )

        x_scores = []
        bluesky_scores = []
        x_only_count = 0
        bluesky_only_count = 0
        both_count = 0
        platform_winner = {}

        for row in cursor.fetchall():
            content_id = row[0]
            x_score = row[1]
            bluesky_score = row[2]

            if x_score is not None and bluesky_score is not None:
                both_count += 1
                x_scores.append(x_score)
                bluesky_scores.append(bluesky_score)

                # Determine winner
                if x_score > bluesky_score:
                    platform_winner[content_id] = 'x'
                elif bluesky_score > x_score:
                    platform_winner[content_id] = 'bluesky'
                else:
                    platform_winner[content_id] = 'tie'
            elif x_score is not None:
                x_only_count += 1
            elif bluesky_score is not None:
                bluesky_only_count += 1

        # Calculate averages
        avg_x_score = sum(x_scores) / len(x_scores) if x_scores else 0.0
        avg_bluesky_score = sum(bluesky_scores) / len(bluesky_scores) if bluesky_scores else 0.0

        # Calculate correlation if we have enough paired samples
        correlation = None
        if len(x_scores) >= 3 and len(bluesky_scores) >= 3:
            try:
                correlation = statistics.correlation(x_scores, bluesky_scores)
            except statistics.StatisticsError:
                correlation = None

        return CrossPlatformReport(
            period_days=days,
            avg_x_score=round(avg_x_score, 2),
            avg_bluesky_score=round(avg_bluesky_score, 2),
            correlation=round(correlation, 3) if correlation is not None else None,
            x_only_count=x_only_count,
            bluesky_only_count=bluesky_only_count,
            both_count=both_count,
            platform_winner=platform_winner,
        )

    def profile_growth_report(
        self,
        days: int = 30,
        platform: str = "all",
    ) -> ProfileGrowthReport:
        """Summarize profile growth, posting volume, and engagement by platform.

        Args:
            days: Number of days to look back
            platform: 'x', 'bluesky', or 'all'

        Returns:
            ProfileGrowthReport with one PlatformGrowthStats per requested platform
        """
        if platform == "all":
            platforms = ["x", "bluesky"]
        elif platform in {"x", "bluesky"}:
            platforms = [platform]
        else:
            raise ValueError("platform must be one of: x, bluesky, all")

        period_end = datetime.now(timezone.utc)
        period_start = period_end - timedelta(days=days)

        return ProfileGrowthReport(
            period_days=days,
            period_start=period_start,
            period_end=period_end,
            platforms={
                name: self._profile_growth_for_platform(name, days)
                for name in platforms
            },
        )

    def _profile_growth_for_platform(
        self,
        platform: str,
        days: int,
    ) -> PlatformGrowthStats:
        """Build profile growth stats for a single platform."""
        profile_rows = self._profile_metric_bounds(platform, days)
        first_profile = profile_rows[0] if profile_rows else None
        latest_profile = profile_rows[-1] if profile_rows else None

        start_followers = first_profile["follower_count"] if first_profile else None
        end_followers = latest_profile["follower_count"] if latest_profile else None
        follower_delta = (
            end_followers - start_followers
            if start_followers is not None and end_followers is not None
            else None
        )
        follower_delta_pct = (
            (follower_delta / start_followers * 100)
            if follower_delta is not None and start_followers
            else None
        )

        start_following = first_profile["following_count"] if first_profile else None
        end_following = latest_profile["following_count"] if latest_profile else None
        following_delta = (
            end_following - start_following
            if start_following is not None and end_following is not None
            else None
        )

        start_post_count = first_profile["tweet_count"] if first_profile else None
        end_post_count = latest_profile["tweet_count"] if latest_profile else None
        profile_post_delta = (
            end_post_count - start_post_count
            if start_post_count is not None and end_post_count is not None
            else None
        )

        posting_volume = self._posting_volume(platform, days)
        engagement_scores = self._latest_engagement_scores(platform, days)

        return PlatformGrowthStats(
            platform=platform,
            start_followers=start_followers,
            end_followers=end_followers,
            follower_delta=follower_delta,
            follower_delta_pct=round(follower_delta_pct, 2)
            if follower_delta_pct is not None else None,
            start_following=start_following,
            end_following=end_following,
            following_delta=following_delta,
            start_post_count=start_post_count,
            end_post_count=end_post_count,
            profile_post_delta=profile_post_delta,
            posting_volume=posting_volume,
            engagement_count=len(engagement_scores),
            avg_engagement_score=round(sum(engagement_scores) / len(engagement_scores), 2)
            if engagement_scores else 0.0,
            min_engagement_score=round(min(engagement_scores), 2)
            if engagement_scores else 0.0,
            max_engagement_score=round(max(engagement_scores), 2)
            if engagement_scores else 0.0,
            total_engagement_score=round(sum(engagement_scores), 2),
        )

    def _profile_metric_bounds(self, platform: str, days: int) -> list[dict]:
        """Return profile metric snapshots in the requested period."""
        cursor = self.db.conn.execute(
            """SELECT follower_count, following_count, tweet_count,
                      listed_count, fetched_at
               FROM profile_metrics
               WHERE platform = ?
                 AND fetched_at >= datetime('now', ?)
               ORDER BY fetched_at ASC""",
            (platform, f'-{days} days'),
        )
        return [dict(row) for row in cursor.fetchall()]

    def _posting_volume(self, platform: str, days: int) -> int:
        """Count successful platform publications in the requested period."""
        cursor = self.db.conn.execute(
            """SELECT COUNT(*)
               FROM content_publications
               WHERE platform = ?
                 AND status = 'published'
                 AND published_at >= datetime('now', ?)""",
            (platform, f'-{days} days'),
        )
        row = cursor.fetchone()
        return int(row[0]) if row else 0

    def _latest_engagement_scores(self, platform: str, days: int) -> list[float]:
        """Return latest engagement score per published item for one platform."""
        if platform == "x":
            engagement_table = "post_engagement"
        elif platform == "bluesky":
            engagement_table = "bluesky_engagement"
        else:
            raise ValueError("platform must be one of: x, bluesky")

        cursor = self.db.conn.execute(
            f"""SELECT latest.engagement_score
                FROM content_publications cp
                INNER JOIN (
                    SELECT content_id, engagement_score,
                           ROW_NUMBER() OVER (
                               PARTITION BY content_id ORDER BY fetched_at DESC
                           ) AS rn
                    FROM {engagement_table}
                    WHERE engagement_score IS NOT NULL
                ) latest ON latest.content_id = cp.content_id AND latest.rn = 1
                WHERE cp.platform = ?
                  AND cp.status = 'published'
                  AND cp.published_at >= datetime('now', ?)""",
            (platform, f'-{days} days'),
        )
        return [float(row[0]) for row in cursor.fetchall()]

    def campaign_performance_report(
        self,
        campaign_id: int | None = None,
        active: bool = False,
        days: int = 30,
        top_limit: int = 5,
    ) -> Optional[CampaignPerformanceReport]:
        """Summarize topic completion and performance for one content campaign.

        Args:
            campaign_id: Explicit campaign ID to report.
            active: If true, report the currently active campaign.
            days: Engagement/newsletter lookback window.
            top_limit: Number of top content items to include.

        Returns:
            CampaignPerformanceReport or None if the campaign does not exist.
        """
        campaign = self._resolve_campaign(campaign_id=campaign_id, active=active)
        if campaign is None:
            return None

        period_end = datetime.now(timezone.utc)
        period_start = period_end - timedelta(days=days)
        topics = self._campaign_topic_rows(campaign["id"])
        topic_counts = self._campaign_topic_counts(topics)
        content_rows = self._campaign_content_rows(campaign["id"], days)
        newsletter_by_content = self._newsletter_sends_by_content(
            [row["content_id"] for row in content_rows if row.get("content_id")],
            days,
        )

        content_items = []
        eval_scores = []
        x_scores = []
        bluesky_scores = []
        newsletter_sends: dict[int, list[dict]] = {}

        for row in content_rows:
            content_id = row["content_id"]
            x_score = row["x_engagement_score"]
            bluesky_score = row["bluesky_engagement_score"]
            sends = newsletter_by_content.get(content_id, [])
            newsletter_sends[content_id] = sends

            if row["eval_score"] is not None:
                eval_scores.append(float(row["eval_score"]))
            if x_score is not None:
                x_scores.append(float(x_score))
            if bluesky_score is not None:
                bluesky_scores.append(float(bluesky_score))

            content_items.append({
                "planned_topic_id": row["planned_topic_id"],
                "content_id": content_id,
                "topic": row["topic"],
                "angle": row["angle"],
                "content_type": row["content_type"],
                "content": row["content"],
                "eval_score": row["eval_score"],
                "published": row["published"],
                "published_url": row["published_url"],
                "published_at": row["published_at"],
                "x_engagement_score": x_score,
                "bluesky_engagement_score": bluesky_score,
                "newsletter_sends": sends,
                "combined_engagement_score": (
                    float(x_score or 0.0) + float(bluesky_score or 0.0)
                ),
            })

        top_content = sorted(
            content_items,
            key=lambda item: (
                item["combined_engagement_score"],
                item["eval_score"] or 0.0,
                item["published_at"] or "",
            ),
            reverse=True,
        )[:top_limit]

        total_subscribers = sum(
            send["subscriber_count"] or 0
            for sends in newsletter_sends.values()
            for send in sends
        )
        unique_newsletter_sends = {
            send["send_id"]: send
            for sends in newsletter_sends.values()
            for send in sends
        }

        per_platform = {
            "x": self._engagement_summary(x_scores),
            "bluesky": self._engagement_summary(bluesky_scores),
            "newsletter": {
                "send_count": len(unique_newsletter_sends),
                "content_count": sum(1 for sends in newsletter_sends.values() if sends),
                "subscriber_count_total": total_subscribers,
                "avg_subscribers_per_send": round(
                    total_subscribers / len(unique_newsletter_sends), 2
                ) if unique_newsletter_sends else 0.0,
            },
        }

        return CampaignPerformanceReport(
            campaign=campaign,
            period_days=days,
            period_start=period_start,
            period_end=period_end,
            topic_counts=topic_counts,
            avg_eval_score=round(sum(eval_scores) / len(eval_scores), 2)
            if eval_scores else 0.0,
            per_platform_engagement=per_platform,
            top_content=top_content,
            gaps=self._campaign_gaps(topics, content_items),
        )

    def campaign_retrospectives(
        self,
        campaign_id: int | None = None,
        active: bool = False,
        statuses: tuple[str, ...] = ("active", "completed"),
        top_limit: int = 3,
    ) -> list[CampaignRetrospectiveReport]:
        """Return retrospective performance totals for one or more campaigns.

        By default, retrospectives cover active and completed campaigns so a
        completed campaign remains reportable after it leaves the active window.
        """
        campaigns = self._resolve_retrospective_campaigns(
            campaign_id=campaign_id,
            active=active,
            statuses=statuses,
        )
        return [
            self._campaign_retrospective_from_rows(campaign, top_limit=top_limit)
            for campaign in campaigns
        ]

    def campaign_retrospective_report(
        self,
        campaign_id: int | None = None,
        active: bool = False,
        top_limit: int = 3,
    ) -> Optional[CampaignRetrospectiveReport]:
        """Return one retrospective campaign report, or None if not found."""
        reports = self.campaign_retrospectives(
            campaign_id=campaign_id,
            active=active,
            top_limit=top_limit,
        )
        return reports[0] if reports else None

    def _resolve_campaign(
        self,
        campaign_id: int | None,
        active: bool,
    ) -> dict | None:
        """Resolve campaign selection for campaign reporting."""
        if campaign_id is not None:
            return self.db.get_campaign(campaign_id)
        if active:
            return self.db.get_active_campaign()

        cursor = self.db.conn.execute(
            """SELECT *
               FROM content_campaigns
               ORDER BY start_date DESC NULLS LAST, created_at DESC
               LIMIT 1"""
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    def _resolve_retrospective_campaigns(
        self,
        campaign_id: int | None,
        active: bool,
        statuses: tuple[str, ...],
    ) -> list[dict]:
        """Resolve campaign selection for retrospective reporting."""
        if campaign_id is not None:
            campaign = self.db.get_campaign(campaign_id)
            return [campaign] if campaign else []
        if active:
            campaign = self.db.get_active_campaign()
            return [campaign] if campaign else []

        placeholders = ",".join("?" for _ in statuses)
        cursor = self.db.conn.execute(
            f"""SELECT *
                FROM content_campaigns
                WHERE status IN ({placeholders})
                ORDER BY start_date DESC NULLS LAST, created_at DESC, id DESC""",
            statuses,
        )
        return [dict(row) for row in cursor.fetchall()]

    def _campaign_retrospective_from_rows(
        self,
        campaign: dict,
        top_limit: int,
    ) -> CampaignRetrospectiveReport:
        """Build a retrospective summary from campaign/topic/publication rows."""
        rows = self._campaign_retrospective_rows(campaign["id"])
        generated_topic_ids = {
            row["planned_topic_id"]
            for row in rows
            if row["planned_topic_id"] is not None and row["content_id"] is not None
        }
        published_rows = [
            row for row in rows
            if row["publication_id"] is not None and row["publication_status"] == "published"
        ]

        content_items = self._campaign_retrospective_content_items(rows)
        engagement_scores = [
            item["combined_engagement_score"]
            for item in content_items
            if item["published_platforms"]
        ]
        top_content = sorted(
            [item for item in content_items if item["published_platforms"]],
            key=lambda item: (
                item["combined_engagement_score"],
                item["eval_score"] or 0.0,
                item["latest_published_at"] or "",
            ),
            reverse=True,
        )[:top_limit]

        missed_planned_topics = [
            {
                "planned_topic_id": row["planned_topic_id"],
                "topic": row["topic"],
                "angle": row["angle"],
                "target_date": row["target_date"],
                "status": row["topic_status"],
            }
            for row in rows
            if row["planned_topic_id"] is not None
            and row["content_id"] is None
            and row["topic_status"] != "skipped"
        ]

        return CampaignRetrospectiveReport(
            campaign=campaign,
            planned_topics=len({
                row["planned_topic_id"]
                for row in rows
                if row["planned_topic_id"] is not None
            }),
            generated_topics=len(generated_topic_ids),
            published_items=len(published_rows),
            avg_engagement_score=round(
                sum(engagement_scores) / len(engagement_scores), 2
            ) if engagement_scores else 0.0,
            top_content=top_content,
            missed_planned_topics=missed_planned_topics,
            platform_split=self._campaign_retrospective_platform_split(published_rows),
        )

    def _campaign_retrospective_rows(self, campaign_id: int) -> list[dict]:
        """Join campaign topics, generated content, publications, and engagement."""
        cursor = self.db.conn.execute(
            """SELECT cc.id AS campaign_id,
                      pt.id AS planned_topic_id,
                      pt.topic,
                      pt.angle,
                      pt.target_date,
                      pt.status AS topic_status,
                      gc.id AS content_id,
                      gc.content_type,
                      gc.content,
                      gc.eval_score,
                      gc.created_at AS content_created_at,
                      cp.id AS publication_id,
                      cp.platform,
                      cp.status AS publication_status,
                      cp.platform_url,
                      cp.published_at AS platform_published_at,
                      CASE
                          WHEN cp.platform = 'x' THEN pe.engagement_score
                          WHEN cp.platform = 'bluesky' THEN be.engagement_score
                          ELSE NULL
                      END AS engagement_score
               FROM content_campaigns cc
               LEFT JOIN planned_topics pt ON pt.campaign_id = cc.id
               LEFT JOIN generated_content gc ON gc.id = pt.content_id
               LEFT JOIN content_publications cp
                      ON cp.content_id = gc.id
                     AND cp.status = 'published'
               LEFT JOIN (
                   SELECT content_id, engagement_score,
                          ROW_NUMBER() OVER (
                              PARTITION BY content_id ORDER BY fetched_at DESC
                          ) AS rn
                   FROM post_engagement
                   WHERE engagement_score IS NOT NULL
               ) pe ON pe.content_id = gc.id AND pe.rn = 1
               LEFT JOIN (
                   SELECT content_id, engagement_score,
                          ROW_NUMBER() OVER (
                              PARTITION BY content_id ORDER BY fetched_at DESC
                          ) AS rn
                   FROM bluesky_engagement
                   WHERE engagement_score IS NOT NULL
               ) be ON be.content_id = gc.id AND be.rn = 1
               WHERE cc.id = ?
               ORDER BY pt.target_date ASC NULLS LAST,
                        pt.created_at ASC,
                        gc.created_at ASC,
                        cp.platform ASC""",
            (campaign_id,),
        )
        return [dict(row) for row in cursor.fetchall()]

    def _campaign_retrospective_content_items(self, rows: list[dict]) -> list[dict]:
        """Collapse publication rows into one scored item per generated content row."""
        items: dict[int, dict] = {}
        for row in rows:
            content_id = row["content_id"]
            if content_id is None:
                continue
            item = items.setdefault(
                content_id,
                {
                    "planned_topic_id": row["planned_topic_id"],
                    "content_id": content_id,
                    "topic": row["topic"],
                    "angle": row["angle"],
                    "content_type": row["content_type"],
                    "content": row["content"],
                    "eval_score": row["eval_score"],
                    "published_platforms": [],
                    "platform_scores": {},
                    "combined_engagement_score": 0.0,
                    "latest_published_at": None,
                },
            )
            if row["publication_status"] != "published" or not row["platform"]:
                continue

            platform = row["platform"]
            if platform not in item["published_platforms"]:
                item["published_platforms"].append(platform)
            score = row["engagement_score"]
            if score is not None:
                score = float(score)
                item["platform_scores"][platform] = score
                item["combined_engagement_score"] += score
            published_at = row["platform_published_at"]
            if published_at and (
                item["latest_published_at"] is None
                or published_at > item["latest_published_at"]
            ):
                item["latest_published_at"] = published_at

        for item in items.values():
            item["combined_engagement_score"] = round(
                item["combined_engagement_score"], 2
            )
            item["published_platforms"].sort()
        return list(items.values())

    def _campaign_retrospective_platform_split(
        self,
        published_rows: list[dict],
    ) -> dict[str, dict]:
        """Aggregate publication count and latest engagement by platform."""
        platforms: dict[str, dict] = {}
        for row in published_rows:
            platform = row["platform"] or "unknown"
            stats = platforms.setdefault(
                platform,
                {
                    "published_items": 0,
                    "engagement_count": 0,
                    "avg_engagement_score": 0.0,
                    "total_engagement_score": 0.0,
                },
            )
            stats["published_items"] += 1
            if row["engagement_score"] is not None:
                stats["engagement_count"] += 1
                stats["total_engagement_score"] += float(row["engagement_score"])

        for stats in platforms.values():
            total = stats["total_engagement_score"]
            count = stats["engagement_count"]
            stats["total_engagement_score"] = round(total, 2)
            stats["avg_engagement_score"] = round(total / count, 2) if count else 0.0
        return platforms

    def _campaign_topic_rows(self, campaign_id: int) -> list[dict]:
        """Return all planned topics for a campaign."""
        cursor = self.db.conn.execute(
            """SELECT *
               FROM planned_topics
               WHERE campaign_id = ?
               ORDER BY target_date ASC NULLS LAST, created_at ASC""",
            (campaign_id,),
        )
        return [dict(row) for row in cursor.fetchall()]

    def _campaign_topic_counts(self, topics: list[dict]) -> dict[str, int]:
        """Count planned topic statuses using stable output keys."""
        counts = {
            "total": len(topics),
            "planned": 0,
            "generated": 0,
            "skipped": 0,
        }
        for topic in topics:
            status = topic.get("status") or "planned"
            counts[status] = counts.get(status, 0) + 1
        return counts

    def _campaign_content_rows(self, campaign_id: int, days: int) -> list[dict]:
        """Return generated content linked to campaign topics with latest engagement."""
        cursor = self.db.conn.execute(
            """SELECT pt.id AS planned_topic_id,
                      pt.topic,
                      pt.angle,
                      pt.status AS topic_status,
                      gc.id AS content_id,
                      gc.content_type,
                      gc.content,
                      gc.eval_score,
                      gc.published,
                      gc.published_url,
                      gc.published_at,
                      gc.created_at,
                      pe.engagement_score AS x_engagement_score,
                      be.engagement_score AS bluesky_engagement_score
               FROM planned_topics pt
               INNER JOIN generated_content gc ON gc.id = pt.content_id
               LEFT JOIN (
                   SELECT content_id, engagement_score,
                          ROW_NUMBER() OVER (
                              PARTITION BY content_id ORDER BY fetched_at DESC
                          ) AS rn
                   FROM post_engagement
                   WHERE fetched_at >= datetime('now', ?)
               ) pe ON pe.content_id = gc.id AND pe.rn = 1
               LEFT JOIN (
                   SELECT content_id, engagement_score,
                          ROW_NUMBER() OVER (
                              PARTITION BY content_id ORDER BY fetched_at DESC
                          ) AS rn
                   FROM bluesky_engagement
                   WHERE fetched_at >= datetime('now', ?)
               ) be ON be.content_id = gc.id AND be.rn = 1
               WHERE pt.campaign_id = ?
               ORDER BY gc.published_at DESC NULLS LAST, gc.created_at DESC""",
            (f"-{days} days", f"-{days} days", campaign_id),
        )
        return [dict(row) for row in cursor.fetchall()]

    def _newsletter_sends_by_content(
        self,
        content_ids: list[int],
        days: int,
    ) -> dict[int, list[dict]]:
        """Map generated content IDs to newsletter sends that included them."""
        result = {content_id: [] for content_id in content_ids}
        if not content_ids:
            return result

        content_id_set = set(content_ids)
        cursor = self.db.conn.execute(
            """SELECT id, issue_id, subject, source_content_ids,
                      subscriber_count, status, sent_at
               FROM newsletter_sends
               WHERE sent_at >= datetime('now', ?)
                 AND source_content_ids IS NOT NULL
               ORDER BY sent_at DESC""",
            (f"-{days} days",),
        )

        for row in cursor.fetchall():
            try:
                source_ids = json.loads(row["source_content_ids"] or "[]")
            except (TypeError, json.JSONDecodeError) as e:
                logger.debug(f"Skipping malformed newsletter source_content_ids: {e}")
                continue

            for source_id in source_ids:
                try:
                    content_id = int(source_id)
                except (TypeError, ValueError):
                    continue
                if content_id not in content_id_set:
                    continue

                result[content_id].append({
                    "send_id": row["id"],
                    "issue_id": row["issue_id"],
                    "subject": row["subject"],
                    "subscriber_count": row["subscriber_count"],
                    "status": row["status"],
                    "sent_at": row["sent_at"],
                })

        return result

    def _engagement_summary(self, scores: list[float]) -> dict:
        """Return aggregate engagement metrics for a platform."""
        return {
            "content_count": len(scores),
            "avg_engagement_score": round(sum(scores) / len(scores), 2)
            if scores else 0.0,
            "total_engagement_score": round(sum(scores), 2),
            "min_engagement_score": round(min(scores), 2) if scores else 0.0,
            "max_engagement_score": round(max(scores), 2) if scores else 0.0,
        }

    def _campaign_gaps(
        self,
        topics: list[dict],
        content_items: list[dict],
    ) -> list[dict]:
        """Identify missing or under-instrumented campaign items."""
        gaps = []
        today = datetime.now(timezone.utc).date().isoformat()
        content_by_topic = {
            item["planned_topic_id"]: item for item in content_items
        }

        for topic in topics:
            if topic.get("status") == "skipped":
                gaps.append({
                    "type": "skipped_topic",
                    "planned_topic_id": topic["id"],
                    "topic": topic["topic"],
                    "angle": topic["angle"],
                    "target_date": topic["target_date"],
                })
                continue

            if topic.get("status") == "planned" and (
                not topic.get("target_date") or topic["target_date"] < today
            ):
                gaps.append({
                    "type": "unfilled_topic",
                    "planned_topic_id": topic["id"],
                    "topic": topic["topic"],
                    "angle": topic["angle"],
                    "target_date": topic["target_date"],
                })

        for item in content_items:
            if item["eval_score"] is None:
                gaps.append({
                    "type": "missing_eval_score",
                    "planned_topic_id": item["planned_topic_id"],
                    "content_id": item["content_id"],
                    "topic": item["topic"],
                })
            if item["published"] == 1 and (
                item["x_engagement_score"] is None
                and item["bluesky_engagement_score"] is None
            ):
                gaps.append({
                    "type": "missing_engagement",
                    "planned_topic_id": item["planned_topic_id"],
                    "content_id": item["content_id"],
                    "topic": item["topic"],
                })

        linked_topic_ids = {
            topic["id"] for topic in topics if topic.get("content_id") is not None
        }
        missing_content = linked_topic_ids - set(content_by_topic)
        for planned_topic_id in sorted(missing_content):
            topic = next(t for t in topics if t["id"] == planned_topic_id)
            gaps.append({
                "type": "missing_content_link",
                "planned_topic_id": topic["id"],
                "topic": topic["topic"],
                "content_id": topic["content_id"],
            })

        return gaps
