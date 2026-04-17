"""Pipeline analytics module for aggregating pipeline health metrics.

Analyzes pipeline_runs, generated_content, and post_engagement tables to provide
comprehensive insights into pipeline performance, filter effectiveness, and
score-engagement correlation.
"""

import json
import logging
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
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
