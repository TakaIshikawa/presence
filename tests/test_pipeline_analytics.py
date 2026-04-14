"""Tests for pipeline analytics module."""

import json
from datetime import datetime, timedelta, timezone

import pytest

from evaluation.pipeline_analytics import PipelineAnalytics, PipelineHealthReport


@pytest.fixture
def sample_pipeline_runs(db):
    """Create sample pipeline runs with various outcomes."""
    now = datetime.now(timezone.utc)

    # Published run
    db.conn.execute(
        """INSERT INTO pipeline_runs
           (batch_id, content_type, candidates_generated,
            best_candidate_index, best_score_before_refine,
            best_score_after_refine, refinement_picked,
            final_score, published, outcome, filter_stats, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "batch-1", "x_thread", 3, 0, 7.5, 8.2, "REFINED",
            8.2, 1, "published",
            json.dumps({"repetition_rejected": 1, "stale_pattern_rejected": 0}),
            (now - timedelta(days=1)).isoformat()
        )
    )

    # Below threshold run
    db.conn.execute(
        """INSERT INTO pipeline_runs
           (batch_id, content_type, candidates_generated,
            best_candidate_index, best_score_before_refine,
            final_score, published, outcome, rejection_reason,
            filter_stats, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "batch-2", "x_thread", 4, 1, 5.5, 5.5, 0,
            "below_threshold", "Score 5.5 below threshold 7.0",
            json.dumps({"repetition_rejected": 2, "semantic_dedup_rejected": 1}),
            (now - timedelta(days=2)).isoformat()
        )
    )

    # All filtered run
    db.conn.execute(
        """INSERT INTO pipeline_runs
           (batch_id, content_type, candidates_generated,
            best_candidate_index, best_score_before_refine,
            published, outcome, rejection_reason,
            filter_stats, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "batch-3", "x_thread", 5, None, None, 0,
            "all_filtered", "All candidates filtered",
            json.dumps({"stale_pattern_rejected": 3, "repetition_rejected": 2}),
            (now - timedelta(days=3)).isoformat()
        )
    )

    # Another published run with original picked
    db.conn.execute(
        """INSERT INTO pipeline_runs
           (batch_id, content_type, candidates_generated,
            best_candidate_index, best_score_before_refine,
            best_score_after_refine, refinement_picked,
            final_score, published, outcome, filter_stats, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "batch-4", "x_thread", 2, 0, 9.1, 8.8, "ORIGINAL",
            9.1, 1, "published",
            json.dumps({"repetition_rejected": 0}),
            (now - timedelta(days=5)).isoformat()
        )
    )

    db.conn.commit()
    return db


@pytest.fixture
def sample_content_with_engagement(db, sample_pipeline_runs):
    """Create generated content with engagement data."""
    now = datetime.now(timezone.utc)

    # Insert content for published runs
    content_id_1 = db.insert_generated_content(
        content_type="x_thread",
        source_commits=["abc123"],
        source_messages=["msg-1"],
        content="Published thread 1",
        eval_score=8.2,
        eval_feedback="Good"
    )
    db.mark_published(content_id_1, "https://x.com/test/1", "tweet-1")

    content_id_2 = db.insert_generated_content(
        content_type="x_thread",
        source_commits=["def456"],
        source_messages=["msg-2"],
        content="Published thread 2",
        eval_score=9.1,
        eval_feedback="Excellent"
    )
    db.mark_published(content_id_2, "https://x.com/test/2", "tweet-2")

    # Add engagement data
    db.insert_engagement(
        content_id=content_id_1,
        tweet_id="tweet-1",
        like_count=10,
        retweet_count=2,
        reply_count=1,
        quote_count=0,
        engagement_score=12.5
    )

    db.insert_engagement(
        content_id=content_id_2,
        tweet_id="tweet-2",
        like_count=25,
        retweet_count=5,
        reply_count=3,
        quote_count=1,
        engagement_score=28.0
    )

    return db


class TestPipelineAnalytics:
    def test_health_report_basic(self, sample_pipeline_runs):
        """Test basic health report generation."""
        analytics = PipelineAnalytics(sample_pipeline_runs)
        report = analytics.health_report(content_type="x_thread", days=30)

        assert report is not None
        assert isinstance(report, PipelineHealthReport)
        assert report.total_runs == 4
        assert report.outcomes["published"] == 2
        assert report.outcomes["below_threshold"] == 1
        assert report.outcomes["all_filtered"] == 1

    def test_health_report_conversion_rate(self, sample_pipeline_runs):
        """Test conversion rate calculation."""
        analytics = PipelineAnalytics(sample_pipeline_runs)
        report = analytics.health_report(content_type="x_thread", days=30)

        # 2 published out of 4 total = 50%
        assert report.conversion_rate == 0.5

    def test_health_report_avg_scores(self, sample_pipeline_runs):
        """Test average score calculations."""
        analytics = PipelineAnalytics(sample_pipeline_runs)
        report = analytics.health_report(content_type="x_thread", days=30)

        # Scores: 8.2, 5.5, 9.1 (all_filtered has no final_score)
        # Average: (8.2 + 5.5 + 9.1) / 3 = 7.6
        assert round(report.avg_final_score, 1) == 7.6

    def test_health_report_avg_candidates(self, sample_pipeline_runs):
        """Test average candidates per run."""
        analytics = PipelineAnalytics(sample_pipeline_runs)
        report = analytics.health_report(content_type="x_thread", days=30)

        # Candidates: 3, 4, 5, 2
        # Average: (3 + 4 + 5 + 2) / 4 = 3.5
        assert report.avg_candidates_per_run == 3.5

    def test_health_report_filter_breakdown(self, sample_pipeline_runs):
        """Test filter stats aggregation."""
        analytics = PipelineAnalytics(sample_pipeline_runs)
        report = analytics.health_report(content_type="x_thread", days=30)

        # Total repetition_rejected: 1 + 2 + 2 + 0 = 5
        assert report.filter_breakdown["repetition_rejected"] == 5
        # Total stale_pattern_rejected: 0 + 0 + 3 + 0 = 3
        assert report.filter_breakdown["stale_pattern_rejected"] == 3
        # semantic_dedup_rejected: 0 + 1 + 0 + 0 = 1
        assert report.filter_breakdown["semantic_dedup_rejected"] == 1

    def test_health_report_score_distribution(self, sample_pipeline_runs):
        """Test score distribution bucketing."""
        analytics = PipelineAnalytics(sample_pipeline_runs)
        report = analytics.health_report(content_type="x_thread", days=30)

        # Scores: 8.2 (7-9), 5.5 (5-7), 9.1 (9-10)
        assert report.score_distribution["7-9"] == 1
        assert report.score_distribution["5-7"] == 1
        assert report.score_distribution["9-10"] == 1
        assert report.score_distribution["0-3"] == 0
        assert report.score_distribution["3-5"] == 0

    def test_health_report_refinement_stats(self, sample_pipeline_runs):
        """Test refinement statistics."""
        analytics = PipelineAnalytics(sample_pipeline_runs)
        report = analytics.health_report(content_type="x_thread", days=30)

        # 2 runs had refinement (batch-1 and batch-4)
        assert report.refinement_stats["total_refined"] == 2
        # 1 picked REFINED (batch-1)
        assert report.refinement_stats["picked_refined"] == 1
        # 1 picked ORIGINAL (batch-4)
        assert report.refinement_stats["picked_original"] == 1

    def test_health_report_no_data(self, db):
        """Test health report with no data."""
        analytics = PipelineAnalytics(db)
        report = analytics.health_report(content_type="x_thread", days=30)

        assert report is None

    def test_health_report_different_content_type(self, sample_pipeline_runs):
        """Test filtering by content type."""
        # Add an x_post run
        now = datetime.now(timezone.utc)
        sample_pipeline_runs.conn.execute(
            """INSERT INTO pipeline_runs
               (batch_id, content_type, candidates_generated,
                best_candidate_index, best_score_before_refine,
                final_score, published, outcome, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                "batch-5", "x_post", 3, 0, 7.0, 7.0, 1, "published",
                (now - timedelta(days=1)).isoformat()
            )
        )
        sample_pipeline_runs.conn.commit()

        analytics = PipelineAnalytics(sample_pipeline_runs)

        # x_thread should still have 4 runs
        thread_report = analytics.health_report(content_type="x_thread", days=30)
        assert thread_report.total_runs == 4

        # x_post should have 1 run
        post_report = analytics.health_report(content_type="x_post", days=30)
        assert post_report.total_runs == 1

    def test_filter_effectiveness(self, sample_pipeline_runs):
        """Test filter effectiveness analysis."""
        analytics = PipelineAnalytics(sample_pipeline_runs)
        effectiveness = analytics.filter_effectiveness(days=30)

        # Total candidates: 3 + 4 + 5 + 2 = 14
        # repetition_rejected: 5 / 14 = 35.7%
        assert effectiveness["repetition_rejected"]["count"] == 5
        assert 35 <= effectiveness["repetition_rejected"]["percentage"] <= 36

        # stale_pattern_rejected: 3 / 14 = 21.4%
        assert effectiveness["stale_pattern_rejected"]["count"] == 3
        assert 21 <= effectiveness["stale_pattern_rejected"]["percentage"] <= 22

    def test_filter_effectiveness_no_filter_stats(self, db):
        """Test filter effectiveness with runs that have no filter_stats."""
        now = datetime.now(timezone.utc)
        db.conn.execute(
            """INSERT INTO pipeline_runs
               (batch_id, content_type, candidates_generated,
                best_candidate_index, best_score_before_refine,
                final_score, published, outcome, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                "batch-1", "x_thread", 3, 0, 7.0, 7.0, 1, "published",
                now.isoformat()
            )
        )
        db.conn.commit()

        analytics = PipelineAnalytics(db)
        effectiveness = analytics.filter_effectiveness(days=30)

        # Should return empty dict when no filter_stats
        assert effectiveness == {}

    def test_score_engagement_correlation(self, sample_content_with_engagement):
        """Test score-engagement correlation data."""
        analytics = PipelineAnalytics(sample_content_with_engagement)
        correlation = analytics.score_engagement_correlation(content_type="x_thread")

        assert len(correlation) == 2

        # Check first entry (most recent)
        assert correlation[0]["eval_score"] == 9.1
        assert correlation[0]["engagement_score"] == 28.0
        assert correlation[0]["content_type"] == "x_thread"

        # Check second entry
        assert correlation[1]["eval_score"] == 8.2
        assert correlation[1]["engagement_score"] == 12.5

    def test_score_engagement_correlation_no_engagement(self, db):
        """Test correlation with no engagement data."""
        # Insert content without engagement
        content_id = db.insert_generated_content(
            content_type="x_thread",
            source_commits=["abc"],
            source_messages=["msg"],
            content="Test",
            eval_score=8.0,
            eval_feedback="Good"
        )
        db.mark_published(content_id, "https://x.com/test/1", "tweet-1")

        analytics = PipelineAnalytics(db)
        correlation = analytics.score_engagement_correlation(content_type="x_thread")

        # Should return empty list (no engagement data)
        assert correlation == []

    def test_trend_weekly_aggregation(self, sample_pipeline_runs):
        """Test weekly trend aggregation."""
        analytics = PipelineAnalytics(sample_pipeline_runs)
        trends = analytics.trend(content_type="x_thread", weeks=8)

        # Should have data for multiple weeks
        assert len(trends) > 0

        # Each trend should have required fields
        for trend in trends:
            assert "week" in trend
            assert "runs" in trend
            assert "published" in trend
            assert "conversion_rate" in trend
            assert "avg_score" in trend
            assert "avg_engagement" in trend

    def test_trend_conversion_rate_calculation(self, sample_pipeline_runs):
        """Test conversion rate calculation in trends."""
        analytics = PipelineAnalytics(sample_pipeline_runs)
        trends = analytics.trend(content_type="x_thread", weeks=8)

        # Find a week with data
        for trend in trends:
            if trend["runs"] > 0:
                expected_rate = (trend["published"] / trend["runs"]) * 100
                assert abs(trend["conversion_rate"] - expected_rate) < 0.1

    def test_trend_no_data(self, db):
        """Test trends with no data."""
        analytics = PipelineAnalytics(db)
        trends = analytics.trend(content_type="x_thread", weeks=8)

        assert trends == []

    def test_engagement_by_score_band(self, sample_content_with_engagement):
        """Test engagement calculation by score bands."""
        analytics = PipelineAnalytics(sample_content_with_engagement)
        report = analytics.health_report(content_type="x_thread", days=30)

        # Score 8.2 (7-9 band) has engagement 12.5
        assert report.avg_engagement_by_score_band["7-9"] == 12.5

        # Score 9.1 (9-10 band) has engagement 28.0
        assert report.avg_engagement_by_score_band["9-10"] == 28.0

        # Other bands should be 0
        assert report.avg_engagement_by_score_band["0-3"] == 0.0
        assert report.avg_engagement_by_score_band["3-5"] == 0.0
        assert report.avg_engagement_by_score_band["5-7"] == 0.0

    def test_engagement_by_score_band_multiple_in_same_band(self, db):
        """Test averaging when multiple scores fall in same band."""
        now = datetime.now(timezone.utc)

        # Create pipeline runs and content items in 7-9 band
        for i, (score, engagement) in enumerate([(7.5, 10.0), (8.5, 20.0)]):
            # Insert pipeline run first
            db.conn.execute(
                """INSERT INTO pipeline_runs
                   (batch_id, content_type, candidates_generated,
                    best_candidate_index, best_score_before_refine,
                    final_score, published, outcome, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    f"batch-{i}", "x_thread", 3, 0, score, score, 1, "published",
                    (now - timedelta(days=i+1)).isoformat()
                )
            )

            content_id = db.insert_generated_content(
                content_type="x_thread",
                source_commits=[f"commit-{i}"],
                source_messages=[f"msg-{i}"],
                content=f"Content {i}",
                eval_score=score,
                eval_feedback="Good"
            )
            db.mark_published(content_id, f"https://x.com/test/{i}", f"tweet-{i}")
            db.insert_engagement(
                content_id=content_id,
                tweet_id=f"tweet-{i}",
                like_count=int(engagement),
                retweet_count=0,
                reply_count=0,
                quote_count=0,
                engagement_score=engagement
            )
        db.conn.commit()

        analytics = PipelineAnalytics(db)
        report = analytics.health_report(content_type="x_thread", days=30)

        # Average of 10.0 and 20.0 = 15.0
        assert report.avg_engagement_by_score_band["7-9"] == 15.0

    def test_health_report_handles_null_filter_stats(self, db):
        """Test that null filter_stats are handled gracefully."""
        now = datetime.now(timezone.utc)
        db.conn.execute(
            """INSERT INTO pipeline_runs
               (batch_id, content_type, candidates_generated,
                best_candidate_index, best_score_before_refine,
                final_score, published, outcome, filter_stats, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                "batch-1", "x_thread", 3, 0, 7.5, 7.5, 1, "published",
                None,  # NULL filter_stats
                now.isoformat()
            )
        )
        db.conn.commit()

        analytics = PipelineAnalytics(db)
        report = analytics.health_report(content_type="x_thread", days=30)

        # Should not crash, filter_breakdown should be empty
        assert report.filter_breakdown == {}

    def test_health_report_handles_invalid_json_filter_stats(self, db):
        """Test that invalid JSON in filter_stats is handled gracefully."""
        now = datetime.now(timezone.utc)
        db.conn.execute(
            """INSERT INTO pipeline_runs
               (batch_id, content_type, candidates_generated,
                best_candidate_index, best_score_before_refine,
                final_score, published, outcome, filter_stats, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                "batch-1", "x_thread", 3, 0, 7.5, 7.5, 1, "published",
                "not valid json",
                now.isoformat()
            )
        )
        db.conn.commit()

        analytics = PipelineAnalytics(db)
        report = analytics.health_report(content_type="x_thread", days=30)

        # Should not crash, filter_breakdown should be empty
        assert report.filter_breakdown == {}

    def test_get_pipeline_runs_helper(self, sample_pipeline_runs):
        """Test the new get_pipeline_runs helper method."""
        runs = sample_pipeline_runs.get_pipeline_runs("x_thread", since_days=30)

        assert len(runs) == 4
        # Should be ordered by created_at DESC
        assert runs[0]["batch_id"] == "batch-1"  # Most recent
        assert runs[-1]["batch_id"] == "batch-4"  # Oldest

    def test_get_pipeline_runs_filters_by_days(self, db):
        """Test that get_pipeline_runs respects the days parameter."""
        now = datetime.now(timezone.utc)

        # Add run from 2 days ago
        db.conn.execute(
            """INSERT INTO pipeline_runs
               (batch_id, content_type, candidates_generated,
                best_candidate_index, best_score_before_refine,
                final_score, published, outcome, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                "batch-recent", "x_thread", 3, 0, 7.0, 7.0, 1, "published",
                (now - timedelta(days=2)).isoformat()
            )
        )

        # Add run from 40 days ago
        db.conn.execute(
            """INSERT INTO pipeline_runs
               (batch_id, content_type, candidates_generated,
                best_candidate_index, best_score_before_refine,
                final_score, published, outcome, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                "batch-old", "x_thread", 3, 0, 7.0, 7.0, 1, "published",
                (now - timedelta(days=40)).isoformat()
            )
        )
        db.conn.commit()

        # Query for last 30 days
        runs = db.get_pipeline_runs("x_thread", since_days=30)

        # Should only get the recent run
        assert len(runs) == 1
        assert runs[0]["batch_id"] == "batch-recent"
