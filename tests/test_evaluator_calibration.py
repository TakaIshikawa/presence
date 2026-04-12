"""Tests for engagement-aware evaluator calibration (Feature 1)."""

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from synthesis.evaluator_v2 import CrossModelEvaluator


class TestBuildCalibrationSection:
    """Tests for _build_calibration_section with engagement_stats."""

    def test_empty_when_no_data(self):
        result = CrossModelEvaluator._build_calibration_section()
        assert result == ""

    def test_empty_when_all_none(self):
        result = CrossModelEvaluator._build_calibration_section(
            resonated=None, low_resonance=None, engagement_stats=None
        )
        assert result == ""

    def test_stats_block_included_when_sufficient_data(self):
        stats = {
            "total_classified": 50,
            "resonated_count": 5,
            "low_resonance_count": 45,
            "avg_eval_score_resonated": 7.8,
            "avg_eval_score_low_resonance": 7.4,
            "scored_7plus_total": 40,
            "scored_7plus_zero_engagement": 36,
            "scored_7plus_zero_pct": 90.0,
        }
        result = CrossModelEvaluator._build_calibration_section(
            engagement_stats=stats
        )
        assert "EVALUATOR ACCURACY REPORT" in result
        assert "40 posts at 7.0+" in result
        assert "36 (90.0%) got ZERO engagement" in result
        assert "resonated: 7.8" in result
        assert "zero-engagement posts: 7.4" in result
        assert "RECALIBRATE" in result

    def test_stats_block_skipped_when_insufficient_data(self):
        stats = {
            "total_classified": 5,  # Below threshold of 10
            "scored_7plus_total": 0,
        }
        result = CrossModelEvaluator._build_calibration_section(
            engagement_stats=stats
        )
        assert "EVALUATOR ACCURACY REPORT" not in result

    def test_example_calibration_still_works(self):
        resonated = [{"content": "Great post about AI agents"}]
        low_resonance = [{"content": "Built some features today"}]
        result = CrossModelEvaluator._build_calibration_section(
            resonated=resonated, low_resonance=low_resonance
        )
        assert "ENGAGEMENT REALITY CHECK" in result
        assert "Great post about AI agents" in result
        assert "Built some features today" in result

    def test_stats_and_examples_combined(self):
        stats = {
            "total_classified": 20,
            "scored_7plus_total": 15,
            "scored_7plus_zero_engagement": 12,
            "scored_7plus_zero_pct": 80.0,
            "avg_eval_score_resonated": 7.5,
            "avg_eval_score_low_resonance": 7.2,
        }
        resonated = [{"content": "Engaging post"}]
        low_resonance = [{"content": "Boring post"}]
        result = CrossModelEvaluator._build_calibration_section(
            resonated=resonated,
            low_resonance=low_resonance,
            engagement_stats=stats,
        )
        # Both sections should be present
        assert "EVALUATOR ACCURACY REPORT" in result
        assert "ENGAGEMENT REALITY CHECK" in result

    def test_stats_with_no_7plus_posts(self):
        stats = {
            "total_classified": 15,
            "scored_7plus_total": 0,
            "scored_7plus_zero_engagement": 0,
            "scored_7plus_zero_pct": 0.0,
            "avg_eval_score_resonated": 6.5,
            "avg_eval_score_low_resonance": 6.0,
        }
        result = CrossModelEvaluator._build_calibration_section(
            engagement_stats=stats
        )
        # Should still include accuracy report with avg scores
        assert "EVALUATOR ACCURACY REPORT" in result
        assert "resonated: 6.5" in result

    def test_stats_with_missing_avg_scores(self):
        stats = {
            "total_classified": 15,
            "scored_7plus_total": 10,
            "scored_7plus_zero_engagement": 8,
            "scored_7plus_zero_pct": 80.0,
            "avg_eval_score_resonated": None,
            "avg_eval_score_low_resonance": None,
        }
        result = CrossModelEvaluator._build_calibration_section(
            engagement_stats=stats
        )
        assert "EVALUATOR ACCURACY REPORT" in result
        assert "10 posts at 7.0+" in result
        # Should not include avg score line when both are None
        assert "Average score" not in result


class TestGetEngagementCalibrationStats:
    """Tests for db.get_engagement_calibration_stats()."""

    def test_empty_db(self, db):
        stats = db.get_engagement_calibration_stats()
        assert stats["total_classified"] == 0
        assert stats["resonated_count"] == 0
        assert stats["low_resonance_count"] == 0
        assert stats["scored_7plus_total"] == 0

    def test_with_classified_posts(self, db):
        # Insert some published posts with auto_quality
        for i in range(5):
            db.conn.execute(
                """INSERT INTO generated_content
                   (content_type, content, eval_score, published, auto_quality)
                   VALUES (?, ?, ?, 1, ?)""",
                ("x_post", f"Post {i}", 7.5, "resonated")
            )
        for i in range(10):
            db.conn.execute(
                """INSERT INTO generated_content
                   (content_type, content, eval_score, published, auto_quality)
                   VALUES (?, ?, ?, 1, ?)""",
                ("x_post", f"Low post {i}", 7.2, "low_resonance")
            )
        db.conn.commit()

        stats = db.get_engagement_calibration_stats()
        assert stats["total_classified"] == 15
        assert stats["resonated_count"] == 5
        assert stats["low_resonance_count"] == 10
        assert stats["avg_eval_score_resonated"] == 7.5
        assert stats["avg_eval_score_low_resonance"] == 7.2
        assert stats["scored_7plus_total"] == 15
        assert stats["scored_7plus_zero_engagement"] == 10
        assert stats["scored_7plus_zero_pct"] == pytest.approx(66.7, abs=0.1)

    def test_filters_by_content_type(self, db):
        db.conn.execute(
            """INSERT INTO generated_content
               (content_type, content, eval_score, published, auto_quality)
               VALUES ('x_post', 'post', 8.0, 1, 'resonated')"""
        )
        db.conn.execute(
            """INSERT INTO generated_content
               (content_type, content, eval_score, published, auto_quality)
               VALUES ('x_thread', 'thread', 8.0, 1, 'resonated')"""
        )
        db.conn.commit()

        stats = db.get_engagement_calibration_stats("x_post")
        assert stats["resonated_count"] == 1

    def test_excludes_unclassified(self, db):
        db.conn.execute(
            """INSERT INTO generated_content
               (content_type, content, eval_score, published, auto_quality)
               VALUES ('x_post', 'classified', 7.0, 1, 'resonated')"""
        )
        db.conn.execute(
            """INSERT INTO generated_content
               (content_type, content, eval_score, published)
               VALUES ('x_post', 'unclassified', 8.0, 1)"""
        )
        db.conn.commit()

        stats = db.get_engagement_calibration_stats()
        assert stats["total_classified"] == 1
        assert stats["scored_7plus_total"] == 1


class TestMetaMethods:
    """Tests for db.get_meta() and db.set_meta()."""

    def test_get_missing_key(self, db):
        assert db.get_meta("nonexistent") is None

    def test_set_and_get(self, db):
        db.set_meta("test_key", "test_value")
        assert db.get_meta("test_key") == "test_value"

    def test_upsert(self, db):
        db.set_meta("key", "value1")
        db.set_meta("key", "value2")
        assert db.get_meta("key") == "value2"

    def test_json_value(self, db):
        import json
        data = {"rules": ["rule1", "rule2"], "count": 5}
        db.set_meta("analysis", json.dumps(data))
        result = json.loads(db.get_meta("analysis"))
        assert result["rules"] == ["rule1", "rule2"]


class TestGetAllClassifiedPosts:
    """Tests for db.get_all_classified_posts()."""

    def test_empty_db(self, db):
        result = db.get_all_classified_posts()
        assert result == {"resonated": [], "low_resonance": []}

    def test_returns_grouped_posts(self, db):
        db.conn.execute(
            """INSERT INTO generated_content
               (content_type, content, eval_score, published, auto_quality)
               VALUES ('x_post', 'good post', 8.0, 1, 'resonated')"""
        )
        db.conn.execute(
            """INSERT INTO generated_content
               (content_type, content, eval_score, published, auto_quality)
               VALUES ('x_post', 'bad post', 7.0, 1, 'low_resonance')"""
        )
        db.conn.commit()

        result = db.get_all_classified_posts()
        assert len(result["resonated"]) == 1
        assert len(result["low_resonance"]) == 1
        assert result["resonated"][0]["content"] == "good post"
        assert result["low_resonance"][0]["content"] == "bad post"

    def test_includes_engagement_score(self, db):
        db.conn.execute(
            """INSERT INTO generated_content
               (id, content_type, content, eval_score, published, auto_quality, tweet_id)
               VALUES (1, 'x_post', 'good post', 8.0, 1, 'resonated', 'tw-1')"""
        )
        db.conn.execute(
            """INSERT INTO post_engagement
               (content_id, tweet_id, like_count, retweet_count, reply_count,
                quote_count, engagement_score, fetched_at)
               VALUES (1, 'tw-1', 3, 0, 1, 0, 7.0, '2026-04-10T00:00:00')"""
        )
        db.conn.commit()

        result = db.get_all_classified_posts()
        assert result["resonated"][0]["engagement_score"] == 7.0
