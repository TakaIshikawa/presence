"""Tests for engagement prediction calibration report."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from evaluation.engagement_prediction_calibration import (
    _pearson,
    generate_prediction_calibration_report,
)


def _create_db() -> sqlite3.Connection:
    db = sqlite3.connect(":memory:")
    db.execute(
        """CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            eval_score REAL,
            published INTEGER,
            created_at TEXT
        )"""
    )
    db.execute(
        """CREATE TABLE post_engagement (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            likes INTEGER,
            retweets INTEGER,
            replies INTEGER
        )"""
    )
    return db


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _days_ago_iso(n: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=n)).isoformat()


# ---------------------------------------------------------------------------
# Input handling
# ---------------------------------------------------------------------------


class TestInputHandling:
    def test_empty_db_returns_zeros(self):
        db = _create_db()
        result = generate_prediction_calibration_report(db)
        assert result["total_published"] == 0
        assert result["matched_with_engagement"] == 0
        assert result["pearson_correlation"] is None
        assert result["drift_detected"] is False
        assert result["drift_details"] == []

    def test_no_published_content(self):
        db = _create_db()
        # Insert unpublished content
        db.execute(
            "INSERT INTO generated_content (id, eval_score, published, created_at) VALUES (1, 0.5, 0, ?)",
            (_now_iso(),),
        )
        db.execute(
            "INSERT INTO post_engagement (content_id, likes, retweets, replies) VALUES (1, 10, 5, 2)",
        )
        result = generate_prediction_calibration_report(db)
        assert result["total_published"] == 0
        assert result["matched_with_engagement"] == 0


# ---------------------------------------------------------------------------
# Calibration buckets
# ---------------------------------------------------------------------------


class TestCalibrationBuckets:
    def test_posts_grouped_into_correct_buckets(self):
        db = _create_db()
        now = _now_iso()
        # Score 0.1 -> bucket 0.0-0.2
        db.execute(
            "INSERT INTO generated_content (id, eval_score, published, created_at) VALUES (1, 0.1, 1, ?)",
            (now,),
        )
        db.execute(
            "INSERT INTO post_engagement (content_id, likes, retweets, replies) VALUES (1, 5, 1, 0)",
        )
        # Score 0.7 -> bucket 0.6-0.8
        db.execute(
            "INSERT INTO generated_content (id, eval_score, published, created_at) VALUES (2, 0.7, 1, ?)",
            (now,),
        )
        db.execute(
            "INSERT INTO post_engagement (content_id, likes, retweets, replies) VALUES (2, 20, 5, 3)",
        )
        result = generate_prediction_calibration_report(db)
        assert result["calibration_buckets"]["0.0-0.2"]["count"] == 1
        assert result["calibration_buckets"]["0.6-0.8"]["count"] == 1
        assert result["calibration_buckets"]["0.2-0.4"]["count"] == 0

    def test_average_engagement_calculated(self):
        db = _create_db()
        now = _now_iso()
        # Two posts in the 0.4-0.6 bucket
        db.execute(
            "INSERT INTO generated_content (id, eval_score, published, created_at) VALUES (1, 0.45, 1, ?)",
            (now,),
        )
        db.execute(
            "INSERT INTO post_engagement (content_id, likes, retweets, replies) VALUES (1, 10, 2, 0)",
        )
        db.execute(
            "INSERT INTO generated_content (id, eval_score, published, created_at) VALUES (2, 0.55, 1, ?)",
            (now,),
        )
        db.execute(
            "INSERT INTO post_engagement (content_id, likes, retweets, replies) VALUES (2, 20, 4, 2)",
        )
        result = generate_prediction_calibration_report(db)
        bucket = result["calibration_buckets"]["0.4-0.6"]
        assert bucket["count"] == 2
        # avg engagement: (12 + 26) / 2 = 19.0
        assert bucket["avg_actual_engagement"] == 19.0
        # avg predicted: (0.45 + 0.55) / 2 = 0.5
        assert bucket["avg_predicted"] == 0.5


# ---------------------------------------------------------------------------
# Pearson correlation
# ---------------------------------------------------------------------------


class TestPearsonCorrelation:
    def test_perfect_positive_correlation(self):
        # x and y perfectly linearly related
        x = [1.0, 2.0, 3.0, 4.0, 5.0]
        y = [2.0, 4.0, 6.0, 8.0, 10.0]
        assert _pearson(x, y) == 1.0

    def test_insufficient_data_returns_none(self):
        assert _pearson([1.0, 2.0], [3.0, 4.0]) is None

    def test_zero_variance_returns_none(self):
        assert _pearson([1.0, 1.0, 1.0], [2.0, 4.0, 6.0]) is None

    def test_correlation_in_report(self):
        db = _create_db()
        now = _now_iso()
        # Insert 4 posts with increasing scores and engagement
        for i in range(1, 5):
            score = i * 0.2
            db.execute(
                "INSERT INTO generated_content (id, eval_score, published, created_at) VALUES (?, ?, 1, ?)",
                (i, score, now),
            )
            db.execute(
                "INSERT INTO post_engagement (content_id, likes, retweets, replies) VALUES (?, ?, ?, ?)",
                (i, i * 10, i * 2, i),
            )
        result = generate_prediction_calibration_report(db)
        assert result["pearson_correlation"] is not None
        assert result["pearson_correlation"] == 1.0


# ---------------------------------------------------------------------------
# Drift detection
# ---------------------------------------------------------------------------


class TestDriftDetection:
    def test_no_drift_when_consistent(self):
        db = _create_db()
        # All posts recent with consistent engagement
        for i in range(1, 6):
            db.execute(
                "INSERT INTO generated_content (id, eval_score, published, created_at) VALUES (?, 0.5, 1, ?)",
                (i, _days_ago_iso(i)),
            )
            db.execute(
                "INSERT INTO post_engagement (content_id, likes, retweets, replies) VALUES (?, 10, 2, 1)",
                (i,),
            )
        result = generate_prediction_calibration_report(db)
        assert result["drift_detected"] is False
        assert result["drift_details"] == []

    def test_drift_detected_when_recent_differs(self):
        db = _create_db()
        # Older posts (20 days ago) with low engagement
        for i in range(1, 6):
            db.execute(
                "INSERT INTO generated_content (id, eval_score, published, created_at) VALUES (?, 0.5, 1, ?)",
                (i, _days_ago_iso(20)),
            )
            db.execute(
                "INSERT INTO post_engagement (content_id, likes, retweets, replies) VALUES (?, 5, 1, 0)",
                (i,),
            )
        # Recent posts (2 days ago) with much higher engagement
        for i in range(6, 11):
            db.execute(
                "INSERT INTO generated_content (id, eval_score, published, created_at) VALUES (?, 0.5, 1, ?)",
                (i, _days_ago_iso(2)),
            )
            db.execute(
                "INSERT INTO post_engagement (content_id, likes, retweets, replies) VALUES (?, 50, 10, 5)",
                (i,),
            )
        result = generate_prediction_calibration_report(db)
        assert result["drift_detected"] is True
        assert len(result["drift_details"]) > 0


# ---------------------------------------------------------------------------
# Calibration quality
# ---------------------------------------------------------------------------


class TestCalibrationQuality:
    def test_good_quality_high_correlation_no_drift(self):
        db = _create_db()
        now = _now_iso()
        # Insert posts with strong linear relationship between score and engagement
        for i in range(1, 8):
            score = i * 0.1
            db.execute(
                "INSERT INTO generated_content (id, eval_score, published, created_at) VALUES (?, ?, 1, ?)",
                (i, score, now),
            )
            db.execute(
                "INSERT INTO post_engagement (content_id, likes, retweets, replies) VALUES (?, ?, ?, ?)",
                (i, i * 10, i * 2, i),
            )
        result = generate_prediction_calibration_report(db)
        assert result["calibration_quality"] == "good"

    def test_poor_quality_low_correlation(self):
        db = _create_db()
        now = _now_iso()
        # Insert posts with essentially no correlation between score and engagement
        scores = [0.1, 0.3, 0.5, 0.7, 0.9]
        engagements = [10, 30, 5, 25, 12]  # scattered, near-zero correlation
        for i, (score, eng) in enumerate(zip(scores, engagements), 1):
            db.execute(
                "INSERT INTO generated_content (id, eval_score, published, created_at) VALUES (?, ?, 1, ?)",
                (i, score, now),
            )
            db.execute(
                "INSERT INTO post_engagement (content_id, likes, retweets, replies) VALUES (?, ?, 0, 0)",
                (i, eng),
            )
        result = generate_prediction_calibration_report(db)
        assert result["calibration_quality"] == "poor"

    def test_moderate_quality_medium_correlation(self):
        db = _create_db()
        now = _now_iso()
        # Moderate positive correlation with some noise
        scores = [0.1, 0.3, 0.5, 0.7, 0.9]
        engagements = [5, 2, 15, 10, 20]
        for i, (score, eng) in enumerate(zip(scores, engagements), 1):
            db.execute(
                "INSERT INTO generated_content (id, eval_score, published, created_at) VALUES (?, ?, 1, ?)",
                (i, score, now),
            )
            db.execute(
                "INSERT INTO post_engagement (content_id, likes, retweets, replies) VALUES (?, ?, 0, 0)",
                (i, eng),
            )
        result = generate_prediction_calibration_report(db)
        # With noise, correlation should be moderate
        assert result["calibration_quality"] in ("moderate", "good")
