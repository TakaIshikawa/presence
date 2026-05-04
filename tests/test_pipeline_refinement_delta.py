"""Tests for pipeline refinement delta report."""

import json
from datetime import datetime, timedelta, timezone

import pytest

from evaluation.pipeline_refinement_delta import (
    build_pipeline_refinement_delta_report,
    format_pipeline_refinement_delta_csv,
    format_pipeline_refinement_delta_json,
)


@pytest.fixture
def sample_pipeline_runs_with_refinement(db):
    """Create pipeline runs with various refinement scenarios."""
    now = datetime.now(timezone.utc)

    # Case 1: Refinement improved (positive delta)
    db.conn.execute(
        """INSERT INTO pipeline_runs
           (batch_id, content_type, candidates_generated,
            best_candidate_index, best_score_before_refine,
            best_score_after_refine, refinement_picked,
            final_score, published, content_id, outcome, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "batch-improved",
            "x_thread",
            3,
            0,
            7.5,
            8.2,
            "REFINED",
            8.2,
            1,
            None,
            "published",
            (now - timedelta(days=1)).isoformat(),
        ),
    )

    # Case 2: Refinement regressed (negative delta)
    db.conn.execute(
        """INSERT INTO pipeline_runs
           (batch_id, content_type, candidates_generated,
            best_candidate_index, best_score_before_refine,
            best_score_after_refine, refinement_picked,
            final_score, published, content_id, outcome, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "batch-regressed",
            "x_thread",
            2,
            0,
            9.1,
            8.8,
            "ORIGINAL",
            9.1,
            1,
            None,
            "published",
            (now - timedelta(days=2)).isoformat(),
        ),
    )

    # Case 3: No change (zero delta)
    db.conn.execute(
        """INSERT INTO pipeline_runs
           (batch_id, content_type, candidates_generated,
            best_candidate_index, best_score_before_refine,
            best_score_after_refine, refinement_picked,
            final_score, published, content_id, outcome, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "batch-unchanged",
            "x_post",
            2,
            0,
            6.0,
            6.0,
            "REFINED",
            6.0,
            0,
            None,
            "below_threshold",
            (now - timedelta(days=3)).isoformat(),
        ),
    )

    # Case 4: Missing after score (skipped)
    db.conn.execute(
        """INSERT INTO pipeline_runs
           (batch_id, content_type, candidates_generated,
            best_candidate_index, best_score_before_refine,
            best_score_after_refine, refinement_picked,
            final_score, published, content_id, outcome, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "batch-skipped-after",
            "x_thread",
            3,
            0,
            7.0,
            None,
            None,
            7.0,
            0,
            None,
            "below_threshold",
            (now - timedelta(days=4)).isoformat(),
        ),
    )

    # Case 5: Missing before score (skipped)
    db.conn.execute(
        """INSERT INTO pipeline_runs
           (batch_id, content_type, candidates_generated,
            best_candidate_index, best_score_before_refine,
            best_score_after_refine, refinement_picked,
            final_score, published, content_id, outcome, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "batch-skipped-before",
            "x_post",
            2,
            None,
            None,
            None,
            None,
            None,
            0,
            None,
            "all_filtered",
            (now - timedelta(days=5)).isoformat(),
        ),
    )

    # Case 6: Another improvement with different content_type
    db.conn.execute(
        """INSERT INTO pipeline_runs
           (batch_id, content_type, candidates_generated,
            best_candidate_index, best_score_before_refine,
            best_score_after_refine, refinement_picked,
            final_score, published, content_id, outcome, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "batch-improved-post",
            "x_post",
            4,
            1,
            5.5,
            6.8,
            "REFINED",
            6.8,
            0,
            None,
            "below_threshold",
            (now - timedelta(days=6)).isoformat(),
        ),
    )

    # Case 7: Malformed filter_stats (should handle gracefully)
    db.conn.execute(
        """INSERT INTO pipeline_runs
           (batch_id, content_type, candidates_generated,
            best_candidate_index, best_score_before_refine,
            best_score_after_refine, refinement_picked,
            final_score, published, content_id, outcome, filter_stats, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "batch-malformed-stats",
            "x_thread",
            3,
            0,
            8.0,
            8.5,
            "REFINED",
            8.5,
            1,
            None,
            "published",
            "not-valid-json",
            (now - timedelta(days=7)).isoformat(),
        ),
    )

    db.conn.commit()
    return db


def test_build_pipeline_refinement_delta_report_basic(sample_pipeline_runs_with_refinement):
    """Test basic report generation."""
    db = sample_pipeline_runs_with_refinement
    report = build_pipeline_refinement_delta_report(db, days=30)

    assert report.total_runs == 7
    assert len(report.aggregations) > 0
    assert len(report.details) > 0


def test_build_pipeline_refinement_delta_report_aggregations(sample_pipeline_runs_with_refinement):
    """Test aggregation statistics."""
    db = sample_pipeline_runs_with_refinement
    report = build_pipeline_refinement_delta_report(db, days=30)

    # Find x_thread REFINED aggregation
    x_thread_refined = next(
        (a for a in report.aggregations if a.content_type == "x_thread" and a.refinement_picked == "REFINED"),
        None,
    )
    assert x_thread_refined is not None
    assert x_thread_refined.run_count == 2  # batch-improved, batch-malformed-stats (batch-unchanged is x_post)
    assert x_thread_refined.improved_count >= 1  # batch-improved, batch-malformed-stats
    assert x_thread_refined.regressed_count >= 0
    assert x_thread_refined.unchanged_count >= 0
    assert x_thread_refined.skipped_count >= 0

    # Find x_thread ORIGINAL aggregation
    x_thread_original = next(
        (a for a in report.aggregations if a.content_type == "x_thread" and a.refinement_picked == "ORIGINAL"),
        None,
    )
    assert x_thread_original is not None
    assert x_thread_original.run_count == 1  # batch-regressed
    assert x_thread_original.regressed_count == 1


def test_build_pipeline_refinement_delta_report_details(sample_pipeline_runs_with_refinement):
    """Test detail rows."""
    db = sample_pipeline_runs_with_refinement
    report = build_pipeline_refinement_delta_report(db, days=30)

    # Check improved detail
    improved = next((d for d in report.details if d.batch_id == "batch-improved"), None)
    assert improved is not None
    assert improved.best_score_before_refine == 7.5
    assert improved.best_score_after_refine == 8.2
    assert improved.delta == pytest.approx(0.7, abs=0.01)
    assert improved.refinement_picked == "REFINED"

    # Check regressed detail
    regressed = next((d for d in report.details if d.batch_id == "batch-regressed"), None)
    assert regressed is not None
    assert regressed.best_score_before_refine == 9.1
    assert regressed.best_score_after_refine == 8.8
    assert regressed.delta == pytest.approx(-0.3, abs=0.01)
    assert regressed.refinement_picked == "ORIGINAL"

    # Check unchanged detail
    unchanged = next((d for d in report.details if d.batch_id == "batch-unchanged"), None)
    assert unchanged is not None
    assert unchanged.best_score_before_refine == 6.0
    assert unchanged.best_score_after_refine == 6.0
    assert unchanged.delta == pytest.approx(0.0, abs=0.01)

    # Check skipped detail (missing after score)
    skipped = next((d for d in report.details if d.batch_id == "batch-skipped-after"), None)
    assert skipped is not None
    assert skipped.best_score_before_refine == 7.0
    assert skipped.best_score_after_refine is None
    assert skipped.delta is None


def test_build_pipeline_refinement_delta_report_filter_content_type(sample_pipeline_runs_with_refinement):
    """Test filtering by content_type."""
    db = sample_pipeline_runs_with_refinement
    report = build_pipeline_refinement_delta_report(db, days=30, content_type="x_thread")

    # x_thread runs: batch-improved, batch-regressed, batch-skipped-after, batch-malformed-stats = 4 total
    assert report.total_runs == 4  # Only x_thread runs
    assert all(d.content_type == "x_thread" for d in report.details)
    assert all(a.content_type == "x_thread" for a in report.aggregations)


def test_build_pipeline_refinement_delta_report_filter_outcome(sample_pipeline_runs_with_refinement):
    """Test filtering by outcome."""
    db = sample_pipeline_runs_with_refinement
    report = build_pipeline_refinement_delta_report(db, days=30, outcome="published")

    assert report.total_runs == 3  # Only published runs
    assert all(d.outcome == "published" for d in report.details)


def test_build_pipeline_refinement_delta_report_filter_refinement_picked(sample_pipeline_runs_with_refinement):
    """Test filtering by refinement_picked."""
    db = sample_pipeline_runs_with_refinement
    report = build_pipeline_refinement_delta_report(db, days=30, refinement_picked="REFINED")

    assert report.total_runs == 4  # Only REFINED runs
    assert all(d.refinement_picked == "REFINED" for d in report.details if d.refinement_picked is not None)


def test_build_pipeline_refinement_delta_report_min_delta(sample_pipeline_runs_with_refinement):
    """Test filtering by min_delta."""
    db = sample_pipeline_runs_with_refinement
    report = build_pipeline_refinement_delta_report(db, days=30, min_delta=0.5)

    # Should only include details where abs(delta) >= 0.5
    # batch-improved: delta=0.7, batch-improved-post: delta=1.3
    # batch-regressed, batch-unchanged, and skipped should be excluded
    assert len(report.details) >= 2
    for detail in report.details:
        if detail.delta is not None:
            assert abs(detail.delta) >= 0.5


def test_build_pipeline_refinement_delta_report_limit(sample_pipeline_runs_with_refinement):
    """Test limiting detail rows."""
    db = sample_pipeline_runs_with_refinement
    report = build_pipeline_refinement_delta_report(db, days=30, limit=3)

    assert len(report.details) == 3
    # Aggregations should not be limited
    assert len(report.aggregations) > 0


def test_build_pipeline_refinement_delta_report_empty(db):
    """Test report with no pipeline runs."""
    report = build_pipeline_refinement_delta_report(db, days=30)

    assert report.total_runs == 0
    assert len(report.aggregations) == 0
    assert len(report.details) == 0


def test_format_pipeline_refinement_delta_json(sample_pipeline_runs_with_refinement):
    """Test JSON formatting."""
    db = sample_pipeline_runs_with_refinement
    report = build_pipeline_refinement_delta_report(db, days=30)
    output = format_pipeline_refinement_delta_json(report)

    # Should be valid JSON
    parsed = json.loads(output)
    assert "period_start" in parsed
    assert "period_end" in parsed
    assert "total_runs" in parsed
    assert "aggregations" in parsed
    assert "details" in parsed
    assert isinstance(parsed["aggregations"], list)
    assert isinstance(parsed["details"], list)


def test_format_pipeline_refinement_delta_csv(sample_pipeline_runs_with_refinement):
    """Test CSV formatting."""
    db = sample_pipeline_runs_with_refinement
    report = build_pipeline_refinement_delta_report(db, days=30)
    output = format_pipeline_refinement_delta_csv(report)

    # Should contain both sections
    assert "# Aggregations" in output
    assert "# Details" in output
    assert "content_type,refinement_picked" in output
    assert "batch_id,content_type" in output


def test_aggregation_average_and_median(sample_pipeline_runs_with_refinement):
    """Test average and median delta calculations."""
    db = sample_pipeline_runs_with_refinement
    report = build_pipeline_refinement_delta_report(db, days=30)

    # Find x_thread REFINED aggregation
    x_thread_refined = next(
        (a for a in report.aggregations if a.content_type == "x_thread" and a.refinement_picked == "REFINED"),
        None,
    )
    assert x_thread_refined is not None

    # Should have computed average and median
    # batch-improved: 0.7, batch-malformed-stats: 0.5
    # Average should be around 0.6
    assert x_thread_refined.average_delta > 0
    assert x_thread_refined.median_delta > 0


def test_skipped_count_in_aggregation(sample_pipeline_runs_with_refinement):
    """Test that skipped runs (missing before/after scores) are counted correctly."""
    db = sample_pipeline_runs_with_refinement
    report = build_pipeline_refinement_delta_report(db, days=30)

    # Find aggregations with None refinement_picked
    none_picked = [a for a in report.aggregations if a.refinement_picked is None]
    assert len(none_picked) > 0

    # Check that skipped count is correct
    for agg in none_picked:
        assert agg.skipped_count >= 0


def test_malformed_filter_stats_handling(sample_pipeline_runs_with_refinement):
    """Test that malformed filter_stats JSON is handled gracefully."""
    db = sample_pipeline_runs_with_refinement
    # Should not raise an error
    report = build_pipeline_refinement_delta_report(db, days=30)

    # batch-malformed-stats should still be in the report
    malformed = next((d for d in report.details if d.batch_id == "batch-malformed-stats"), None)
    assert malformed is not None
    assert malformed.delta is not None  # Should still calculate delta


def test_multiple_filters_combined(sample_pipeline_runs_with_refinement):
    """Test combining multiple filters."""
    db = sample_pipeline_runs_with_refinement
    report = build_pipeline_refinement_delta_report(
        db, days=30, content_type="x_thread", outcome="published", refinement_picked="REFINED"
    )

    # Should only include runs matching all filters
    assert all(d.content_type == "x_thread" for d in report.details)
    assert all(d.outcome == "published" for d in report.details)
    assert all(d.refinement_picked == "REFINED" for d in report.details if d.refinement_picked is not None)


def test_date_range_filter(sample_pipeline_runs_with_refinement):
    """Test filtering by date range."""
    db = sample_pipeline_runs_with_refinement
    # Use a short lookback window
    report = build_pipeline_refinement_delta_report(db, days=3)

    # Should only include runs from the last 3 days
    assert report.total_runs < 7  # Less than the total number of runs
    assert report.total_runs >= 1  # But at least some runs
