"""Tests for pipeline rejection taxonomy reporting."""

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from evaluation.pipeline_rejections import (  # noqa: E402
    PipelineRejectionAnalytics,
    iter_filter_rejections,
    normalize_rejection_reason,
)
from pipeline_rejections import format_json_report, format_text_report  # noqa: E402


def _insert_run(
    db,
    *,
    batch_id: str,
    content_type: str = "x_thread",
    outcome: str = "below_threshold",
    rejection_reason: str | None = None,
    filter_stats: str | None = None,
    final_score: float | None = 0.0,
    published: int = 0,
    created_at: str | None = None,
):
    now = datetime.now(timezone.utc)
    db.conn.execute(
        """INSERT INTO pipeline_runs
           (batch_id, content_type, candidates_generated, final_score, published,
            outcome, rejection_reason, filter_stats, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            batch_id,
            content_type,
            3,
            final_score,
            published,
            outcome,
            rejection_reason,
            filter_stats,
            created_at or (now - timedelta(days=1)).isoformat(),
        ),
    )
    db.conn.commit()


def test_normalize_rejection_reason_taxonomy():
    assert normalize_rejection_reason("Score 5.5 below threshold 7.0") == "below_threshold"
    assert normalize_rejection_reason("All candidates filtered (repetitive)") == "all_filtered"
    assert normalize_rejection_reason("Dry run only", outcome=None) == "dry_run"
    assert normalize_rejection_reason("Persona guard failed: too promotional") == "filter.persona_guard"
    assert normalize_rejection_reason("Unexpected evaluator response") == "unknown"


def test_iter_filter_rejections_splits_known_filter_keys():
    stats = {
        "repetition_rejected": 2,
        "stale_pattern_rejected": 1,
        "claim_check_rejected": 0,
        "thread_validation_valid": False,
        "persona_guard": {"passed": False},
        "claim_check_final_unsupported": [{"claim": "x"}, {"claim": "y"}],
    }

    rejections = iter_filter_rejections(stats)

    assert ("filter.repetition", "repetition_rejected", 2) in rejections
    assert ("filter.stale_pattern", "stale_pattern_rejected", 1) in rejections
    assert ("filter.thread_validation", "thread_validation_valid", 1) in rejections
    assert ("filter.persona_guard", "persona_guard", 1) in rejections
    assert ("filter.claim_check_final", "claim_check_final_unsupported", 2) in rejections


def test_report_aggregates_by_category_and_content_type(db):
    _insert_run(
        db,
        batch_id="below-thread",
        content_type="x_thread",
        outcome="below_threshold",
        rejection_reason="Score 5.5 below threshold 7.0",
        filter_stats=json.dumps({"repetition_rejected": 2, "stale_pattern_rejected": 1}),
        final_score=5.5,
    )
    _insert_run(
        db,
        batch_id="below-post",
        content_type="x_post",
        outcome="below_threshold",
        rejection_reason="Score 6.0 below threshold 7.0",
        filter_stats=json.dumps({"repetition_rejected": 1}),
        final_score=6.0,
    )
    _insert_run(
        db,
        batch_id="all-filtered",
        content_type="x_thread",
        outcome="all_filtered",
        rejection_reason="All candidates filtered",
        filter_stats=json.dumps({"topic_saturated_rejected": 3}),
        final_score=None,
    )
    _insert_run(
        db,
        batch_id="published",
        content_type="x_thread",
        outcome="published",
        rejection_reason=None,
        filter_stats=json.dumps({"repetition_rejected": 9}),
        final_score=8.0,
        published=1,
    )

    report = PipelineRejectionAnalytics(db).report(days=30)
    categories = {category.category: category for category in report.categories}

    assert report.total_runs == 4
    assert report.rejected_runs == 3
    assert categories["below_threshold"].count == 2
    assert categories["below_threshold"].content_types == {"x_post": 1, "x_thread": 1}
    assert categories["filter.repetition"].count == 3
    assert categories["filter.repetition"].content_types == {"x_post": 1, "x_thread": 2}
    assert categories["filter.topic_saturation"].count == 3
    assert categories["all_filtered"].raw_examples == ["All candidates filtered"]
    assert "published" not in categories


def test_report_content_type_filter_and_min_count(db):
    _insert_run(
        db,
        batch_id="thread",
        content_type="x_thread",
        rejection_reason="Score 5.5 below threshold 7.0",
        filter_stats=json.dumps({"repetition_rejected": 2}),
    )
    _insert_run(
        db,
        batch_id="post",
        content_type="x_post",
        rejection_reason="All candidates filtered",
        outcome="all_filtered",
        filter_stats=json.dumps({"stale_pattern_rejected": 1}),
    )

    report = PipelineRejectionAnalytics(db).report(
        days=30,
        content_type="x_thread",
        min_count=2,
    )

    assert report.total_runs == 1
    assert report.rejected_runs == 1
    assert [category.category for category in report.categories] == ["filter.repetition"]


def test_malformed_filter_stats_are_tolerated_and_reported(db):
    _insert_run(
        db,
        batch_id="malformed",
        content_type="x_thread",
        outcome="below_threshold",
        rejection_reason="Score 5.5 below threshold 7.0",
        filter_stats="{not json",
    )

    report = PipelineRejectionAnalytics(db).report(days=30)
    categories = {category.category: category for category in report.categories}

    assert categories["below_threshold"].count == 1
    assert len(report.parse_warnings) == 1
    assert report.parse_warnings[0].batch_id == "malformed"
    assert "Malformed filter_stats JSON" in report.parse_warnings[0].message


def test_json_output_includes_raw_examples_and_parse_warnings(db):
    _insert_run(
        db,
        batch_id="malformed",
        content_type="x_thread",
        outcome="below_threshold",
        rejection_reason="Score 5.5 below threshold 7.0",
        filter_stats="{not json",
    )
    report = PipelineRejectionAnalytics(db).report(days=30)

    payload = json.loads(format_json_report(report))

    assert payload["categories"][0]["raw_examples"] == ["Score 5.5 below threshold 7.0"]
    assert payload["parse_warnings"][0]["batch_id"] == "malformed"
    assert payload["period_end"]


def test_text_output_includes_top_causes_and_warnings(db):
    _insert_run(
        db,
        batch_id="malformed",
        content_type="x_thread",
        outcome="below_threshold",
        rejection_reason="Score 5.5 below threshold 7.0",
        filter_stats="{not json",
    )
    report = PipelineRejectionAnalytics(db).report(days=30)

    output = format_text_report(report)

    assert "Pipeline Rejections (last 30 days)" in output
    assert "below_threshold" in output
    assert "Score 5.5 below threshold 7.0" in output
    assert "Parse Warnings:" in output
