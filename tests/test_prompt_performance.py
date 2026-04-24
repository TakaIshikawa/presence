"""Tests for prompt performance analytics."""

import json
from datetime import datetime, timedelta, timezone

from evaluation.prompt_performance import (
    PromptPerformanceAnalyzer,
    format_prompt_performance_json,
    format_prompt_performance_text,
)


def _set_created_at(db, table: str, row_id: int, created_at: datetime) -> None:
    db.conn.execute(
        f"UPDATE {table} SET created_at = ? WHERE id = ?",
        (created_at.isoformat(), row_id),
    )
    db.conn.commit()


def _add_eval_result(db, content_type: str, score: float, candidates: int, created_at):
    batch_id = db.create_eval_batch(
        content_type=content_type,
        generator_model="claude",
        evaluator_model="claude",
        threshold=7.0,
        label="test",
    )
    result_id = db.add_eval_result(
        batch_id=batch_id,
        content_type=content_type,
        generator_model="claude",
        evaluator_model="claude",
        threshold=7.0,
        source_window_hours=24,
        prompt_count=2,
        commit_count=3,
        candidate_count=candidates,
        final_score=score,
    )
    _set_created_at(db, "eval_results", result_id, created_at)
    return result_id


def _add_pipeline_run(db, batch_id: str, content_type: str, outcome: str, created_at):
    run_id = db.insert_pipeline_run(
        batch_id=batch_id,
        content_type=content_type,
        candidates_generated=3,
        best_candidate_index=0,
        best_score_before_refine=8.0,
        final_score=8.0,
        published=outcome == "published",
        outcome=outcome,
    )
    _set_created_at(db, "pipeline_runs", run_id, created_at)
    return run_id


def _add_prediction(db, prompt, error: float, created_at):
    content_id = db.insert_generated_content(
        content_type=prompt["prompt_type"],
        source_commits=[],
        source_messages=[],
        content=f"content {prompt['version']} {error}",
        eval_score=8.0,
        eval_feedback="ok",
    )
    prediction_id = db.insert_prediction(
        content_id=content_id,
        predicted_score=10.0,
        prompt_type=prompt["prompt_type"],
        prompt_version=str(prompt["version"]),
        prompt_hash=prompt["prompt_hash"],
    )
    db.conn.execute(
        """UPDATE engagement_predictions
           SET actual_engagement_score = ?,
               prediction_error = ?,
               created_at = ?
           WHERE id = ?""",
        (10.0 + error, error, created_at.isoformat(), prediction_id),
    )
    db.conn.commit()
    return prediction_id


def test_empty_database_returns_valid_empty_report(db):
    report = PromptPerformanceAnalyzer(db).build_report(days=30, min_runs=2)

    assert report.rows == []
    assert report.totals.prompt_versions == 0

    payload = json.loads(format_prompt_performance_json(report))
    assert payload["status"] == "empty"
    assert payload["totals"]["total_runs"] == 0

    text = format_prompt_performance_text(report)
    assert "No prompt versions matched" in text


def test_report_groups_metrics_by_prompt_version_hash(db):
    now = datetime.now(timezone.utc)
    v1 = db.register_prompt_version("x_post", "prompt one")
    v2 = db.register_prompt_version("x_post", "prompt two")
    db.conn.execute(
        "UPDATE prompt_versions SET created_at = ? WHERE id = ?",
        ((now - timedelta(days=5)).isoformat(), v1["id"]),
    )
    db.conn.execute(
        "UPDATE prompt_versions SET created_at = ? WHERE id = ?",
        ((now - timedelta(days=2)).isoformat(), v2["id"]),
    )
    db.conn.commit()

    _add_eval_result(db, "x_post", 8.0, 2, now - timedelta(days=4))
    _add_eval_result(db, "x_post", 5.0, 4, now - timedelta(days=3))
    _add_pipeline_run(db, "v1-pub", "x_post", "published", now - timedelta(days=4))
    _add_pipeline_run(
        db, "v1-low", "x_post", "below_threshold", now - timedelta(days=3)
    )
    _add_prediction(db, v1, 2.0, now - timedelta(days=4))

    _add_eval_result(db, "x_post", 9.0, 5, now - timedelta(days=1))
    _add_eval_result(db, "x_post", 8.0, 3, now - timedelta(hours=12))
    _add_pipeline_run(db, "v2-pub-a", "x_post", "published", now - timedelta(days=1))
    _add_pipeline_run(db, "v2-pub-b", "x_post", "published", now - timedelta(hours=8))
    _add_prediction(db, v2, -1.0, now - timedelta(days=1))
    _add_prediction(db, v2, 1.0, now - timedelta(hours=8))

    report = PromptPerformanceAnalyzer(db).build_report(days=30, min_runs=3)

    assert len(report.rows) == 2
    first, second = report.rows
    assert (first.prompt_type, first.version, first.prompt_hash) == (
        "x_post",
        1,
        v1["prompt_hash"],
    )
    assert first.total_runs == 5
    assert first.eval_result_count == 2
    assert first.pipeline_run_count == 2
    assert first.prediction_count == 1
    assert first.avg_eval_score == 6.5
    assert first.pass_rate == 0.5
    assert first.avg_candidate_count == 3.0
    assert first.avg_prediction_error == 2.0
    assert first.mean_absolute_prediction_error == 2.0
    assert first.published_count == 1
    assert first.publish_rate == 0.5
    assert first.outcomes == {"below_threshold": 1, "published": 1}
    assert first.insufficient_sample is False

    assert second.version == 2
    assert second.total_runs == 6
    assert second.avg_eval_score == 8.5
    assert second.pass_rate == 1.0
    assert second.avg_candidate_count == 4.0
    assert second.avg_prediction_error == 0.0
    assert second.mean_absolute_prediction_error == 1.0
    assert second.publish_rate == 1.0

    assert report.totals.prompt_versions == 2
    assert report.totals.total_runs == 11
    assert report.totals.published == 3


def test_report_marks_insufficient_samples_and_filters_prompt_type(db):
    now = datetime.now(timezone.utc)
    x_post = db.register_prompt_version("x_post", "prompt one")
    x_thread = db.register_prompt_version("x_thread", "prompt one")
    _add_prediction(db, x_post, 1.0, now - timedelta(days=1))
    _add_prediction(db, x_thread, 1.0, now - timedelta(days=1))

    report = PromptPerformanceAnalyzer(db).build_report(
        days=30,
        prompt_type="x_post",
        min_runs=2,
    )

    assert len(report.rows) == 1
    assert report.rows[0].prompt_type == "x_post"
    assert report.rows[0].total_runs == 1
    assert report.rows[0].insufficient_sample is True
    assert report.totals.insufficient_samples == 1


def test_text_output_highlights_best_and_worst_when_enough_samples(db):
    now = datetime.now(timezone.utc)
    v1 = db.register_prompt_version("x_post", "prompt one")
    v2 = db.register_prompt_version("x_post", "prompt two")
    db.conn.execute(
        "UPDATE prompt_versions SET created_at = ? WHERE id = ?",
        ((now - timedelta(days=5)).isoformat(), v1["id"]),
    )
    db.conn.execute(
        "UPDATE prompt_versions SET created_at = ? WHERE id = ?",
        ((now - timedelta(days=2)).isoformat(), v2["id"]),
    )
    db.conn.commit()

    _add_eval_result(db, "x_post", 5.0, 2, now - timedelta(days=4))
    _add_pipeline_run(db, "low-run", "x_post", "below_threshold", now - timedelta(days=4))
    _add_prediction(db, v1, 4.0, now - timedelta(days=4))

    _add_eval_result(db, "x_post", 9.0, 2, now - timedelta(days=1))
    _add_pipeline_run(db, "high-run", "x_post", "published", now - timedelta(days=1))
    _add_prediction(db, v2, 0.5, now - timedelta(days=1))

    report = PromptPerformanceAnalyzer(db).build_report(days=30, min_runs=3)
    text = format_prompt_performance_text(report)

    assert "Best:  x_post v2" in text
    assert "Worst: x_post v1" in text


def test_text_output_skips_ranking_when_samples_are_insufficient(db):
    now = datetime.now(timezone.utc)
    prompt = db.register_prompt_version("x_post", "prompt one")
    _add_prediction(db, prompt, 1.0, now - timedelta(days=1))

    report = PromptPerformanceAnalyzer(db).build_report(days=30, min_runs=2)
    text = format_prompt_performance_text(report)

    assert "No prompt versions have enough samples" in text
