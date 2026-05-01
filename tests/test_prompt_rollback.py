"""Tests for prompt rollback recommendation reporting."""

from __future__ import annotations

import json
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from prompt_rollback import main  # noqa: E402
from evaluation.prompt_rollback import (  # noqa: E402
    build_prompt_rollback_report,
    format_prompt_rollback_json,
    format_prompt_rollback_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


def _set_created_at(db, table: str, row_id: int, created_at: datetime) -> None:
    db.conn.execute(
        f"UPDATE {table} SET created_at = ? WHERE id = ?",
        (created_at.isoformat(), row_id),
    )
    db.conn.commit()


def _set_prompt_created_at(db, prompt: dict, created_at: datetime) -> None:
    _set_created_at(db, "prompt_versions", prompt["id"], created_at)


def _add_eval_result(db, content_type: str, score: float, created_at: datetime) -> int:
    batch_id = db.create_eval_batch(
        content_type=content_type,
        generator_model="claude",
        evaluator_model="claude",
        threshold=7.0,
        label="rollback-test",
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
        candidate_count=2,
        final_score=score,
    )
    _set_created_at(db, "eval_results", result_id, created_at)
    return result_id


def _add_pipeline_run(db, batch_id: str, content_type: str, published: bool, created_at: datetime) -> int:
    run_id = db.insert_pipeline_run(
        batch_id=batch_id,
        content_type=content_type,
        candidates_generated=2,
        best_candidate_index=0,
        best_score_before_refine=8.0,
        final_score=8.0,
        published=published,
        outcome="published" if published else "below_threshold",
    )
    _set_created_at(db, "pipeline_runs", run_id, created_at)
    return run_id


def _add_prediction(
    db,
    prompt: dict,
    *,
    predicted: float,
    actual: float,
    created_at: datetime,
) -> int:
    content_id = db.insert_generated_content(
        content_type=prompt["prompt_type"],
        source_commits=[],
        source_messages=[],
        content=f"content v{prompt['version']} {actual}",
        eval_score=actual,
        eval_feedback="ok",
    )
    _set_created_at(db, "generated_content", content_id, created_at)
    prediction_id = db.insert_prediction(
        content_id=content_id,
        predicted_score=predicted,
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
        (actual, actual - predicted, created_at.isoformat(), prediction_id),
    )
    db.conn.commit()
    return prediction_id


def _add_signal_set(
    db,
    prompt: dict,
    *,
    eval_score: float,
    published: bool,
    predicted: float,
    actual: float,
    created_at: datetime,
) -> None:
    _add_eval_result(db, prompt["prompt_type"], eval_score, created_at)
    _add_pipeline_run(
        db,
        f"{prompt['prompt_type']}-v{prompt['version']}-{created_at.timestamp()}",
        prompt["prompt_type"],
        published,
        created_at,
    )
    _add_prediction(db, prompt, predicted=predicted, actual=actual, created_at=created_at)


def test_recommends_rollback_when_current_underperforms_previous_baseline(db):
    v1 = db.register_prompt_version("x_post", "strong prompt")
    v2 = db.register_prompt_version("x_post", "weak prompt")
    _set_prompt_created_at(db, v1, NOW - timedelta(days=10))
    _set_prompt_created_at(db, v2, NOW - timedelta(days=4))
    _add_signal_set(
        db,
        v1,
        eval_score=8.5,
        published=True,
        predicted=9.0,
        actual=10.0,
        created_at=NOW - timedelta(days=8),
    )
    _add_signal_set(
        db,
        v2,
        eval_score=5.0,
        published=False,
        predicted=9.0,
        actual=4.0,
        created_at=NOW - timedelta(days=2),
    )

    report = build_prompt_rollback_report(db, days=30, min_samples=3, now=NOW)
    recommendation = report.recommendations[0]

    assert recommendation.decision == "rollback"
    assert recommendation.current.version == 2
    assert recommendation.candidate_previous.version == 1
    assert recommendation.metric_deltas["avg_eval_score"] < -1.0
    assert recommendation.metric_deltas["publish_rate"] < -0.2
    assert "underperforms" in recommendation.reasons[0]
    assert "Candidate previous: v1" in format_prompt_rollback_text(report)


def test_minimum_sample_threshold_prevents_noisy_rollback(db):
    v1 = db.register_prompt_version("x_post", "strong prompt")
    v2 = db.register_prompt_version("x_post", "weak prompt")
    _set_prompt_created_at(db, v1, NOW - timedelta(days=10))
    _set_prompt_created_at(db, v2, NOW - timedelta(days=4))
    _add_signal_set(
        db,
        v1,
        eval_score=9.0,
        published=True,
        predicted=9.0,
        actual=9.0,
        created_at=NOW - timedelta(days=8),
    )
    _add_eval_result(db, "x_post", 3.0, NOW - timedelta(days=2))

    report = build_prompt_rollback_report(db, days=30, min_samples=4, now=NOW)
    recommendation = report.recommendations[0]

    assert recommendation.current.version == 2
    assert recommendation.decision == "watch"
    assert "Sample threshold prevents" in recommendation.reasons[0]
    assert any("Current version has" in caveat for caveat in recommendation.confidence_caveats)


def test_uses_latest_eligible_previous_version_as_candidate(db):
    v1 = db.register_prompt_version("x_thread", "baseline prompt")
    v2 = db.register_prompt_version("x_thread", "low sample prompt")
    v3 = db.register_prompt_version("x_thread", "current prompt")
    _set_prompt_created_at(db, v1, NOW - timedelta(days=12))
    _set_prompt_created_at(db, v2, NOW - timedelta(days=8))
    _set_prompt_created_at(db, v3, NOW - timedelta(days=4))
    _add_signal_set(
        db,
        v1,
        eval_score=8.0,
        published=True,
        predicted=8.0,
        actual=8.5,
        created_at=NOW - timedelta(days=10),
    )
    _add_eval_result(db, "x_thread", 7.0, NOW - timedelta(days=7))
    _add_signal_set(
        db,
        v3,
        eval_score=7.8,
        published=True,
        predicted=8.0,
        actual=8.0,
        created_at=NOW - timedelta(days=2),
    )

    report = build_prompt_rollback_report(
        db,
        prompt_type="x_thread",
        days=30,
        min_samples=3,
        now=NOW,
    )

    recommendation = report.recommendations[0]
    assert recommendation.current.version == 3
    assert recommendation.candidate_previous.version == 1
    assert recommendation.decision == "keep"


def test_missing_optional_tables_return_empty_state_without_errors():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE prompt_versions (
            id INTEGER PRIMARY KEY,
            prompt_type TEXT NOT NULL,
            version INTEGER NOT NULL,
            prompt_hash TEXT NOT NULL,
            prompt_text TEXT NOT NULL,
            created_at TEXT
        );
        INSERT INTO prompt_versions
            (id, prompt_type, version, prompt_hash, prompt_text, created_at)
        VALUES
            (1, 'x_post', 1, 'hash-1', 'prompt', '2026-04-20T12:00:00+00:00');
        """
    )

    report = build_prompt_rollback_report(conn, days=30, min_samples=2, now=NOW)
    payload = json.loads(format_prompt_rollback_json(report))

    assert payload["status"] == "ok"
    assert report.recommendations[0].decision == "watch"
    assert "pipeline_runs" in report.missing_optional_tables
    assert "engagement_predictions" in report.missing_optional_tables
    assert "No eval_results rows" in " ".join(report.recommendations[0].confidence_caveats)


def test_cli_supports_prompt_type_lookback_min_samples_and_json(db, capsys):
    prompt = db.register_prompt_version("x_post", "cli prompt")
    _set_prompt_created_at(db, prompt, NOW - timedelta(days=3))

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("prompt_rollback.script_context", fake_script_context):
        exit_code = main(
            [
                "--prompt-type",
                "x_post",
                "--lookback-days",
                "14",
                "--min-samples",
                "2",
                "--json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["prompt_type"] == "x_post"
    assert payload["days"] == 14
    assert payload["min_samples"] == 2
