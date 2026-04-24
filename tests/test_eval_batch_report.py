"""Tests for eval batch comparison reports."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import eval_batch_report as cli
from evaluation.eval_batch_report import (
    build_eval_batch_report,
    format_json_report,
    format_text_report,
)


def _add_batch(
    db,
    *,
    label: str,
    content_type: str = "x_thread",
    generator_model: str = "claude-sonnet",
    evaluator_model: str = "claude-opus",
    threshold: float = 0.7,
    created_at: str = "2026-04-24T00:00:00+00:00",
) -> int:
    batch_id = db.create_eval_batch(
        content_type=content_type,
        generator_model=generator_model,
        evaluator_model=evaluator_model,
        threshold=threshold,
        label=label,
    )
    db.conn.execute(
        "UPDATE eval_batches SET created_at = ? WHERE id = ?",
        (created_at, batch_id),
    )
    db.conn.commit()
    return batch_id


def _add_result(
    db,
    batch_id: int,
    *,
    score: float | None,
    rejection_reason: str | None = None,
    filter_stats: dict | None = None,
    prompt_count: int = 2,
    commit_count: int = 3,
    candidate_count: int = 4,
    content_type: str = "x_thread",
    generator_model: str = "claude-sonnet",
    evaluator_model: str = "claude-opus",
    threshold: float = 0.7,
) -> int:
    return db.add_eval_result(
        batch_id=batch_id,
        content_type=content_type,
        generator_model=generator_model,
        evaluator_model=evaluator_model,
        threshold=threshold,
        source_window_hours=8,
        prompt_count=prompt_count,
        commit_count=commit_count,
        candidate_count=candidate_count,
        final_score=score,
        rejection_reason=rejection_reason,
        filter_stats=filter_stats,
        final_content="draft",
    )


def test_json_report_includes_metadata_aggregates_filter_totals_and_deltas(db):
    baseline_id = _add_batch(db, label="baseline")
    _add_result(db, baseline_id, score=7.0, filter_stats={"repetition_rejected": 1})
    _add_result(
        db,
        baseline_id,
        score=8.0,
        rejection_reason="too similar",
        filter_stats={"repetition_rejected": 2, "stale_pattern_rejected": 1},
    )

    compare_id = _add_batch(
        db,
        label="variant-a",
        generator_model="claude-haiku",
        evaluator_model="claude-opus",
    )
    _add_result(db, compare_id, score=9.0, filter_stats={"repetition_rejected": 1})
    _add_result(db, compare_id, score=8.0, filter_stats={"stale_pattern_rejected": 3})

    report = build_eval_batch_report(
        db,
        baseline_id,
        [compare_id],
        label="experiment",
        days=7,
        now=datetime(2026, 4, 25, tzinfo=timezone.utc),
    )
    payload = json.loads(format_json_report(report))

    assert payload["label"] == "experiment"
    assert payload["baseline"]["batch_id"] == baseline_id
    assert payload["baseline"]["content_type"] == "x_thread"
    assert payload["baseline"]["generator_model"] == "claude-sonnet"
    assert payload["baseline"]["evaluator_model"] == "claude-opus"
    assert payload["baseline"]["threshold"] == 0.7
    assert payload["baseline"]["result_count"] == 2
    assert payload["baseline"]["average_final_score"] == 7.5
    assert payload["baseline"]["rejection_rate"] == 0.5
    assert payload["baseline"]["filter_stats"] == {
        "repetition_rejected": 3,
        "stale_pattern_rejected": 1,
    }
    assert payload["comparisons"][0]["batch"]["average_final_score"] == 8.5
    assert payload["comparisons"][0]["average_final_score_delta"] == 1.0
    assert payload["comparisons"][0]["rejection_rate_delta"] == -0.5
    assert payload["comparisons"][0]["filter_stats_delta"] == {
        "repetition_rejected": -2,
        "stale_pattern_rejected": 2,
    }


def test_text_report_single_batch_is_stable(db):
    baseline_id = _add_batch(db, label="baseline")
    _add_result(db, baseline_id, score=7.25, filter_stats={"persona_rejected": 2})

    report = build_eval_batch_report(
        db,
        baseline_id,
        [],
        days=7,
        now=datetime(2026, 4, 25, tzinfo=timezone.utc),
    )
    output = format_text_report(report)

    assert "Eval Batch Comparison Report" in output
    assert f"Batch {baseline_id}: baseline" in output
    assert "Avg score: 7.25" in output
    assert "Rejections: 0 (0.0%)" in output
    assert "Filters: persona_rejected=2" in output
    assert "Comparisons: none requested" in output


def test_text_report_multi_batch_includes_deltas(db):
    baseline_id = _add_batch(db, label="baseline")
    _add_result(db, baseline_id, score=7.0, filter_stats={"repetition_rejected": 2})

    compare_id = _add_batch(db, label="variant-a")
    _add_result(
        db,
        compare_id,
        score=8.5,
        rejection_reason="gate",
        filter_stats={"repetition_rejected": 1},
    )

    report = build_eval_batch_report(
        db,
        baseline_id,
        [compare_id],
        days=7,
        now=datetime(2026, 4, 25, tzinfo=timezone.utc),
    )
    output = format_text_report(report)

    assert "Comparisons" in output
    assert "variant-a" in output
    assert "+1.50" in output
    assert "+100.00%" in output
    assert "Filter delta: repetition_rejected=-1" in output


def test_empty_state_for_missing_baseline_is_stable(db):
    report = build_eval_batch_report(
        db,
        999,
        [1000],
        days=7,
        now=datetime(2026, 4, 25, tzinfo=timezone.utc),
    )

    assert format_text_report(report) == (
        "Eval Batch Comparison Report\n"
        "No baseline batch found.\n"
        "Missing/skipped batch IDs: 999, 1000"
    )
    payload = json.loads(format_json_report(report))
    assert payload["baseline"] is None
    assert payload["comparisons"] == []
    assert payload["missing_batch_ids"] == [999, 1000]


def test_days_filter_skips_old_batches(db):
    old_id = _add_batch(db, label="old", created_at="2026-03-01T00:00:00+00:00")

    report = build_eval_batch_report(
        db,
        old_id,
        [],
        days=7,
        now=datetime(2026, 4, 25, tzinfo=timezone.utc),
    )

    assert report.baseline is None
    assert report.missing_batch_ids == [old_id]


def test_cli_argument_handling_prints_json(db, capsys):
    baseline_id = _add_batch(db, label="baseline")
    _add_result(db, baseline_id, score=7.0)

    compare_id = _add_batch(db, label="variant")
    _add_result(db, compare_id, score=8.0)

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("eval_batch_report.script_context", fake_script_context), patch(
        "sys.argv",
        [
            "eval_batch_report.py",
            "--baseline-batch-id",
            str(baseline_id),
            "--compare-batch-id",
            str(compare_id),
            "--label",
            "cli-report",
            "--days",
            "7",
            "--format",
            "json",
        ],
    ):
        cli.main()

    payload = json.loads(capsys.readouterr().out)
    assert payload["label"] == "cli-report"
    assert payload["days"] == 7
    assert payload["baseline_batch_id"] == baseline_id
    assert payload["compare_batch_ids"] == [compare_id]
    assert payload["comparisons"][0]["batch_id"] == compare_id
