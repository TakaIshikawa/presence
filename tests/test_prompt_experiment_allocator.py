"""Tests for prompt experiment allocation."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from evaluation.prompt_experiment_allocator import (
    allocate_prompt_experiments,
    format_prompt_experiment_allocation_json,
    format_prompt_experiment_allocation_text,
)


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "allocate_prompt_experiments.py"
)
spec = importlib.util.spec_from_file_location("allocate_prompt_experiments_script", SCRIPT_PATH)
allocate_prompt_experiments_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(allocate_prompt_experiments_script)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _set_created_at(db, table: str, row_id: int, created_at: datetime) -> None:
    db.conn.execute(
        f"UPDATE {table} SET created_at = ? WHERE id = ?",
        (created_at.isoformat(), row_id),
    )
    db.conn.commit()


def _prompt(db, prompt_type: str, text: str, created_at: datetime) -> dict:
    prompt = db.register_prompt_version(prompt_type, text)
    _set_created_at(db, "prompt_versions", prompt["id"], created_at)
    return db.get_prompt_version(prompt_type, prompt["prompt_hash"])


def _prediction(
    db,
    prompt: dict,
    *,
    created_at: datetime,
    predicted_score: float,
    actual_score: float | None,
) -> int:
    content_id = db.insert_generated_content(
        content_type=prompt["prompt_type"],
        source_commits=[],
        source_messages=[],
        content=f"content for {prompt['prompt_type']} v{prompt['version']}",
        eval_score=actual_score or predicted_score,
        eval_feedback="ok",
    )
    prediction_id = db.insert_prediction(
        content_id=content_id,
        predicted_score=predicted_score,
        prompt_type=prompt["prompt_type"],
        prompt_version=str(prompt["version"]),
        prompt_hash=prompt["prompt_hash"],
    )
    error = None if actual_score is None else actual_score - predicted_score
    db.conn.execute(
        """UPDATE engagement_predictions
           SET created_at = ?, actual_engagement_score = ?, prediction_error = ?
           WHERE id = ?""",
        (created_at.isoformat(), actual_score, error, prediction_id),
    )
    db.conn.commit()
    return prediction_id


def _pipeline_run(
    db,
    *,
    batch_id: str,
    content_type: str,
    final_score: float,
    published: bool,
    created_at: datetime,
) -> int:
    run_id = db.insert_pipeline_run(
        batch_id=batch_id,
        content_type=content_type,
        candidates_generated=3,
        best_candidate_index=0,
        best_score_before_refine=final_score,
        final_score=final_score,
        published=published,
        outcome="published" if published else "below_threshold",
    )
    _set_created_at(db, "pipeline_runs", run_id, created_at)
    return run_id


def _by_version(report):
    return {row.version: row for row in report.allocations}


def test_cold_start_allocates_exploration_and_reports_warnings(db):
    _prompt(db, "x_post", "first prompt", NOW - timedelta(days=3))
    _prompt(db, "x_post", "second prompt", NOW - timedelta(days=2))

    report = allocate_prompt_experiments(
        db,
        prompt_type="x_post",
        total_runs=5,
        explore_percent=100,
        min_runs=2,
        now=NOW,
    )

    rows = _by_version(report)
    assert sum(row.total_runs for row in rows.values()) == 5
    assert rows[1].total_runs == 3
    assert rows[2].total_runs == 2
    assert rows[1].exploration_runs == 3
    assert rows[2].exploration_runs == 2
    assert all("No performance outcomes" in row.warnings[-1] for row in rows.values())
    assert "Exploration allocation" in rows[1].reasons[0]

    text = format_prompt_experiment_allocation_text(report)
    assert "Prompt Experiment Allocation" in text
    assert "warnings:" in text


def test_mixed_performance_explores_under_sampled_then_exploits_best(db):
    low = _prompt(db, "x_post", "low prompt", NOW - timedelta(days=5))
    high = _prompt(db, "x_post", "high prompt", NOW - timedelta(days=3))
    cold = _prompt(db, "x_post", "cold prompt", NOW - timedelta(days=1))

    _prediction(db, low, created_at=NOW - timedelta(days=4), predicted_score=8.0, actual_score=4.0)
    _pipeline_run(
        db,
        batch_id="low-a",
        content_type="x_post",
        final_score=4.0,
        published=False,
        created_at=NOW - timedelta(days=4),
    )
    _pipeline_run(
        db,
        batch_id="low-b",
        content_type="x_post",
        final_score=5.0,
        published=False,
        created_at=NOW - timedelta(days=4, hours=1),
    )

    _prediction(db, high, created_at=NOW - timedelta(days=2), predicted_score=8.5, actual_score=9.0)
    _prediction(db, high, created_at=NOW - timedelta(days=2, hours=1), predicted_score=9.0, actual_score=9.0)
    _pipeline_run(
        db,
        batch_id="high-a",
        content_type="x_post",
        final_score=9.0,
        published=True,
        created_at=NOW - timedelta(days=2),
    )
    _pipeline_run(
        db,
        batch_id="high-b",
        content_type="x_post",
        final_score=8.0,
        published=True,
        created_at=NOW - timedelta(days=2, hours=1),
    )
    assert cold["version"] == 3

    report = allocate_prompt_experiments(
        db,
        prompt_type="x_post",
        total_runs=10,
        explore_percent=30,
        min_runs=3,
        now=NOW,
    )

    rows = _by_version(report)
    assert sum(row.total_runs for row in report.allocations) == 10
    assert rows[3].exploration_runs == 3
    assert rows[3].exploitation_runs == 0
    assert rows[2].exploitation_runs > rows[1].exploitation_runs
    assert rows[2].performance_score > rows[1].performance_score
    assert rows[3].reasons[0].startswith("Exploration allocation")


def test_prompt_type_filter_excludes_other_prompt_versions(db):
    x_post = _prompt(db, "x_post", "post prompt", NOW - timedelta(days=2))
    x_thread = _prompt(db, "x_thread", "thread prompt", NOW - timedelta(days=2))
    _prediction(
        db,
        x_post,
        created_at=NOW - timedelta(days=1),
        predicted_score=8.0,
        actual_score=9.0,
    )
    _prediction(
        db,
        x_thread,
        created_at=NOW - timedelta(days=1),
        predicted_score=8.0,
        actual_score=3.0,
    )

    report = allocate_prompt_experiments(
        db,
        prompt_type="x_thread",
        total_runs=4,
        explore_percent=50,
        min_runs=2,
        now=NOW,
    )

    assert [row.prompt_type for row in report.allocations] == ["x_thread"]
    assert report.allocations[0].total_runs == 4
    payload = json.loads(format_prompt_experiment_allocation_json(report))
    assert payload["filters"]["prompt_type"] == "x_thread"


@pytest.mark.parametrize(
    ("total_runs", "explore_percent"),
    [(0, 25), (1, 25), (7, 50), (13, 35), (20, 0), (20, 100)],
)
def test_allocation_totals_match_requested_budget(db, total_runs, explore_percent):
    one = _prompt(db, "x_post", "one", NOW - timedelta(days=3))
    two = _prompt(db, "x_post", "two", NOW - timedelta(days=2))
    _prediction(
        db,
        one,
        created_at=NOW - timedelta(days=1),
        predicted_score=7.0,
        actual_score=8.0,
    )
    _prediction(
        db,
        two,
        created_at=NOW - timedelta(days=1),
        predicted_score=7.0,
        actual_score=7.0,
    )

    report = allocate_prompt_experiments(
        db,
        total_runs=total_runs,
        explore_percent=explore_percent,
        min_runs=1,
        now=NOW,
    )

    assert sum(row.total_runs for row in report.allocations) == total_runs
    assert report.counts["total_runs"] == total_runs


def test_cli_outputs_json(db, capsys):
    _prompt(db, "x_post", "cli prompt", NOW - timedelta(days=1))

    with patch.object(
        allocate_prompt_experiments_script,
        "script_context",
        return_value=_script_context(db),
    ):
        exit_code = allocate_prompt_experiments_script.main(
            [
                "--prompt-type",
                "x_post",
                "--total-runs",
                "3",
                "--explore-percent",
                "100",
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["artifact_type"] == "prompt_experiment_allocation"
    assert payload["counts"]["total_runs"] == 3
