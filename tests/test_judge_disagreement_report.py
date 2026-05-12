"""Tests for judge disagreement reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import json

from evaluation.judge_disagreement_report import (
    build_judge_disagreement_report,
    format_judge_disagreement_json,
    format_judge_disagreement_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)


def _batch(db, *, label: str = "pv1", evaluator_model: str = "judge-a") -> int:
    return int(
        db.conn.execute(
            """INSERT INTO eval_batches
               (label, content_type, generator_model, evaluator_model, threshold, created_at)
               VALUES (?, 'x_post', 'gen-a', ?, 7.0, ?)""",
            (label, evaluator_model, NOW.isoformat()),
        ).lastrowid
    )


def _result(
    db,
    *,
    batch_id: int,
    content: str,
    evaluator_model: str,
    score: float,
    threshold: float = 7.0,
    reason: str | None = None,
) -> int:
    return int(
        db.conn.execute(
            """INSERT INTO eval_results
               (batch_id, content_type, generator_model, evaluator_model, threshold,
                source_window_hours, prompt_count, commit_count, candidate_count,
                final_score, rejection_reason, filter_stats, final_content, created_at)
               VALUES (?, 'x_post', 'gen-a', ?, ?, 24, 1, 1, 1, ?, ?, ?, ?, ?)""",
            (
                batch_id,
                evaluator_model,
                threshold,
                score,
                reason,
                json.dumps({"prompt_version": "pv1"}),
                content,
                NOW.isoformat(),
            ),
        ).lastrowid
    )


def test_identifies_high_score_spread_groups(db):
    batch = _batch(db)
    _result(db, batch_id=batch, content="same candidate", evaluator_model="judge-a", score=8.5)
    _result(db, batch_id=batch, content="same candidate", evaluator_model="judge-b", score=5.0)

    report = build_judge_disagreement_report(
        db,
        score_spread_threshold=2.0,
        now=NOW,
    )

    assert len(report.rows) == 1
    row = report.rows[0]
    assert row.content_type == "x_post"
    assert row.prompt_version == "pv1"
    assert row.evaluator_models == ("judge-a", "judge-b")
    assert row.score_min == 5.0
    assert row.score_max == 8.5
    assert row.has_high_score_spread is True


def test_pass_fail_conflicts_are_reported_separately(db):
    batch = _batch(db)
    _result(db, batch_id=batch, content="near threshold", evaluator_model="judge-a", score=7.1)
    _result(
        db,
        batch_id=batch,
        content="near threshold",
        evaluator_model="judge-b",
        score=6.9,
        reason="Too generic for this prompt.",
    )

    report = build_judge_disagreement_report(
        db,
        score_spread_threshold=5.0,
        now=NOW,
    )
    payload = json.loads(format_judge_disagreement_json(report))
    text = format_judge_disagreement_text(report)

    assert report.totals["pass_fail_conflict_count"] == 1
    assert report.totals["score_spread_conflict_count"] == 0
    assert payload["pass_fail_conflicts"][0]["has_pass_fail_conflict"] is True
    assert payload["pass_fail_conflicts"][0]["reason_snippets"] == [
        "Too generic for this prompt."
    ]
    assert "pass_fail=True" in text


def test_groups_are_stable_and_non_disagreements_are_ignored(db):
    batch = _batch(db)
    _result(db, batch_id=batch, content="steady", evaluator_model="judge-a", score=8.0)
    _result(db, batch_id=batch, content="steady", evaluator_model="judge-b", score=8.5)

    report = build_judge_disagreement_report(db, score_spread_threshold=2.0, now=NOW)

    assert report.rows == ()
    assert report.totals["candidate_group_count"] == 1
