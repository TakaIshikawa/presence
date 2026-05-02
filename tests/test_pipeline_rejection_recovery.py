"""Tests for actionable pipeline rejection recovery planning."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from evaluation.pipeline_rejection_recovery import (  # noqa: E402
    build_pipeline_rejection_recovery_report,
    format_pipeline_rejection_recovery_json,
    format_pipeline_rejection_recovery_text,
)
from pipeline_rejection_recovery import main, parse_args  # noqa: E402


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)


def _content(db, text: str, *, content_type: str = "x_thread") -> int:
    return db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=5.0,
        eval_feedback="Needs work",
    )


def _insert_run(
    db,
    *,
    batch_id: str,
    content_id: int | None,
    outcome: str = "below_threshold",
    rejection_reason: str | None = None,
    filter_stats: dict | str | None = None,
    created_at: datetime | None = None,
) -> int:
    run_id = db.insert_pipeline_run(
        batch_id=batch_id,
        content_type="x_thread",
        candidates_generated=3,
        best_candidate_index=0,
        best_score_before_refine=5.0,
        best_score_after_refine=5.4,
        final_score=5.4,
        published=False,
        content_id=content_id,
        outcome=outcome,
        rejection_reason=rejection_reason,
        filter_stats=filter_stats if isinstance(filter_stats, dict) else None,
    )
    if isinstance(filter_stats, str):
        db.conn.execute(
            "UPDATE pipeline_runs SET filter_stats = ? WHERE id = ?",
            (filter_stats, run_id),
        )
    db.conn.execute(
        "UPDATE pipeline_runs SET created_at = ? WHERE id = ?",
        ((created_at or NOW - timedelta(days=1)).isoformat(), run_id),
    )
    db.conn.commit()
    return run_id


def _insert_feedback(
    db,
    *,
    content_id: int,
    feedback_type: str,
    notes: str,
    created_at: datetime | None = None,
) -> None:
    db.conn.execute(
        """INSERT INTO content_feedback
           (content_id, feedback_type, notes, created_at)
           VALUES (?, ?, ?, ?)""",
        (content_id, feedback_type, notes, (created_at or NOW - timedelta(days=1)).isoformat()),
    )
    db.conn.commit()


def test_report_groups_rejections_by_stage_and_normalized_reason(db):
    first_id = _content(db, "A thread with an unsupported claim about launch metrics.")
    second_id = _content(db, "A second draft repeats the same stale framing.")
    _insert_run(
        db,
        batch_id="claim-check",
        content_id=first_id,
        outcome="all_filtered",
        rejection_reason="All candidates filtered",
        filter_stats={"claim_check_rejected": 2},
    )
    _insert_run(
        db,
        batch_id="repetition",
        content_id=second_id,
        rejection_reason="Score 5.4 below threshold 7.0",
        filter_stats={"repetition_rejected": 1},
    )

    report = build_pipeline_rejection_recovery_report(db, days=7, now=NOW)
    groups = {(group["stage"], group["reason"]): group for group in report["groups"]}

    assert groups[("filter", "claim_check")]["count"] == 2
    assert groups[("filter", "claim_check")]["recommendation"] == "add evidence"
    assert groups[("filter", "claim_check")]["candidate_content_ids"] == [first_id]
    assert groups[("filter", "repetition")]["recommendation"] == "retire pattern"
    assert groups[("evaluation", "below_threshold")]["recommendation"] == "adjust hook"
    assert second_id in groups[("evaluation", "below_threshold")]["candidate_content_ids"]


def test_report_includes_feedback_and_generated_content_rejection_signals(db):
    feedback_id = _content(db, "Opening is generic and needs sharper proof.")
    curation_id = _content(db, "Too specific to an internal detail without context.")
    db.conn.execute(
        "UPDATE generated_content SET curation_quality = ?, created_at = ? WHERE id = ?",
        ("too_specific", (NOW - timedelta(days=1)).isoformat(), curation_id),
    )
    db.conn.commit()
    _insert_feedback(
        db,
        content_id=feedback_id,
        feedback_type="reject",
        notes="Needs evidence before this claim is usable.",
    )

    report = build_pipeline_rejection_recovery_report(db, days=7, now=NOW)
    groups = {(group["stage"], group["reason"]): group for group in report["groups"]}

    assert groups[("manual_feedback", "claim_check")]["recommendation"] == "add evidence"
    assert groups[("manual_feedback", "claim_check")]["candidate_content_ids"] == [feedback_id]
    assert groups[("content_state", "missing_evidence")]["candidate_content_ids"] == [curation_id]
    assert "Too specific" in groups[("content_state", "missing_evidence")]["representative_examples"][0]["summary"]


def test_report_stage_filter_limit_and_representatives_are_stable(db):
    first_id = _content(db, "Thread one is too long and unfocused.")
    second_id = _content(db, "Thread two is too long and unfocused.")
    _insert_run(
        db,
        batch_id="long-1",
        content_id=first_id,
        outcome="all_filtered",
        rejection_reason="Thread validation failed: too long",
        filter_stats={"thread_validation_valid": False},
    )
    _insert_run(
        db,
        batch_id="long-2",
        content_id=second_id,
        outcome="all_filtered",
        rejection_reason="Thread validation failed: too long",
        filter_stats={"thread_validation_valid": False},
    )

    report = build_pipeline_rejection_recovery_report(
        db,
        days=7,
        stage="filter",
        limit=1,
        now=NOW,
    )

    assert report["stage"] == "filter"
    assert len(report["groups"]) == 1
    assert report["groups"][0]["reason"] == "thread_length"
    assert report["groups"][0]["recommendation"] == "shorten thread"
    assert report["groups"][0]["representative_run_ids"] == [1, 2]
    assert report["groups"][0]["candidate_content_ids"] == [first_id, second_id]


def test_text_and_json_formatters_are_deterministic(db):
    content_id = _content(db, "Hook starts too slowly for a short post.", content_type="x_post")
    _insert_run(
        db,
        batch_id="slow-hook",
        content_id=content_id,
        rejection_reason="Score 5.4 below threshold 7.0",
    )
    _insert_run(
        db,
        batch_id="bad-json",
        content_id=None,
        rejection_reason="Score 5.4 below threshold 7.0",
        filter_stats="{not json",
    )

    report = build_pipeline_rejection_recovery_report(db, days=7, now=NOW)
    text = format_pipeline_rejection_recovery_text(report)
    payload = json.loads(format_pipeline_rejection_recovery_json(report))

    assert "Pipeline rejection recovery report" in text
    assert "evaluation / below_threshold: 2 -> adjust hook" in text
    assert f"candidates: {content_id}" in text
    assert "Parse warnings:" in text
    assert payload["groups"][0]["stage"]
    assert payload["parse_warnings"][0]["batch_id"] == "bad-json"
    assert format_pipeline_rejection_recovery_json(report) == format_pipeline_rejection_recovery_json(report)


def test_cli_argument_parsing_and_main_text_output(monkeypatch, capsys):
    args = parse_args(["--days", "3", "--stage", "filter", "--limit", "2", "--format", "json"])

    assert args.days == 3
    assert args.stage == "filter"
    assert args.limit == 2
    assert args.format == "json"

    class DummyContext:
        def __enter__(self):
            return None, object()

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_script_context():
        return DummyContext()

    def fake_build_report(db, *, days, stage, limit):
        return {
            "generated_at": NOW.isoformat(),
            "window_days": days,
            "stage": stage or "all",
            "limit": limit,
            "totals": {"events": 0, "groups": 0, "candidate_content": 0, "by_stage": {}},
            "groups": [],
            "parse_warnings": [],
            "empty_state": {
                "is_empty": True,
                "schema_present": True,
                "message": "No recent pipeline rejections found.",
            },
        }

    monkeypatch.setattr("pipeline_rejection_recovery.script_context", fake_script_context)
    monkeypatch.setattr(
        "pipeline_rejection_recovery.build_pipeline_rejection_recovery_report",
        fake_build_report,
    )

    assert main(["--days", "3", "--stage", "filter", "--limit", "2"]) == 0
    output = capsys.readouterr().out

    assert "Pipeline rejection recovery report" in output
    assert "Stage: filter" in output
