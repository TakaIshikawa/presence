"""Tests for reply quality calibration reports."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from evaluation.reply_quality_calibration import (  # noqa: E402
    ReplyQualityCalibrator,
    build_reply_quality_calibration_report,
    format_reply_quality_calibration_json,
    format_reply_quality_calibration_markdown,
    format_text_report,
)
from reply_quality_calibration import main  # noqa: E402


NOW = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)


def _insert_review_reply(
    db,
    *,
    tweet_id: str,
    score: float,
    status: str = "pending",
    flags: list[str] | None = None,
    detected_at: str = "2026-04-24 10:00:00",
) -> int:
    reply_id = db.insert_reply_draft(
        inbound_tweet_id=tweet_id,
        inbound_author_handle="alice",
        inbound_author_id="user_A",
        inbound_text="Nice post!",
        our_tweet_id=f"our-{tweet_id}",
        our_content_id=None,
        our_post_text="Our original post",
        draft_text="Thanks for the thoughtful note",
        quality_score=score,
        quality_flags=json.dumps(flags or []),
        status=status,
    )
    db.conn.execute(
        "UPDATE reply_queue SET detected_at = ?, reviewed_at = ? WHERE id = ?",
        (detected_at, detected_at, reply_id),
    )
    db.conn.commit()
    return reply_id


def _insert_scored_reply(db, tweet_id: str, **kwargs) -> int:
    defaults = dict(
        inbound_tweet_id=tweet_id,
        inbound_author_handle="alice",
        inbound_author_id="user-1",
        inbound_text="Nice post",
        our_tweet_id="our-1",
        our_content_id=None,
        our_post_text="Our post",
        draft_text="Thanks, that matches what we saw too.",
        platform="x",
        intent="other",
        priority="normal",
        quality_score=7.0,
        quality_flags=None,
        status="pending",
    )
    defaults.update(kwargs)
    return db.insert_reply_draft(**defaults)


def test_report_groups_scores_and_outcome_rates(db):
    low_rejected = _insert_review_reply(
        db,
        tweet_id="low-rejected",
        score=3.2,
        status="dismissed",
        flags=["generic"],
    )
    low_approved = _insert_review_reply(
        db,
        tweet_id="low-approved",
        score=3.8,
        status="posted",
    )
    high_approved = _insert_review_reply(
        db,
        tweet_id="high-approved",
        score=8.4,
        status="posted",
    )

    db.record_reply_review_event(
        low_rejected,
        "rejected",
        actor="reviewer",
        old_status="pending",
        new_status="dismissed",
        notes="Too generic",
        created_at="2026-04-24 10:05:00",
    )
    db.record_reply_review_event(
        low_approved,
        "approved",
        actor="reviewer",
        old_status="pending",
        new_status="pending",
        notes="Approved draft for publishing",
        created_at="2026-04-24 10:06:00",
    )
    db.record_reply_review_event(
        high_approved,
        "posted",
        actor="publisher",
        old_status="pending",
        new_status="posted",
        notes="posted",
        created_at="2026-04-24 10:07:00",
    )

    report = ReplyQualityCalibrator(db).build_report(days=7, min_samples=2, now=NOW)

    band_2_4 = next(b for b in report.score_bands if b.band == "2-4")
    assert band_2_4.count == 2
    assert band_2_4.approval_count == 1
    assert band_2_4.rejection_count == 1
    assert band_2_4.dismissal_count == 1
    assert band_2_4.approval_rate == 0.5
    assert band_2_4.rejection_rate == 0.5
    assert band_2_4.dismissal_rate == 0.5
    assert band_2_4.common_failure_reasons == [
        {"reason": "flag:generic", "count": 1},
        {"reason": "too generic", "count": 1},
    ]
    assert report.sample_count == 3
    assert report.approval_count == 2
    assert report.rejection_count == 1
    assert report.dismissal_count == 1


def test_review_events_are_associated_with_correct_reply_draft(db):
    target = _insert_review_reply(db, tweet_id="target", score=7.4, status="posted")
    other = _insert_review_reply(db, tweet_id="other", score=7.6, status="posted")

    db.record_reply_review_event(
        target,
        "approved",
        actor="reviewer",
        old_status="pending",
        new_status="pending",
        created_at="2026-04-24 10:01:00",
    )
    db.record_reply_review_event(
        other,
        "rejected",
        actor="reviewer",
        old_status="pending",
        new_status="dismissed",
        notes="Wrong thread",
        created_at="2026-04-24 10:02:00",
    )

    report = ReplyQualityCalibrator(db).build_report(days=7, min_samples=1, now=NOW)
    band_6_8 = next(b for b in report.score_bands if b.band == "6-8")

    assert band_6_8.count == 2
    assert band_6_8.approval_count == 2
    assert band_6_8.rejection_count == 1
    assert report.common_failure_reasons == [{"reason": "wrong thread", "count": 1}]


def test_json_output_is_stable_and_includes_threshold_recommendation(db):
    for idx in range(2):
        reply_id = _insert_review_reply(
            db,
            tweet_id=f"reject-{idx}",
            score=5.2 + idx * 0.1,
            status="dismissed",
            flags=["stage_mismatch"],
        )
        db.record_reply_review_event(
            reply_id,
            "rejected",
            actor="reviewer",
            old_status="pending",
            new_status="dismissed",
            notes="Stage mismatch",
            created_at="2026-04-24 10:00:00",
        )

    report = ReplyQualityCalibrator(db).build_report(days=7, min_samples=2, now=NOW)
    parsed = json.loads(format_reply_quality_calibration_json(report))

    assert list(parsed.keys()) == sorted(parsed.keys())
    assert parsed["threshold_recommendation"] == {
        "action": "monitor",
        "current_threshold": 6.0,
        "high_rejection_bands": ["4-6"],
        "min_samples": 2,
        "rationale": (
            "One or more score bands meet the minimum sample size and have rejection rates "
            "of at least 40%."
        ),
        "recommended_threshold": 6.0,
    }
    assert parsed["score_bands"][2]["band"] == "4-6"
    assert parsed["score_bands"][2]["rejection_rate"] == 1.0


def test_markdown_output_includes_same_findings(db):
    reply_id = _insert_review_reply(
        db,
        tweet_id="md-reject",
        score=2.5,
        status="dismissed",
        flags=["sycophantic"],
    )
    db.record_reply_review_event(
        reply_id,
        "rejected",
        actor="reviewer",
        old_status="pending",
        new_status="dismissed",
        notes="Dismissed during manual review",
        created_at="2026-04-24 10:00:00",
    )

    report = ReplyQualityCalibrator(db).build_report(days=7, min_samples=1, now=NOW)
    markdown = format_reply_quality_calibration_markdown(report)

    assert "# Reply Quality Calibration Report" in markdown
    assert "| 2-4 | 1 | 2.50 | 0.0% | 100.0% | 100.0%" in markdown
    assert "flag:sycophantic (1)" in markdown
    assert "dismissed during manual review: 1" in markdown
    assert "## Threshold Recommendation" in markdown
    assert "High rejection bands: 2-4" in markdown


def test_report_groups_score_distributions_by_intent_priority_platform_and_flag(db):
    _insert_scored_reply(
        db,
        "approved-question",
        status="approved",
        quality_score=7.5,
        quality_flags=json.dumps(["specific"]),
        intent="question",
        priority="high",
        platform="x",
    )
    _insert_scored_reply(
        db,
        "dismissed-appreciation",
        status="dismissed",
        quality_score=3.5,
        quality_flags=json.dumps(["generic", "sycophantic"]),
        intent="appreciation",
        priority="low",
        platform="bluesky",
    )

    report = build_reply_quality_calibration_report(db, days=30, threshold=6.0)

    assert report["groups"]["intent"]["question"]["count"] == 1
    assert report["groups"]["priority"]["high"]["avg_score"] == 7.5
    assert report["groups"]["platform"]["bluesky"]["status_counts"] == {
        "dismissed": 1
    }
    assert report["groups"]["quality_flag"]["generic"]["count"] == 1
    assert report["groups"]["quality_flag"]["specific"]["at_or_above_threshold"] == 1


def test_report_identifies_likely_false_positives_and_false_negatives(db):
    low_accepted = _insert_scored_reply(
        db,
        "approved-low-score",
        status="approved",
        quality_score=4.5,
        quality_flags=json.dumps(["generic"]),
        intent="question",
    )
    high_posted = _insert_scored_reply(
        db,
        "posted-low-score",
        status="posted",
        quality_score=5.5,
        quality_flags=None,
        intent="bug_report",
    )
    high_dismissed = _insert_scored_reply(
        db,
        "dismissed-high-score",
        status="dismissed",
        quality_score=8.0,
        quality_flags=json.dumps(["stage_mismatch"]),
        intent="question",
    )
    _insert_scored_reply(db, "pending-low-score", status="pending", quality_score=2.0)

    report = build_reply_quality_calibration_report(db, days=30, threshold=6.0)

    assert {item["id"] for item in report["likely_false_positives"]} == {
        low_accepted,
        high_posted,
    }
    assert report["likely_false_positive_count"] == 2
    assert report["likely_false_negatives"] == [
        {
            "id": high_dismissed,
            "status": "dismissed",
            "quality_score": 8.0,
            "quality_flags": ["stage_mismatch"],
            "intent": "question",
            "priority": "normal",
            "platform": "x",
            "author": "alice",
            "detected_at": report["likely_false_negatives"][0]["detected_at"],
        }
    ]
    assert report["likely_false_negative_count"] == 1


def test_report_summarizes_common_rejection_flags_and_threshold_recommendations(db):
    _insert_scored_reply(
        db,
        "dismissed-generic",
        status="dismissed",
        quality_score=3.0,
        quality_flags=json.dumps(["generic"]),
        intent="appreciation",
    )
    _insert_scored_reply(
        db,
        "dismissed-generic-stage",
        status="dismissed",
        quality_score=8.0,
        quality_flags=json.dumps(["generic", "stage_mismatch"]),
        intent="appreciation",
    )
    _insert_scored_reply(
        db,
        "approved-high",
        status="approved",
        quality_score=7.0,
        quality_flags=json.dumps([]),
        intent="question",
    )

    report = build_reply_quality_calibration_report(db, days=30, threshold=6.0)

    assert report["common_rejection_flags"][0] == {"flag": "generic", "count": 2}
    recommendation = report["threshold_recommendation"]
    assert recommendation["current_threshold"] == 6.0
    assert "recommended_threshold" in recommendation
    assert "expected_mismatch_rate" in recommendation
    assert "rationale" in recommendation


def test_text_report_includes_calibration_sections(db):
    _insert_scored_reply(
        db,
        "dismissed-high",
        status="dismissed",
        quality_score=8.0,
        quality_flags=json.dumps(["generic"]),
        intent="question",
    )

    text = format_text_report(
        build_reply_quality_calibration_report(db, days=14, threshold=6.0)
    )

    assert "Reply Quality Calibration Report" in text
    assert "False negatives: 1 dismissed at or above threshold" in text
    assert "Common Rejection Flags" in text
    assert "Groups" in text


def test_cli_outputs_json_without_storing_report(capsys):
    db = MagicMock()
    report = {
        "threshold_recommendation": {"recommended_threshold": 6.0},
        "sample_size": 0,
    }

    with (
        patch("reply_quality_calibration.script_context") as script_context,
        patch(
            "reply_quality_calibration.build_reply_quality_calibration_report",
            return_value=report,
        ) as build_report,
    ):
        script_context.return_value.__enter__.return_value = (MagicMock(), db)
        assert main(["--days", "7", "--threshold", "5.5", "--format", "text"]) == 0

    build_report.assert_called_once_with(db, days=7, threshold=5.5)


def test_cli_outputs_legacy_json_report(capsys):
    db = MagicMock()
    report = MagicMock()

    with (
        patch("reply_quality_calibration.script_context") as script_context,
        patch(
            "reply_quality_calibration.ReplyQualityCalibrator",
        ) as calibrator_cls,
        patch(
            "reply_quality_calibration.format_reply_quality_calibration_json",
            return_value='{"status":"ok"}',
        ),
    ):
        calibrator = calibrator_cls.return_value
        calibrator.build_report.return_value = report
        script_context.return_value.__enter__.return_value = (MagicMock(), db)
        assert main(["--days", "7", "--json"]) == 0

    calibrator.build_report.assert_called_once_with(days=7, min_samples=5)
    assert json.loads(capsys.readouterr().out) == {"status": "ok"}
