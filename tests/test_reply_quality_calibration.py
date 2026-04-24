"""Tests for reply quality calibration reports."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from evaluation.reply_quality_calibration import (
    ReplyQualityCalibrator,
    format_reply_quality_calibration_json,
    format_reply_quality_calibration_markdown,
)


NOW = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)


def _insert_reply(
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


def test_report_groups_scores_and_outcome_rates(db):
    low_rejected = _insert_reply(
        db,
        tweet_id="low-rejected",
        score=3.2,
        status="dismissed",
        flags=["generic"],
    )
    low_approved = _insert_reply(db, tweet_id="low-approved", score=3.8, status="posted")
    high_approved = _insert_reply(db, tweet_id="high-approved", score=8.4, status="posted")

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

    report = ReplyQualityCalibrator(db).build_report(
        days=7,
        min_samples=2,
        now=NOW,
    )

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
    target = _insert_reply(db, tweet_id="target", score=7.4, status="posted")
    other = _insert_reply(db, tweet_id="other", score=7.6, status="posted")

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
        reply_id = _insert_reply(
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
    rendered = format_reply_quality_calibration_json(report)
    parsed = json.loads(rendered)

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
    reply_id = _insert_reply(
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
