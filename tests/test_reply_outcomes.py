"""Tests for reply outcome analytics."""

import json

from evaluation.reply_outcomes import (
    build_reply_outcome_report,
    format_reply_outcome_json,
    format_reply_outcome_text,
)


def _insert_reply(db, tweet_id: str, **kwargs) -> int:
    defaults = dict(
        inbound_tweet_id=tweet_id,
        inbound_author_handle="alice",
        inbound_author_id="user_A",
        inbound_text="Nice post!",
        our_tweet_id="our_tw_1",
        our_content_id=None,
        our_post_text="Our original post",
        draft_text="Thanks for the kind words",
    )
    defaults.update(kwargs)
    return db.insert_reply_draft(**defaults)


def _set_times(db, reply_id: int, **kwargs) -> None:
    assignments = ", ".join(f"{key} = ?" for key in kwargs)
    db.conn.execute(
        f"UPDATE reply_queue SET {assignments} WHERE id = ?",
        (*kwargs.values(), reply_id),
    )
    db.conn.commit()


def _rows_and_events(db):
    rows = [dict(row) for row in db.conn.execute("SELECT * FROM reply_queue ORDER BY id")]
    events = [dict(row) for row in db.conn.execute("SELECT * FROM reply_review_events ORDER BY id")]
    return rows, events


def test_build_report_groups_counts_rates_quality_and_event_timings(db):
    posted_id = _insert_reply(
        db,
        "posted-1",
        platform="x",
        intent="question",
        priority="high",
        status="posted",
        quality_score=8.0,
    )
    dismissed_id = _insert_reply(
        db,
        "dismissed-1",
        platform="x",
        intent="question",
        priority="low",
        status="dismissed",
        quality_score=4.0,
    )
    _insert_reply(
        db,
        "pending-1",
        platform="bluesky",
        intent="appreciation",
        priority="normal",
        status="pending",
        quality_score=6.0,
    )
    approved_id = _insert_reply(
        db,
        "approved-1",
        platform="bluesky",
        intent="appreciation",
        priority="normal",
        status="approved",
    )

    for reply_id in (posted_id, dismissed_id, approved_id):
        _set_times(db, reply_id, detected_at="2026-04-20 00:00:00")
    _set_times(
        db,
        posted_id,
        reviewed_at="2026-04-20T10:00:00+00:00",
        posted_at="2026-04-20T11:00:00+00:00",
    )
    _set_times(db, approved_id, reviewed_at="2026-04-20T04:00:00+00:00")

    db.record_reply_review_event(
        posted_id,
        "approved",
        old_status="pending",
        new_status="approved",
        created_at="2026-04-20T01:00:00+00:00",
    )
    db.record_reply_review_event(
        posted_id,
        "posted",
        old_status="approved",
        new_status="posted",
        created_at="2026-04-20T03:00:00+00:00",
    )
    db.record_reply_review_event(
        dismissed_id,
        "rejected",
        old_status="pending",
        new_status="dismissed",
        created_at="2026-04-20T02:00:00+00:00",
    )

    rows, events = _rows_and_events(db)
    report = build_reply_outcome_report(rows, events, days=30, platform=None, intent=None)

    assert report["overall"]["counts"] == {
        "total": 4,
        "pending": 1,
        "approved": 1,
        "posted": 1,
        "dismissed": 1,
        "other": 0,
    }
    assert report["overall"]["conversion_rates"]["posted_rate"] == 0.25
    assert report["overall"]["conversion_rates"]["reviewed_rate"] == 0.75
    assert report["overall"]["avg_quality_score"] == 6.0
    assert report["overall"]["timing"]["median_time_to_review_hours"] == 2.0
    assert report["overall"]["timing"]["median_time_to_post_hours"] == 3.0

    by_platform = {group["group"]: group for group in report["by_platform"]}
    assert by_platform["x"]["counts"]["posted"] == 1
    assert by_platform["bluesky"]["counts"]["pending"] == 1

    by_intent = {group["group"]: group for group in report["by_intent"]}
    assert by_intent["question"]["conversion_rates"]["dismissed_rate"] == 0.5
    assert by_intent["appreciation"]["conversion_rates"]["posted_rate"] == 0.0


def test_report_handles_missing_timestamps_without_errors():
    rows = [
        {
            "id": 1,
            "platform": None,
            "intent": None,
            "priority": None,
            "status": "pending",
            "quality_score": None,
            "detected_at": None,
            "reviewed_at": None,
            "posted_at": None,
        }
    ]

    report = build_reply_outcome_report(rows, [])

    assert report["overall"]["counts"]["pending"] == 1
    assert report["overall"]["timing"]["median_time_to_review_hours"] is None
    assert report["overall"]["timing"]["median_time_to_post_hours"] is None
    assert report["by_platform"][0]["group"] == "x"
    assert report["by_intent"][0]["group"] == "other"
    assert report["by_priority"][0]["group"] == "normal"


def test_json_output_is_stable_and_machine_readable():
    report = build_reply_outcome_report(
        [
            {
                "id": 1,
                "platform": "x",
                "intent": "question",
                "priority": "high",
                "status": "posted",
                "quality_score": 7.5,
                "detected_at": "2026-04-20T00:00:00+00:00",
                "reviewed_at": "2026-04-20T01:00:00+00:00",
                "posted_at": "2026-04-20T02:00:00+00:00",
            }
        ],
        [],
        days=7,
        platform="x",
        intent="question",
    )

    encoded = format_reply_outcome_json(report)
    decoded = json.loads(encoded)

    assert encoded == format_reply_outcome_json(report)
    assert decoded["filters"] == {"days": 7, "platform": "x", "intent": "question"}
    assert decoded["overall"]["counts"]["posted"] == 1
    assert decoded["overall"]["timing"]["median_time_to_post_hours"] == 2.0


def test_text_output_highlights_low_posted_and_high_dismissal_intents():
    report = build_reply_outcome_report(
        [
            {
                "id": 1,
                "platform": "x",
                "intent": "question",
                "priority": "normal",
                "status": "dismissed",
                "quality_score": 3.0,
                "detected_at": "2026-04-20 00:00:00",
                "reviewed_at": "2026-04-20 01:00:00",
                "posted_at": None,
            },
            {
                "id": 2,
                "platform": "x",
                "intent": "question",
                "priority": "normal",
                "status": "pending",
                "quality_score": 5.0,
                "detected_at": "2026-04-20 00:00:00",
                "reviewed_at": None,
                "posted_at": None,
            },
        ],
        [],
    )

    text = format_reply_outcome_text(report)

    assert "By intent" in text
    assert "question" in text
    assert "LOW_POSTED" in text
    assert "HIGH_DISMISSAL" in text
