"""Tests for content idea promotion candidate ranking."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from promote_content_ideas import main
from synthesis.content_idea_promotion import (
    build_content_idea_promotion_report,
    format_content_idea_promotion_json,
    format_content_idea_promotion_text,
    normalize_topic_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


def _content(
    db,
    topic: str,
    *,
    x: float | None = None,
    linkedin: float | None = None,
    bluesky: float | None = None,
) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=f"Post about {topic}",
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.insert_content_topics(content_id, [(topic, "", 0.9)])
    if x is not None:
        db.insert_engagement(
            content_id=content_id,
            tweet_id=f"tweet-{content_id}",
            like_count=0,
            retweet_count=0,
            reply_count=0,
            quote_count=0,
            engagement_score=x,
        )
        db.conn.execute(
            "UPDATE post_engagement SET fetched_at = ? WHERE content_id = ?",
            ("2026-04-28T12:00:00+00:00", content_id),
        )
    if linkedin is not None:
        db.insert_linkedin_engagement(
            content_id=content_id,
            post_id=f"li-{content_id}",
            engagement_score=linkedin,
            fetched_at="2026-04-29T12:00:00+00:00",
        )
    if bluesky is not None:
        db.insert_bluesky_engagement(
            content_id=content_id,
            bluesky_uri=f"at://post/{content_id}",
            like_count=0,
            repost_count=0,
            reply_count=0,
            quote_count=0,
            engagement_score=bluesky,
        )
        db.conn.execute(
            "UPDATE bluesky_engagement SET fetched_at = ? WHERE content_id = ?",
            ("2026-04-30T12:00:00+00:00", content_id),
        )
    db.conn.commit()
    return content_id


def test_normalize_topic_text_collapses_case_punctuation_and_spacing():
    assert normalize_topic_text("  AI-Agents: Testing!! ") == "ai agents testing"


def test_ranks_open_unsnoozed_ideas_by_related_recent_engagement(db):
    testing_id = db.add_content_idea("Promote testing idea", topic="Testing")
    agents_id = db.add_content_idea("Promote agents idea", topic="AI Agents", priority="high")
    closed_id = db.add_content_idea("Closed testing idea", topic="Testing")
    snoozed_id = db.add_content_idea("Snoozed testing idea", topic="Testing")
    expired_snooze_id = db.add_content_idea("Expired snooze testing idea", topic="Testing")
    db.dismiss_content_idea(closed_id)
    db.snooze_content_idea(snoozed_id, "2026-05-10T12:00:00+00:00")
    db.snooze_content_idea(expired_snooze_id, "2026-04-01T12:00:00+00:00")

    testing_content = _content(db, "testing", x=8.0, linkedin=5.0)
    agents_content = _content(db, "ai-agents", bluesky=20.0)

    report = build_content_idea_promotion_report(db, days=14, limit=10, now=NOW)

    assert [candidate.idea_id for candidate in report.candidates] == [
        agents_id,
        testing_id,
        expired_snooze_id,
    ]
    assert report.candidates[0].topic == "AI Agents"
    assert report.candidates[0].priority == "high"
    assert report.candidates[0].matched_content_ids == (agents_content,)
    assert "bluesky" in report.candidates[0].score_reasons[2]
    assert report.candidates[1].matched_content_ids == (testing_content,)
    assert report.candidates[2].matched_content_ids == (testing_content,)
    assert "x" in report.candidates[1].score_reasons[2]
    assert "linkedin" in report.candidates[1].score_reasons[2]


def test_include_snoozed_allows_open_snoozed_candidates(db):
    snoozed_id = db.add_content_idea("Snoozed but promising", topic="testing")
    db.snooze_content_idea(snoozed_id, "2026-05-10T12:00:00+00:00")
    _content(db, "testing", x=10.0)

    assert build_content_idea_promotion_report(db, now=NOW).candidates == ()

    report = build_content_idea_promotion_report(db, include_snoozed=True, now=NOW)

    assert [candidate.idea_id for candidate in report.candidates] == [snoozed_id]


def test_old_engagement_snapshots_do_not_score_candidates(db):
    db.add_content_idea("Old engagement should not qualify", topic="testing")
    content_id = _content(db, "testing", x=10.0)
    db.conn.execute(
        "UPDATE post_engagement SET fetched_at = ? WHERE content_id = ?",
        ("2025-01-01T12:00:00+00:00", content_id),
    )
    db.conn.commit()

    report = build_content_idea_promotion_report(db, days=30, now=NOW)

    assert report.candidates == ()


def test_limit_and_json_output(db):
    first_id = db.add_content_idea("First", topic="testing")
    db.add_content_idea("Second", topic="ops")
    _content(db, "testing", x=10.0)
    _content(db, "ops", x=5.0)

    report = build_content_idea_promotion_report(db, limit=1, now=NOW)
    payload = json.loads(format_content_idea_promotion_json(report))

    assert [candidate.idea_id for candidate in report.candidates] == [first_id]
    assert payload["candidate_count"] == 1
    assert payload["candidates"][0]["idea_id"] == first_id
    assert payload["candidates"][0]["score_reasons"]


def test_text_output_includes_candidate_details(db):
    idea_id = db.add_content_idea("Readable output idea", topic="testing")
    content_id = _content(db, "testing", x=7.0)

    output = format_content_idea_promotion_text(
        build_content_idea_promotion_report(db, now=NOW)
    )

    assert "Content Idea Promotion Candidates" in output
    assert f"idea #{idea_id}" in output
    assert f"#{content_id}" in output
    assert "Average matched engagement" in output


def test_cli_wiring_json_output(db, capsys):
    idea_id = db.add_content_idea("CLI candidate", topic="testing")
    _content(db, "testing", x=9.0)

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("promote_content_ideas.script_context", fake_script_context):
        exit_code = main(["--days", "30", "--limit", "5", "--format", "json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["candidates"][0]["idea_id"] == idea_id
