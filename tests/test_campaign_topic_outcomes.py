"""Tests for campaign planned-topic outcome reporting."""

from __future__ import annotations

import json
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from campaign_topic_outcomes import main  # noqa: E402
from evaluation.campaign_topic_outcomes import (  # noqa: E402
    build_campaign_topic_outcomes_report,
    format_campaign_topic_outcomes_json,
    format_campaign_topic_outcomes_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


def _content(db, text: str, *, created_at: str = "2026-04-20T12:00:00+00:00") -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ? WHERE id = ?",
        (created_at, content_id),
    )
    db.conn.commit()
    return content_id


def _generated_topic(
    db,
    content_id: int,
    *,
    campaign_id: int | None,
    topic: str,
    target_date: str = "2026-04-20",
) -> int:
    planned_id = db.insert_planned_topic(
        topic=topic,
        angle=f"{topic} angle",
        target_date=target_date,
        campaign_id=campaign_id,
    )
    db.mark_planned_topic_generated(planned_id, content_id)
    return planned_id


def test_report_labels_wins_neutral_unpublished_and_no_metrics(db):
    campaign_id = db.create_campaign(name="Launch", start_date="2026-04-01")
    won_content = _content(db, "Won")
    neutral_content = _content(db, "Neutral")
    unpublished_content = _content(db, "Unpublished")
    no_metrics_content = _content(db, "No metrics")

    _generated_topic(db, won_content, campaign_id=campaign_id, topic="won")
    _generated_topic(db, neutral_content, campaign_id=campaign_id, topic="neutral")
    _generated_topic(db, unpublished_content, campaign_id=campaign_id, topic="draft")
    _generated_topic(db, no_metrics_content, campaign_id=campaign_id, topic="quiet")

    db.upsert_publication_success(won_content, "x", published_at="2026-04-21T12:00:00+00:00")
    db.upsert_publication_success(won_content, "linkedin", published_at="2026-04-21T13:00:00+00:00")
    db.insert_engagement(won_content, "tweet-won", 0, 0, 0, 0, 4.0)
    db.insert_linkedin_engagement(
        won_content,
        post_id="li-won",
        engagement_score=2.0,
        fetched_at="2026-04-22T12:00:00+00:00",
    )

    db.upsert_publication_success(neutral_content, "bluesky", published_at="2026-04-22T12:00:00+00:00")
    db.insert_bluesky_engagement(
        neutral_content,
        bluesky_uri="at://neutral",
        like_count=0,
        repost_count=0,
        reply_count=0,
        quote_count=0,
        engagement_score=1.0,
    )

    db.upsert_publication_success(no_metrics_content, "x", published_at="2026-04-23T12:00:00+00:00")

    report = build_campaign_topic_outcomes_report(
        db,
        campaign_id=campaign_id,
        days=60,
        now=NOW,
    )

    rows = {row.topic: row for row in report.rows}
    assert rows["won"].outcome == "won"
    assert rows["won"].platforms == ("linkedin", "x")
    assert rows["won"].total_engagement == 6.0
    assert rows["neutral"].outcome == "neutral"
    assert rows["neutral"].publish_status == "published"
    assert rows["draft"].outcome == "missed_publish"
    assert rows["draft"].publish_status == "unpublished"
    assert rows["quiet"].outcome == "no_metrics"


def test_filters_by_campaign_id_and_days_independently(db):
    first_campaign = db.create_campaign(name="First")
    second_campaign = db.create_campaign(name="Second")
    recent_first = _content(db, "Recent first", created_at="2026-04-25T12:00:00+00:00")
    recent_second = _content(db, "Recent second", created_at="2026-04-25T12:00:00+00:00")
    old_first = _content(db, "Old first", created_at="2026-01-01T12:00:00+00:00")

    _generated_topic(
        db,
        recent_first,
        campaign_id=first_campaign,
        topic="recent-first",
        target_date="2026-04-25",
    )
    _generated_topic(
        db,
        recent_second,
        campaign_id=second_campaign,
        topic="recent-second",
        target_date="2026-04-25",
    )
    _generated_topic(
        db,
        old_first,
        campaign_id=first_campaign,
        topic="old-first",
        target_date="2026-01-01",
    )

    campaign_report = build_campaign_topic_outcomes_report(
        db,
        campaign_id=first_campaign,
        days=None,
        now=NOW,
    )
    days_report = build_campaign_topic_outcomes_report(db, days=14, now=NOW)

    assert [row.topic for row in campaign_report.rows] == ["old-first", "recent-first"]
    assert [row.topic for row in days_report.rows] == ["recent-first", "recent-second"]


def test_json_and_text_format_empty_without_required_tables():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_campaign_topic_outcomes_report(conn, now=NOW)
    text = format_campaign_topic_outcomes_text(report)

    assert report.rows == ()
    assert json.loads(format_campaign_topic_outcomes_json(report))["rows"] == []
    assert "No generated planned topics found." in text


def test_cli_prints_json_when_flag_is_passed(db, capsys):
    campaign_id = db.create_campaign(name="CLI")
    content_id = _content(db, "CLI content")
    _generated_topic(db, content_id, campaign_id=campaign_id, topic="cli")

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("campaign_topic_outcomes.script_context", fake_script_context):
        exit_code = main(["--campaign-id", str(campaign_id), "--days", "30", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["campaign_id"] == campaign_id
    assert payload["days"] == 30
    assert payload["rows"][0]["topic"] == "cli"


def test_cli_prints_readable_table_by_default(db, capsys):
    campaign_id = db.create_campaign(name="Table")
    content_id = _content(db, "Table content")
    _generated_topic(db, content_id, campaign_id=campaign_id, topic="table")

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("campaign_topic_outcomes.script_context", fake_script_context):
        exit_code = main(["--campaign-id", str(campaign_id), "--days", "30"])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "Campaign Planned Topic Outcomes" in output
    assert "table" in output
    assert "Outcome" in output
