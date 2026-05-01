"""Tests for content idea funnel reporting."""

from __future__ import annotations

import json
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from content_idea_funnel import main  # noqa: E402
from evaluation.content_idea_funnel import (  # noqa: E402
    build_content_idea_funnel_report,
    format_content_idea_funnel_json,
    format_content_idea_funnel_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


def _set_idea_created_at(db, idea_id: int, created_at: str = "2026-04-20T12:00:00+00:00") -> None:
    db.conn.execute(
        "UPDATE content_ideas SET created_at = ?, updated_at = ? WHERE id = ?",
        (created_at, created_at, idea_id),
    )
    db.conn.commit()


def _content(db, text: str, *, published: bool = False, auto_quality: str | None = None) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET published = ?, published_at = ?, auto_quality = ? WHERE id = ?",
        (
            1 if published else 0,
            "2026-04-25T12:00:00+00:00" if published else None,
            auto_quality,
            content_id,
        ),
    )
    db.conn.commit()
    return content_id


def _row_by_source_topic(report, source: str, topic: str):
    return next(row for row in report.rows if row.source == source and row.topic == topic)


def test_full_funnel_counts_and_rates_by_source_and_topic(db):
    idea_id = db.add_content_idea(
        "Release thread",
        topic="release",
        source="release_digest",
    )
    _set_idea_created_at(db, idea_id)
    content_id = _content(db, "Published release post", published=True, auto_quality="resonated")
    planned_id = db.promote_content_idea(idea_id, "2026-04-24", topic="release")
    db.mark_planned_topic_generated(planned_id, content_id)
    db.insert_engagement(
        content_id=content_id,
        tweet_id="tweet-1",
        like_count=1,
        retweet_count=1,
        reply_count=1,
        quote_count=1,
        engagement_score=8.0,
    )

    report = build_content_idea_funnel_report(db, days=60, now=NOW)
    row = _row_by_source_topic(report, "release_digest", "release")

    assert row.counts == {
        "created": 1,
        "promoted": 1,
        "planned": 1,
        "generated": 1,
        "published": 1,
        "resonated": 1,
    }
    assert row.conversion_rates["published"] == 1.0
    assert row.previous_stage_rates["resonated"] == 1.0
    assert row.largest_dropoff.lost_count == 0
    assert row.planned_topic_ids == (planned_id,)
    assert row.content_ids == (content_id,)


def test_abandoned_ideas_drop_at_promotion_stage(db):
    open_id = db.add_content_idea("Open issue follow-up", topic="issues", source="issue_digest")
    dismissed_id = db.add_content_idea(
        "Dismissed issue follow-up",
        topic="issues",
        source="issue_digest",
        status="dismissed",
    )
    _set_idea_created_at(db, open_id)
    _set_idea_created_at(db, dismissed_id)

    report = build_content_idea_funnel_report(db, days=60, now=NOW)
    row = _row_by_source_topic(report, "issue_digest", "issues")

    assert row.counts["created"] == 2
    assert row.counts["promoted"] == 0
    assert row.counts["planned"] == 0
    assert row.largest_dropoff.from_stage == "created"
    assert row.largest_dropoff.to_stage == "promoted"
    assert row.largest_dropoff.drop_rate == 1.0
    assert "*Created->Promoted -2 (100%)*" in format_content_idea_funnel_text(report)


def test_promoted_but_unpublished_idea_drops_at_publication(db):
    idea_id = db.add_content_idea("Queue a tip", topic="tips", source="manual")
    _set_idea_created_at(db, idea_id)
    planned_id = db.promote_content_idea(idea_id, "2026-04-24", topic="tips")
    content_id = _content(db, "Generated but not published", published=False)
    db.mark_planned_topic_generated(planned_id, content_id)
    db.upsert_publication_failure(content_id, "x", "network", error_category="network")

    report = build_content_idea_funnel_report(db, days=60, now=NOW)
    row = _row_by_source_topic(report, "manual", "tips")

    assert row.counts["promoted"] == 1
    assert row.counts["planned"] == 1
    assert row.counts["generated"] == 1
    assert row.counts["published"] == 0
    assert row.counts["resonated"] == 0
    assert row.largest_dropoff.from_stage == "generated"
    assert row.largest_dropoff.to_stage == "published"


def test_missing_optional_tables_still_reports_minimal_funnel():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE content_ideas (
            id INTEGER PRIMARY KEY,
            note TEXT,
            topic TEXT,
            status TEXT,
            source TEXT,
            source_metadata TEXT,
            created_at TEXT
        );
        CREATE TABLE planned_topics (
            id INTEGER PRIMARY KEY,
            topic TEXT,
            source_material TEXT,
            status TEXT,
            content_id INTEGER,
            created_at TEXT
        );
        CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            content TEXT,
            published INTEGER,
            published_at TEXT,
            auto_quality TEXT,
            created_at TEXT
        );
        INSERT INTO content_ideas
            (id, note, topic, status, source, source_metadata, created_at)
        VALUES
            (1, 'Minimal idea', 'ops', 'promoted', 'manual', NULL, '2026-04-20T12:00:00+00:00');
        INSERT INTO planned_topics
            (id, topic, source_material, status, content_id, created_at)
        VALUES
            (10, 'ops', '{"content_idea_id": 1}', 'generated', 100, '2026-04-21T12:00:00+00:00');
        INSERT INTO generated_content
            (id, content, published, published_at, auto_quality, created_at)
        VALUES
            (100, 'Minimal content', 1, '2026-04-22T12:00:00+00:00', NULL, '2026-04-22T12:00:00+00:00');
        """
    )

    report = build_content_idea_funnel_report(conn, now=NOW)
    row = _row_by_source_topic(report, "manual", "ops")
    payload = json.loads(format_content_idea_funnel_json(report))

    assert row.counts["published"] == 1
    assert row.counts["resonated"] == 0
    assert "content_publications" in report.missing_optional_tables
    assert "post_engagement" in report.missing_optional_tables
    assert payload["rows"][0]["counts"]["created"] == 1
    assert format_content_idea_funnel_json(report) == format_content_idea_funnel_json(report)


def test_cli_supports_json_source_topic_and_lookback_flags(db, capsys):
    idea_id = db.add_content_idea("CLI idea", topic="ops", source="manual")
    _set_idea_created_at(db, idea_id)

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("content_idea_funnel.script_context", fake_script_context):
        exit_code = main(
            [
                "--lookback-days",
                "30",
                "--source",
                "manual",
                "--topic",
                "ops",
                "--group-by",
                "topic",
                "--json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["days"] == 30
    assert payload["group_by"] == "topic"
    assert payload["source"] == "manual"
    assert payload["topic"] == "ops"
    assert payload["rows"][0]["topic"] == "ops"
