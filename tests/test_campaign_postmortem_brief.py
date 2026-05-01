"""Tests for campaign postmortem briefs."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

from evaluation.campaign_postmortem_brief import (
    build_campaign_postmortem_brief,
    format_json_brief,
    format_markdown_brief,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "campaign_postmortem_brief.py"
spec = importlib.util.spec_from_file_location("campaign_postmortem_brief_script", SCRIPT_PATH)
campaign_postmortem_brief_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(campaign_postmortem_brief_script)

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(
    db,
    text: str,
    *,
    content_format: str = "micro_story",
    created_at: str = "2026-04-20T12:00:00+00:00",
) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ?, content_format = ? WHERE id = ?",
        (created_at, content_format, content_id),
    )
    db.conn.commit()
    return content_id


def _topic(
    db,
    *,
    campaign_id: int,
    topic: str,
    content_id: int | None = None,
    target_date: str = "2026-04-20",
    status: str = "planned",
) -> int:
    planned_id = db.insert_planned_topic(
        topic=topic,
        angle=f"{topic} angle",
        target_date=target_date,
        campaign_id=campaign_id,
        status=status,
    )
    if content_id is not None:
        db.mark_planned_topic_generated(planned_id, content_id)
    return planned_id


def test_brief_summarizes_wins_misses_latency_formats_and_followups(db):
    campaign_id = db.create_campaign(
        name="Launch",
        goal="ship useful campaign content",
        start_date="2026-04-01",
        end_date="2026-04-30",
        status="completed",
    )
    winner = _content(db, "Winner", content_format="micro_story")
    quiet = _content(db, "Quiet", content_format="question")
    unpublished = _content(db, "Generated but not published", content_format="tip")

    winner_topic = _topic(db, campaign_id=campaign_id, topic="winner", content_id=winner)
    _topic(db, campaign_id=campaign_id, topic="quiet", content_id=quiet)
    _topic(db, campaign_id=campaign_id, topic="unpublished", content_id=unpublished)
    missed_topic = _topic(db, campaign_id=campaign_id, topic="missed")

    db.upsert_publication_success(winner, "x", published_at="2026-04-21T12:00:00+00:00")
    db.insert_engagement(winner, "tweet-winner", 10, 3, 2, 1, 10.0)
    db.upsert_publication_success(quiet, "x", published_at="2026-04-22T12:00:00+00:00")
    db.insert_engagement(quiet, "tweet-quiet", 1, 0, 0, 0, 2.0)

    brief = build_campaign_postmortem_brief(
        db,
        campaign_id=campaign_id,
        days=60,
        include_posts=True,
        now=NOW,
    )

    assert brief["summary"] == {
        "planned_topics": 4,
        "generated_topics": 3,
        "posts_in_window": 3,
        "published_posts": 2,
        "missed_planned_topics": 1,
        "generated_unpublished": 1,
        "avg_normalized_engagement": 1.0,
    }
    assert brief["wins"][0]["content_id"] == winner
    assert brief["wins"][0]["normalized_engagement"] == 1.67
    assert brief["publication_latency"]["avg_hours"] == 36.0
    assert brief["format_performance"][0]["format"] == "micro_story"
    assert brief["missed_planned_topics"][0]["planned_topic_id"] == missed_topic
    assert brief["generated_unpublished_content"][0]["content_id"] == unpublished
    assert {item["type"] for item in brief["misses"]} >= {
        "missed_planned_topic",
        "generated_unpublished_content",
        "low_normalized_engagement",
    }
    assert brief["posts"][0]["planned_topic_id"] == winner_topic


def test_json_and_markdown_are_deterministic(db):
    campaign_id = db.create_campaign(name="Stable", status="completed")
    content_id = _content(db, "Stable content")
    _topic(db, campaign_id=campaign_id, topic="stable", content_id=content_id)
    db.upsert_publication_success(content_id, "x", published_at="2026-04-21T12:00:00+00:00")
    db.insert_engagement(content_id, "tweet-stable", 1, 1, 1, 1, 4.0)

    brief = build_campaign_postmortem_brief(db, campaign_id=campaign_id, days=60, now=NOW)
    payload = json.loads(format_json_brief(brief))
    markdown = format_markdown_brief(brief)

    assert list(payload.keys()) == sorted(payload.keys())
    assert markdown.startswith("# Campaign Postmortem Brief: Stable")
    assert "## Summary" in markdown
    assert "## Recommended Follow-Ups" in markdown
    assert "posts" not in payload


def test_missing_or_empty_campaign_returns_clear_empty_brief():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    missing_tables = build_campaign_postmortem_brief(conn, campaign_id=99, now=NOW)
    assert missing_tables["summary"]["planned_topics"] == 0
    assert missing_tables["missing_required_tables"] == [
        "content_campaigns",
        "planned_topics",
    ]
    assert "Missing required tables" in format_markdown_brief(missing_tables)

    conn.execute("CREATE TABLE content_campaigns (id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute(
        """CREATE TABLE planned_topics (
            id INTEGER PRIMARY KEY,
            campaign_id INTEGER,
            topic TEXT
        )"""
    )
    empty = build_campaign_postmortem_brief(conn, campaign_id=42, now=NOW)
    assert empty["campaign"]["found"] is False
    assert empty["summary"]["planned_topics"] == 0
    assert empty["recommended_follow_ups"][0]["type"] == "campaign_review"


def test_cli_outputs_markdown_and_json(db, capsys):
    campaign_id = db.create_campaign(name="CLI", status="completed")
    content_id = _content(db, "CLI content")
    _topic(db, campaign_id=campaign_id, topic="cli", content_id=content_id)

    with patch.object(
        campaign_postmortem_brief_script,
        "script_context",
        return_value=_script_context(db),
    ):
        exit_code = campaign_postmortem_brief_script.main(
            ["--campaign-id", str(campaign_id), "--days", "30"]
        )
    assert exit_code == 0
    assert capsys.readouterr().out.startswith("# Campaign Postmortem Brief: CLI")

    with patch.object(
        campaign_postmortem_brief_script,
        "script_context",
        return_value=_script_context(db),
    ):
        exit_code = campaign_postmortem_brief_script.main(
            [
                "--campaign-id",
                str(campaign_id),
                "--days",
                "30",
                "--format",
                "json",
                "--include-posts",
            ]
        )
    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["filters"]["include_posts"] is True
    assert payload["posts"][0]["topic"] == "cli"
