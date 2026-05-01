"""Tests for building newsletter sections from published X threads."""

from __future__ import annotations

import importlib.util
import json
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from output.thread_newsletter_sections import (
    FALLBACK_ENGAGEMENT_SCORE,
    ThreadNewsletterSectionBuilder,
    export_to_json,
    format_markdown,
)


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "thread_newsletter_sections.py"
)
spec = importlib.util.spec_from_file_location("thread_newsletter_sections_script", SCRIPT_PATH)
thread_newsletter_sections_script = importlib.util.module_from_spec(spec)
spec.loader.exec_module(thread_newsletter_sections_script)

BASE_TIME = datetime.now(timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _thread(
    db,
    *,
    content: str,
    score: float | None = 10.0,
    published_days_ago: int = 1,
    engagement_days_ago: int = 0,
    content_type: str = "x_thread",
    published: bool = True,
    topic: str | None = None,
    tweet_id: str = "100",
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=8.0,
        eval_feedback="usable",
    )
    if published:
        db.mark_published(content_id, f"https://x.com/taka/status/{tweet_id}", tweet_id=tweet_id)
        db.conn.execute(
            "UPDATE generated_content SET published_at = ? WHERE id = ?",
            ((BASE_TIME - timedelta(days=published_days_ago)).isoformat(), content_id),
        )
    if score is not None:
        db.insert_engagement(
            content_id=content_id,
            tweet_id=tweet_id,
            like_count=8,
            retweet_count=2,
            reply_count=1,
            quote_count=0,
            engagement_score=score,
        )
        db.conn.execute(
            """UPDATE post_engagement
               SET fetched_at = ?
               WHERE id = (SELECT MAX(id) FROM post_engagement WHERE content_id = ?)""",
            ((BASE_TIME - timedelta(days=engagement_days_ago)).isoformat(), content_id),
        )
    if topic:
        db.insert_content_topics(content_id, [(topic, "", 0.9)])
    db.conn.commit()
    return content_id


def test_selects_recent_published_x_threads_by_score_then_recency(db):
    old_id = _thread(
        db,
        content="TWEET 1: Old high scorer\nTWEET 2: But outside the window.",
        score=99.0,
        published_days_ago=45,
        tweet_id="old",
    )
    high_id = _thread(
        db,
        content="TWEET 1: High scorer\nTWEET 2: Strong takeaway.",
        score=18.0,
        published_days_ago=2,
        tweet_id="high",
    )
    recent_tie_id = _thread(
        db,
        content="TWEET 1: Recent tie\nTWEET 2: Newer at the same score.",
        score=12.0,
        published_days_ago=1,
        tweet_id="tie-recent",
    )
    older_tie_id = _thread(
        db,
        content="TWEET 1: Older tie\nTWEET 2: Older at the same score.",
        score=12.0,
        published_days_ago=3,
        tweet_id="tie-old",
    )
    _thread(
        db,
        content="TWEET 1: X post should not appear",
        score=50.0,
        content_type="x_post",
        tweet_id="post",
    )
    _thread(
        db,
        content="TWEET 1: Unpublished thread should not appear",
        score=50.0,
        published=False,
        tweet_id="draft",
    )

    export = ThreadNewsletterSectionBuilder(db).build_export(
        days=30,
        min_score=10.0,
        limit=10,
    )

    assert [section.source_content_id for section in export.sections] == [
        high_id,
        recent_tie_id,
        older_tie_id,
    ]
    assert old_id not in [section.source_content_id for section in export.sections]


def test_builds_deterministic_section_fields_and_fallback_score(db):
    content_id = _thread(
        db,
        content=(
            "TWEET 1: Make reliability reviewable before it becomes urgent.\n"
            "TWEET 2: The pattern is simple: write the check, name the owner.\n"
            "TWEET 3: Then make the failure visible in the same place people already work.\n"
            "TWEET 4: This turns vague process advice into a repeatable habit."
        ),
        score=None,
        topic="operations",
        tweet_id="fallback",
    )

    export = ThreadNewsletterSectionBuilder(db).build_export(days=30, min_score=0, limit=5)

    assert export.fallback_score == FALLBACK_ENGAGEMENT_SCORE
    section = export.sections[0]
    assert section.source_content_id == content_id
    assert section.headline == "Make reliability reviewable before it becomes urgent."
    assert section.summary == "The pattern is simple: write the check, name the owner."
    assert section.bullets == [
        "Then make the failure visible in the same place people already work.",
        "This turns vague process advice into a repeatable habit.",
    ]
    assert section.url == "https://x.com/taka/status/fallback"
    assert section.engagement_score == 0.0
    assert section.score_source == "fallback"
    assert section.topics == ["operations"]


def test_filters_by_topic_and_recent_engagement(db):
    included_id = _thread(
        db,
        content="TWEET 1: Testing thread\nTWEET 2: Current engagement.",
        score=14.0,
        topic="testing",
        tweet_id="testing",
    )
    _thread(
        db,
        content="TWEET 1: Operations thread\nTWEET 2: Wrong topic.",
        score=20.0,
        topic="operations",
        tweet_id="operations",
    )
    _thread(
        db,
        content="TWEET 1: Stale engagement\nTWEET 2: Right topic, old snapshot.",
        score=30.0,
        engagement_days_ago=40,
        topic="testing",
        tweet_id="stale",
    )

    export = ThreadNewsletterSectionBuilder(db).build_export(
        days=30,
        min_score=10,
        topics=["testing"],
        limit=10,
    )

    assert [section.source_content_id for section in export.sections] == [included_id]
    assert export.topics == ["testing"]


def test_formats_stable_json_and_newsletter_markdown(db):
    content_id = _thread(
        db,
        content="TWEET 1: A useful thread\nTWEET 2: A concise newsletter summary.",
        score=11.5,
        topic="writing",
        tweet_id="markdown",
    )

    export = ThreadNewsletterSectionBuilder(db).build_export(days=30, min_score=1)
    payload = json.loads(export_to_json(export))
    markdown = format_markdown(export)

    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["sections"][0]["source_content_id"] == content_id
    assert "## 1. A useful thread" in markdown
    assert "A concise newsletter summary." in markdown
    assert "source_content_id=" + str(content_id) in markdown
    assert "https://x.com/taka/status/markdown" in markdown


def test_script_outputs_json_and_markdown(db, capsys):
    _thread(
        db,
        content="TWEET 1: Script thread\nTWEET 2: Script summary.",
        score=13.0,
        topic="scripts",
        tweet_id="script",
    )

    with patch.object(
        thread_newsletter_sections_script,
        "script_context",
        return_value=_script_context(db),
    ):
        exit_code = thread_newsletter_sections_script.main(
            ["--days", "30", "--min-score", "1", "--topic", "scripts", "--json"]
        )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "thread_newsletter_sections"
    assert payload["sections"][0]["headline"] == "Script thread"

    with patch.object(
        thread_newsletter_sections_script,
        "script_context",
        return_value=_script_context(db),
    ):
        thread_newsletter_sections_script.main(["--days", "30", "--markdown"])

    text = capsys.readouterr().out
    assert "# Thread Newsletter Sections" in text
    assert "Script summary." in text
