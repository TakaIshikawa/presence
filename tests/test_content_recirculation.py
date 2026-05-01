"""Tests for read-only content recirculation selection."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

from synthesis.content_recirculation import (
    build_content_recirculation_report,
    format_content_recirculation_json,
    format_content_recirculation_text,
)


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "select_recirculation_candidates.py"
)
spec = importlib.util.spec_from_file_location("select_recirculation_candidates", SCRIPT_PATH)
select_recirculation_candidates = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(select_recirculation_candidates)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(
    db,
    *,
    content_type: str = "x_post",
    text: str = "Resonated content",
    published_at: str | None = "2026-03-01T12:00:00+00:00",
    topic: str | None = "testing",
    eval_score: float = 7.0,
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=eval_score,
        eval_feedback="ok",
    )
    if published_at:
        db.conn.execute(
            """UPDATE generated_content
               SET published = 1, published_at = ?, published_url = ?
               WHERE id = ?""",
            (published_at, f"https://example.test/{content_id}", content_id),
        )
    if topic:
        db.insert_content_topics(content_id, [(topic, "", 1.0)])
    db.conn.commit()
    return content_id


def _publication(db, content_id: int, platform: str, published_at: str) -> None:
    db.conn.execute(
        """INSERT INTO content_publications
           (content_id, platform, status, platform_url, published_at)
           VALUES (?, ?, 'published', ?, ?)""",
        (content_id, platform, f"https://{platform}.example/{content_id}", published_at),
    )
    db.conn.commit()


def test_selector_excludes_unpublished_recent_publication_and_recent_reuse(db):
    old_id = _content(db, text="Old resonated X post")
    db.insert_engagement(old_id, "tweet-old", 10, 3, 2, 1, 42.0)
    _publication(db, old_id, "x", "2026-03-01T12:00:00+00:00")

    unpublished_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Draft",
        eval_score=9.0,
        eval_feedback="ok",
    )
    db.insert_engagement(unpublished_id, "tweet-draft", 50, 10, 5, 2, 100.0)

    recent_id = _content(
        db,
        text="Recent post",
        published_at="2026-04-25T12:00:00+00:00",
    )
    db.insert_engagement(recent_id, "tweet-recent", 50, 10, 5, 2, 100.0)

    reused_id = _content(db, text="Recently reused post")
    db.insert_engagement(reused_id, "tweet-reused", 40, 8, 4, 1, 90.0)
    variant_id = db.upsert_content_variant(reused_id, "x", "thread", "Recent thread")
    db.conn.execute(
        "UPDATE content_variants SET created_at = ? WHERE id = ?",
        ("2026-04-28T12:00:00+00:00", variant_id),
    )
    db.conn.commit()

    report = build_content_recirculation_report(
        db,
        days_old=30,
        lookback_days=14,
        now=NOW,
    )

    assert [candidate.content_id for candidate in report.candidates] == [old_id]
    assert report.totals["excluded_unpublished"] == 1
    assert report.totals["excluded_recent_publication"] == 1
    assert report.totals["excluded_recent_reuse"] == 1
    assert "thread" in report.candidates[0].recommended_formats
    assert "newsletter_section" in report.candidates[0].recommended_formats


def test_ranks_x_and_bluesky_candidates_by_engagement_age_and_topic_freshness(db):
    old_low = _content(
        db,
        text="Very old lower engagement",
        published_at="2026-01-01T12:00:00+00:00",
        topic="ops",
    )
    db.insert_engagement(old_low, "tweet-low", 1, 0, 0, 0, 5.0)

    x_id = _content(
        db,
        text="High-performing X lesson",
        published_at="2026-02-01T12:00:00+00:00",
        topic="architecture",
    )
    db.insert_engagement(x_id, "tweet-x", 20, 10, 3, 1, 20.0)

    bluesky_id = _content(
        db,
        content_type="bluesky_post",
        text="Strong Bluesky lesson",
        published_at="2026-02-15T12:00:00+00:00",
        topic="testing",
    )
    db.insert_bluesky_engagement(bluesky_id, "at://test/post/1", 30, 10, 2, 1, 65.0)
    _publication(db, bluesky_id, "bluesky", "2026-02-15T12:00:00+00:00")

    report = build_content_recirculation_report(db, days_old=30, now=NOW)

    assert [item.content_id for item in report.candidates[:2]] == [bluesky_id, x_id]
    assert report.candidates[0].engagement_by_platform == {"bluesky": 65.0}
    assert "bluesky_post" in report.candidates[0].recommended_formats
    assert report.candidates[1].score_components["engagement"] > 0


def test_newsletter_candidate_uses_send_metrics_and_recommends_section(db):
    content_id = _content(
        db,
        content_type="newsletter",
        text="Newsletter section that clicked",
        published_at=None,
        topic="release",
    )
    send_id = db.insert_newsletter_send(
        "issue-1",
        "Weekly notes",
        [content_id],
        subscriber_count=100,
    )
    db.conn.execute(
        "UPDATE newsletter_sends SET sent_at = ? WHERE id = ?",
        ("2026-03-10T12:00:00+00:00", send_id),
    )
    db.insert_newsletter_engagement(
        send_id,
        "issue-1",
        opens=50,
        clicks=12,
        unsubscribes=0,
        fetched_at="2026-03-11T12:00:00+00:00",
    )
    db.conn.commit()

    report = build_content_recirculation_report(db, days_old=30, now=NOW)
    candidate = report.candidates[0]

    assert candidate.content_id == content_id
    assert candidate.engagement_by_platform == {"newsletter": 19.6}
    assert candidate.published_at == "2026-03-10T12:00:00+00:00"
    assert "newsletter engagement score 19.60" in candidate.reasons
    assert "blog_seed" in candidate.recommended_formats


def test_no_engagement_fallback_still_returns_old_published_content(db):
    content_id = _content(
        db,
        text="Old useful post without imported metrics",
        published_at="2026-02-01T12:00:00+00:00",
        topic=None,
        eval_score=8.5,
    )

    report = build_content_recirculation_report(db, days_old=30, now=NOW)
    candidate = report.candidates[0]

    assert candidate.content_id == content_id
    assert candidate.engagement_score == 0.0
    assert candidate.score_components["quality_fallback"] == 16.5
    assert "no engagement snapshots" in "; ".join(candidate.reasons)


def test_json_text_and_cli_outputs_are_deterministic(db, capsys):
    content_id = _content(db, text="CLI candidate")
    db.insert_engagement(content_id, "tweet-cli", 4, 2, 1, 0, 18.0)

    report = build_content_recirculation_report(db, days_old=30, now=NOW)
    assert format_content_recirculation_json(report) == format_content_recirculation_json(report)
    payload = json.loads(format_content_recirculation_json(report))
    assert payload["filters"]["days_old"] == 30
    assert payload["candidates"][0]["content_id"] == content_id
    assert "Content Recirculation Selector" in format_content_recirculation_text(report)

    with patch.object(
        select_recirculation_candidates,
        "script_context",
        wraps=lambda: _script_context(db),
    ), patch.object(
        select_recirculation_candidates,
        "build_content_recirculation_report",
        wraps=lambda db, **kwargs: build_content_recirculation_report(
            db,
            now=NOW,
            **kwargs,
        ),
    ):
        assert select_recirculation_candidates.main(
            ["--days-old", "30", "--lookback-days", "14", "--limit", "5", "--json"]
        ) == 0

    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["filters"]["limit"] == 5
    assert cli_payload["candidates"][0]["content_id"] == content_id


def test_missing_required_table_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    try:
        report = build_content_recirculation_report(conn, now=NOW)
    finally:
        conn.close()

    assert report.candidates == ()
    assert "generated_content" in report.missing_tables


def test_rejects_invalid_filters(db):
    for kwargs, message in [
        ({"days_old": 0}, "days-old must be positive"),
        ({"lookback_days": 0}, "lookback-days must be positive"),
        ({"limit": 0}, "limit must be positive"),
    ]:
        try:
            build_content_recirculation_report(db, now=NOW, **kwargs)
        except ValueError as exc:
            assert message in str(exc)
        else:
            raise AssertionError("expected ValueError")
