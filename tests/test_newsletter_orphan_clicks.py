"""Tests for newsletter orphan click reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import json

from evaluation.newsletter_orphan_clicks import (
    build_newsletter_orphan_clicks_report,
    format_newsletter_orphan_clicks_json,
    format_newsletter_orphan_clicks_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)


def _send(db, metadata=None) -> int:
    send_id = db.insert_newsletter_send(
        issue_id="issue-1",
        subject="Subject",
        content_ids=[],
        subscriber_count=100,
        metadata=metadata,
    )
    db.conn.execute(
        "UPDATE newsletter_sends SET sent_at = ? WHERE id = ?",
        ("2026-05-01T00:00:00+00:00", send_id),
    )
    db.conn.commit()
    return int(send_id)


def _click(db, send_id: int, url: str, *, clicks: int, content_id=None, source_kind=None) -> int:
    db.conn.execute(
        """INSERT INTO newsletter_link_clicks
           (newsletter_send_id, issue_id, link_url, clicks, content_id, source_kind, fetched_at)
           VALUES (?, 'issue-1', ?, ?, ?, ?, ?)""",
        (send_id, url, clicks, content_id, source_kind, "2026-05-01T12:00:00+00:00"),
    )
    db.conn.commit()
    return int(db.conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def test_groups_orphan_clicks_by_normalized_url_and_domain(db):
    send_id = _send(db)
    first = _click(db, send_id, "https://Example.com/path?utm_source=n", clicks=2)
    second = _click(db, send_id, "https://example.com/path", clicks=3)
    _click(db, send_id, "https://noise.example/once", clicks=1)

    report = build_newsletter_orphan_clicks_report(
        db,
        min_click_count=2,
        now=NOW,
    )

    assert len(report.groups) == 1
    group = report.groups[0]
    assert group.normalized_url == "https://example.com/path"
    assert group.domain == "example.com"
    assert group.click_count == 5
    assert group.event_count == 2
    assert group.sample_event_ids == (first, second)


def test_excludes_known_content_metadata_and_alias_matches(db):
    content_id = db.insert_generated_content(
        content_type="blog_post",
        source_commits=[],
        source_messages=[],
        content="Known",
        eval_score=8,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET published_url = ? WHERE id = ?",
        ("https://known.example/post", content_id),
    )
    send_id = _send(db, metadata={"sections": [{"url": "https://section.example/a"}]})
    _click(db, send_id, "https://known.example/post?utm_campaign=x", clicks=5)
    _click(db, send_id, "https://section.example/a", clicks=5)
    _click(db, send_id, "https://attributed.example/a", clicks=5, content_id=content_id)
    _click(db, send_id, "https://section-link.example/a", clicks=5, source_kind="section")
    _click(db, send_id, "https://alias.example/a", clicks=5)

    report = build_newsletter_orphan_clicks_report(
        db,
        known_url_aliases={"https://alias.example/a": "https://known.example/post"},
        min_click_count=2,
        now=NOW,
    )

    assert report.groups == ()


def test_minimum_click_count_and_formatters(db):
    send_id = _send(db)
    _click(db, send_id, "https://orphan.example/a", clicks=4)
    _click(db, send_id, "https://orphan.example/b", clicks=1)

    report = build_newsletter_orphan_clicks_report(db, min_click_count=2, now=NOW)
    payload = json.loads(format_newsletter_orphan_clicks_json(report))
    text = format_newsletter_orphan_clicks_text(report)

    assert payload["artifact_type"] == "newsletter_orphan_clicks"
    assert payload["totals"]["group_count"] == 1
    assert payload["groups"][0]["normalized_url"] == "https://orphan.example/a"
    assert "orphan_clicks=4" in text
