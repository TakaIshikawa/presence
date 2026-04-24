"""Tests for newsletter link-click attribution reports."""

import importlib.util
import json
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from evaluation.newsletter_link_performance import NewsletterLinkPerformance


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "newsletter_link_performance.py"
)
spec = importlib.util.spec_from_file_location(
    "newsletter_link_performance_script", SCRIPT_PATH
)
newsletter_link_performance_script = importlib.util.module_from_spec(spec)
spec.loader.exec_module(newsletter_link_performance_script)


def _content(db, content_type, url, body="Body"):
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=body,
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET published = 1, published_url = ? WHERE id = ?",
        (url, content_id),
    )
    db.conn.commit()
    return content_id


@contextmanager
def _script_context(db):
    yield object(), db


def test_report_ranks_clicked_urls_and_maps_to_source_content(db):
    blog_id = _content(
        db,
        "blog_post",
        "https://Example.com/post?utm_source=old&ref=archive",
        body="# Post",
    )
    post_id = _content(db, "x_post", "https://example.com/post-two")
    send_id = db.insert_newsletter_send(
        issue_id="issue-1",
        subject="Weekly",
        content_ids=[blog_id, post_id],
        subscriber_count=100,
    )
    db.insert_newsletter_link_clicks(
        newsletter_send_id=send_id,
        issue_id="issue-1",
        link_clicks=[
            {
                "url": "https://example.com/post-two",
                "clicks": 3,
                "unique_clicks": 2,
            },
            {
                "url": "https://example.com/post?ref=archive",
                "raw_url": (
                    "https://example.com/post?utm_campaign=weekly"
                    "&content_id=%s&ref=archive" % blog_id
                ),
                "clicks": 8,
                "unique_clicks": 4,
            },
        ],
        fetched_at="2026-04-20T10:00:00+00:00",
    )

    report = NewsletterLinkPerformance(db).summarize(days=30, limit=10)

    assert report.total_clicks == 11
    assert report.mapped_clicks == 11
    assert report.unmapped_clicks == 0
    assert [item.url for item in report.ranked_urls] == [
        "https://example.com/post?ref=archive",
        "https://example.com/post-two",
    ]
    top = report.ranked_urls[0]
    assert top.content_id == blog_id
    assert top.content_type == "blog_post"
    assert top.section == "This Week's Post"
    assert report.by_content[0].content_id == blog_id
    assert report.by_content_type[0].content_type == "blog_post"
    assert report.by_issue[0].mapped_clicks == 11


def test_report_counts_unmapped_and_malformed_send_metadata(db):
    send_id = db.insert_newsletter_send(
        issue_id="issue-bad",
        subject="Weekly",
        content_ids=[],
        subscriber_count=50,
    )
    db.conn.execute(
        "UPDATE newsletter_sends SET source_content_ids = ? WHERE id = ?",
        ("not-json", send_id),
    )
    db.conn.commit()
    db.insert_newsletter_link_clicks(
        newsletter_send_id=send_id,
        issue_id="issue-bad",
        link_clicks=[
            {
                "url": "https://example.com/outside?utm_source=buttondown",
                "clicks": 5,
            }
        ],
        fetched_at="2026-04-20T10:00:00+00:00",
    )

    report = NewsletterLinkPerformance(db).summarize(days=30)

    assert report.total_clicks == 5
    assert report.unmapped_clicks == 5
    assert report.unmapped_link_count == 1
    assert report.malformed_send_count == 1
    assert report.ranked_urls[0].attribution_status == "unmapped"


def test_report_marks_urls_matching_multiple_source_items_as_ambiguous(db):
    first_id = _content(db, "x_post", "https://example.com/shared")
    second_id = _content(db, "x_thread", "https://example.com/shared")
    send_id = db.insert_newsletter_send(
        issue_id="issue-shared",
        subject="Weekly",
        content_ids=[first_id, second_id],
        subscriber_count=100,
    )
    db.insert_newsletter_link_clicks(
        newsletter_send_id=send_id,
        issue_id="issue-shared",
        link_clicks=[{"url": "https://example.com/shared", "clicks": 6}],
        fetched_at="2026-04-20T10:00:00+00:00",
    )

    report = NewsletterLinkPerformance(db).summarize(days=30)

    assert report.ambiguous_clicks == 6
    assert report.ambiguous_link_count == 1
    assert report.ranked_urls[0].attribution_status == "ambiguous"
    assert report.by_content == []


def test_report_uses_latest_link_snapshot_per_send_and_url(db):
    content_id = _content(db, "x_post", "https://example.com/latest")
    send_id = db.insert_newsletter_send(
        issue_id="issue-latest",
        subject="Weekly",
        content_ids=[content_id],
        subscriber_count=100,
    )
    db.insert_newsletter_link_clicks(
        newsletter_send_id=send_id,
        issue_id="issue-latest",
        link_clicks=[{"url": "https://example.com/latest", "clicks": 1}],
        fetched_at="2026-04-20T10:00:00+00:00",
    )
    db.insert_newsletter_link_clicks(
        newsletter_send_id=send_id,
        issue_id="issue-latest",
        link_clicks=[{"url": "https://example.com/latest", "clicks": 4}],
        fetched_at="2026-04-21T10:00:00+00:00",
    )

    report = NewsletterLinkPerformance(db).summarize(days=30)

    assert report.total_clicks == 4
    assert report.ranked_urls[0].clicks == 4


def test_script_outputs_stable_json(db, capsys):
    content_id = _content(db, "x_post", "https://example.com/json")
    send_id = db.insert_newsletter_send(
        issue_id="issue-json",
        subject="Weekly",
        content_ids=[content_id],
        subscriber_count=100,
    )
    db.insert_newsletter_link_clicks(
        newsletter_send_id=send_id,
        issue_id="issue-json",
        link_clicks=[{"url": "https://example.com/json", "clicks": 2}],
        fetched_at="2026-04-20T10:00:00+00:00",
    )

    with patch.object(
        newsletter_link_performance_script,
        "script_context",
        return_value=_script_context(db),
    ):
        newsletter_link_performance_script.main(
            ["--days", "30", "--issue-id", "issue-json", "--json", "--limit", "5"]
        )

    payload = json.loads(capsys.readouterr().out)
    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["issue_id"] == "issue-json"
    assert payload["ranked_urls"][0]["content_id"] == content_id
    assert payload["unmapped_link_count"] == 0
