"""Tests for newsletter link-click attribution."""

import importlib.util
import json
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from evaluation.newsletter_link_attribution import NewsletterLinkAttribution


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "newsletter_link_attribution.py"
)
spec = importlib.util.spec_from_file_location("newsletter_link_attribution_script", SCRIPT_PATH)
newsletter_link_attribution_script = importlib.util.module_from_spec(spec)
spec.loader.exec_module(newsletter_link_attribution_script)


@contextmanager
def _script_context(config, db):
    yield config, db


def _published_content(db, content_type: str, url: str | None) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=f"{content_type} body",
        eval_score=8.0,
        eval_feedback="good",
    )
    db.conn.execute(
        "UPDATE generated_content SET published = 1, published_url = ? WHERE id = ?",
        (url, content_id),
    )
    db.conn.commit()
    return content_id


def test_attributes_latest_link_clicks_to_source_content(db):
    post_id = _published_content(db, "x_post", "https://example.com/post")
    blog_id = _published_content(db, "blog_post", "https://example.com/blog")
    send_id = db.insert_newsletter_send(
        issue_id="issue-1",
        subject="Weekly",
        content_ids=[post_id, blog_id],
        subscriber_count=100,
    )
    db.insert_newsletter_link_clicks(
        newsletter_send_id=send_id,
        issue_id="issue-1",
        link_clicks=[
            {"url": "https://example.com/post", "clicks": 1, "unique_clicks": 1},
            {"url": "https://outside.example/read", "clicks": 3},
        ],
        fetched_at="2026-04-24T01:00:00+00:00",
    )
    db.insert_newsletter_link_clicks(
        newsletter_send_id=send_id,
        issue_id="issue-1",
        link_clicks=[
            {"url": "https://example.com/post", "clicks": 5, "unique_clicks": 4},
            {"url": "https://example.com/blog", "clicks": 2, "unique_clicks": 2},
        ],
        fetched_at="2026-04-24T02:00:00+00:00",
    )

    report = NewsletterLinkAttribution(db).summarize(days=90)

    assert [(item.content_id, item.clicks) for item in report.attributed_content] == [
        (post_id, 5),
        (blog_id, 2),
    ]
    assert report.attributed_content[0].content_type == "x_post"
    assert report.attributed_content[0].issue_id == "issue-1"
    assert report.attributed_content[0].unique_clicks == 4
    assert report.unmatched_links == []


def test_matches_links_after_stripping_utm_parameters(db):
    content_id = _published_content(db, "blog_post", "https://example.com/blog/post")
    send_id = db.insert_newsletter_send(
        issue_id="issue-utm",
        subject="Weekly",
        content_ids=[content_id],
        subscriber_count=100,
    )
    db.insert_newsletter_link_clicks(
        newsletter_send_id=send_id,
        issue_id="issue-utm",
        link_clicks=[
            {
                "url": "https://example.com/blog/post?utm_source=newsletter&utm_campaign=w",
                "clicks": 7,
            }
        ],
        fetched_at="2026-04-24T02:00:00+00:00",
    )

    report = NewsletterLinkAttribution(db).summarize(issue_id="issue-utm")

    assert len(report.attributed_content) == 1
    assert report.attributed_content[0].content_id == content_id
    assert report.attributed_content[0].clicks == 7
    assert report.unmatched_links == []


def test_uses_newsletter_body_urls_from_send_metadata(db):
    content_id = _published_content(db, "x_thread", "https://x.com/taka/status/1")
    send_id = db.insert_newsletter_send(
        issue_id="issue-body-url",
        subject="Weekly",
        content_ids=[content_id],
        subscriber_count=100,
        metadata={
            "body_urls": [
                {
                    "content_id": content_id,
                    "url": "https://example.com/archive/thread-1?utm_medium=email",
                }
            ]
        },
    )
    db.insert_newsletter_link_clicks(
        newsletter_send_id=send_id,
        issue_id="issue-body-url",
        link_clicks=[
            {
                "url": "https://example.com/archive/thread-1?utm_medium=email",
                "clicks": 6,
            }
        ],
        fetched_at="2026-04-24T02:00:00+00:00",
    )

    report = NewsletterLinkAttribution(db).summarize(issue_id="issue-body-url")

    assert [(item.content_id, item.clicks) for item in report.attributed_content] == [
        (content_id, 6)
    ]
    assert report.unmatched_links == []


def test_json_output_includes_unmatched_links(db):
    content_id = _published_content(db, "x_post", "https://example.com/post")
    send_id = db.insert_newsletter_send(
        issue_id="issue-json",
        subject="Weekly",
        content_ids=[content_id],
    )
    db.insert_newsletter_link_clicks(
        newsletter_send_id=send_id,
        issue_id="issue-json",
        link_clicks=[{"url": "https://unmatched.example/post", "clicks": 9}],
        fetched_at="2026-04-24T02:00:00+00:00",
    )

    report = NewsletterLinkAttribution(db).summarize(issue_id="issue-json")
    payload = json.loads(newsletter_link_attribution_script.format_json_report(report))

    assert payload["attributed_content"] == []
    assert payload["unmatched_links"][0]["url"] == "https://unmatched.example/post"
    assert payload["unmatched_links"][0]["clicks"] == 9


def test_text_output_ranks_attributed_content_by_clicks(db):
    first_id = _published_content(db, "x_post", "https://example.com/first")
    second_id = _published_content(db, "blog_post", "https://example.com/second")
    send_id = db.insert_newsletter_send(
        issue_id="issue-rank",
        subject="Weekly",
        content_ids=[first_id, second_id],
    )
    db.insert_newsletter_link_clicks(
        newsletter_send_id=send_id,
        issue_id="issue-rank",
        link_clicks=[
            {"url": "https://example.com/first", "clicks": 2},
            {"url": "https://example.com/second", "clicks": 8},
        ],
        fetched_at="2026-04-24T02:00:00+00:00",
    )

    report = NewsletterLinkAttribution(db).summarize(issue_id="issue-rank")
    text = newsletter_link_attribution_script.format_text_report(report)

    assert text.index(f"1. Content {second_id}") < text.index(f"2. Content {first_id}")


def test_script_supports_issue_days_and_json_output(db, capsys):
    content_id = _published_content(db, "x_post", "https://example.com/post")
    send_id = db.insert_newsletter_send(
        issue_id="issue-cli",
        subject="Weekly",
        content_ids=[content_id],
    )
    db.insert_newsletter_link_clicks(
        newsletter_send_id=send_id,
        issue_id="issue-cli",
        link_clicks=[{"url": "https://example.com/post", "clicks": 4}],
        fetched_at="2026-04-24T02:00:00+00:00",
    )

    with patch.object(
        newsletter_link_attribution_script,
        "script_context",
        return_value=_script_context(SimpleNamespace(), db),
    ):
        newsletter_link_attribution_script.main(
            ["--days", "30", "--issue-id", "issue-cli", "--json"]
        )

    payload = json.loads(capsys.readouterr().out)
    assert payload["period_days"] == 30
    assert payload["issue_id"] == "issue-cli"
    assert payload["attributed_content"][0]["content_id"] == content_id
