import json

from evaluation.newsletter_click_attribution import (
    NewsletterClickAttribution,
    format_newsletter_click_attribution_json,
)


def _content(db, content_type: str, url: str | None = None) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=f"{content_type} content",
        eval_score=8.0,
        eval_feedback="ok",
    )
    if url:
        db.conn.execute(
            "UPDATE generated_content SET published_url = ? WHERE id = ?",
            (url, content_id),
        )
        db.conn.commit()
    return content_id


def test_newsletter_click_attribution_groups_by_content_topic_and_url(db):
    content_id = _content(db, "blog_post", "https://example.com/essay")
    db.insert_content_topics(content_id, [("ai-agents", "evals", 0.9)])
    send_id = db.insert_newsletter_send(
        issue_id="issue-clicks",
        subject="Clicks",
        content_ids=[content_id],
        subscriber_count=100,
    )
    db.insert_newsletter_link_clicks(
        newsletter_send_id=send_id,
        issue_id="issue-clicks",
        link_clicks=[
            {
                "url": "https://example.com/essay?utm_source=buttondown",
                "clicks": 3,
                "unique_clicks": 2,
            }
        ],
        fetched_at="2026-04-23T10:00:00+00:00",
    )

    summary = NewsletterClickAttribution(db).summarize(days=30)

    assert summary.total_clicks == 3
    assert summary.attributed_clicks == 3
    assert summary.unattributed_clicks == 0
    assert len(summary.by_content) == 1
    item = summary.by_content[0]
    assert item.content_id == content_id
    assert item.content_type == "blog_post"
    assert item.topic == "ai-agents"
    assert item.links[0].normalized_url == "https://example.com/essay"
    assert item.links[0].source_kind == "published_url"


def test_newsletter_click_attribution_keeps_unattributed_links_separate(db):
    send_id = db.insert_newsletter_send(
        issue_id="issue-unattributed",
        subject="External",
        content_ids=[],
        subscriber_count=100,
    )
    db.insert_newsletter_link_clicks(
        newsletter_send_id=send_id,
        issue_id="issue-unattributed",
        link_clicks=[
            {
                "url": "https://external.example.com/read?utm_campaign=weekly",
                "clicks": 5,
                "unique_clicks": 4,
            }
        ],
        fetched_at="2026-04-23T10:00:00+00:00",
    )

    summary = NewsletterClickAttribution(db).summarize(days=30)

    assert summary.by_content == []
    assert summary.unattributed_clicks == 5
    assert summary.unattributed_links[0].normalized_url == (
        "https://external.example.com/read"
    )


def test_newsletter_click_attribution_uses_latest_snapshot_per_send_link(db):
    content_id = _content(db, "x_post", "https://example.com/post")
    send_id = db.insert_newsletter_send(
        issue_id="issue-latest",
        subject="Latest",
        content_ids=[content_id],
        subscriber_count=100,
    )
    db.insert_newsletter_link_clicks(
        send_id,
        "issue-latest",
        [{"url": "https://example.com/post", "clicks": 1, "unique_clicks": 1}],
        fetched_at="2026-04-23T10:00:00+00:00",
    )
    db.insert_newsletter_link_clicks(
        send_id,
        "issue-latest",
        [{"url": "https://example.com/post", "clicks": 7, "unique_clicks": 3}],
        fetched_at="2026-04-23T12:00:00+00:00",
    )

    summary = NewsletterClickAttribution(db).summarize(days=30)

    assert summary.total_clicks == 7
    assert summary.by_content[0].links[0].latest_fetched_at == (
        "2026-04-23T12:00:00+00:00"
    )


def test_newsletter_click_attribution_json_is_stable(db):
    content_id = _content(db, "x_thread", "https://example.com/thread")
    db.insert_content_topics(content_id, [("testing", "", 0.8)])
    send_id = db.insert_newsletter_send(
        issue_id="issue-json",
        subject="JSON",
        content_ids=[content_id],
    )
    db.insert_newsletter_link_clicks(
        send_id,
        "issue-json",
        [{"url": "https://example.com/thread", "clicks": 2}],
        fetched_at="2026-04-23T10:00:00+00:00",
    )

    summary = NewsletterClickAttribution(db).summarize(days=30)
    payload = format_newsletter_click_attribution_json(summary)
    parsed = json.loads(payload)

    assert payload == format_newsletter_click_attribution_json(summary)
    assert list(parsed) == sorted(parsed)
    assert parsed["by_content"][0]["content_id"] == content_id
    assert parsed["by_content"][0]["links"][0]["normalized_url"] == (
        "https://example.com/thread"
    )
