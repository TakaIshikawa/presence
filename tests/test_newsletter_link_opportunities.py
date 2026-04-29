"""Tests for newsletter link opportunity reporting."""

import importlib.util
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from contextlib import contextmanager
from unittest.mock import patch

from evaluation.newsletter_link_opportunities import (
    NewsletterLinkOpportunityAnalyzer,
    score_link_opportunity_components,
)


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "newsletter_link_opportunities.py"
)
spec = importlib.util.spec_from_file_location("newsletter_link_opportunities_script", SCRIPT_PATH)
newsletter_link_opportunities = importlib.util.module_from_spec(spec)
spec.loader.exec_module(newsletter_link_opportunities)


def _content(db, content, content_type="blog_post", url="https://example.com/post"):
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=8.0,
        eval_feedback="ok",
    )
    published_at = (datetime.now(timezone.utc) - timedelta(days=45)).isoformat()
    db.conn.execute(
        """UPDATE generated_content
           SET published = 1, published_url = ?, published_at = ?
           WHERE id = ?""",
        (url, published_at, content_id),
    )
    db.conn.commit()
    return content_id


def _send_with_link(
    db,
    *,
    issue_id,
    content_ids,
    url,
    clicks,
    raw_url=None,
    subscriber_count=100,
    fetched_at=None,
):
    send_id = db.insert_newsletter_send(
        issue_id=issue_id,
        subject="Weekly",
        content_ids=content_ids,
        subscriber_count=subscriber_count,
    )
    fetched_at = fetched_at or datetime.now(timezone.utc).isoformat()
    db.insert_newsletter_engagement(
        newsletter_send_id=send_id,
        issue_id=issue_id,
        opens=40,
        clicks=clicks,
        unsubscribes=0,
        fetched_at=fetched_at,
    )
    db.insert_newsletter_link_clicks(
        newsletter_send_id=send_id,
        issue_id=issue_id,
        link_clicks=[{"url": url, "raw_url": raw_url, "clicks": clicks}],
        fetched_at=fetched_at,
    )
    return send_id


@contextmanager
def _script_context(db):
    yield None, db


def test_score_components_are_transparent_and_penalize_existing_follow_up():
    without_follow_up = score_link_opportunity_components(
        clicks=10,
        ctr=0.1,
        content_age_days=90,
        has_follow_up_posts=False,
    )
    with_follow_up = score_link_opportunity_components(
        clicks=10,
        ctr=0.1,
        content_age_days=90,
        has_follow_up_posts=True,
    )

    assert without_follow_up == {
        "clicks": 16.0,
        "ctr": 15.0,
        "content_age": 7.5,
        "no_existing_follow_up": 15.0,
    }
    assert with_follow_up["no_existing_follow_up"] == 0.0


def test_summarize_ranks_links_and_includes_json_fields(db):
    low_id = _content(db, "TITLE: Low interest\nBody", url="https://example.com/low")
    high_id = _content(db, "TITLE: Follow-up worthy\nBody", url="https://example.com/high")
    _send_with_link(
        db,
        issue_id="issue-low",
        content_ids=[low_id],
        url="https://example.com/low",
        clicks=3,
    )
    send_id = _send_with_link(
        db,
        issue_id="issue-high",
        content_ids=[high_id],
        url="https://example.com/high",
        clicks=12,
    )

    summary = NewsletterLinkOpportunityAnalyzer(db).summarize(days=30, limit=10)

    assert summary.opportunity_count == 2
    best = summary.opportunities[0]
    assert best.newsletter_send_id == send_id
    assert best.url == "https://example.com/high"
    assert best.title == "Follow-up worthy"
    assert best.clicks == 12
    assert best.ctr == 0.12
    assert best.score > summary.opportunities[1].score
    assert set(best.score_components) == {
        "clicks",
        "ctr",
        "content_age",
        "no_existing_follow_up",
    }
    assert best.suggested_follow_up_angle

    payload = json.loads(newsletter_link_opportunities.format_json_report(summary))
    payload_best = payload["opportunities"][0]
    assert payload_best["newsletter_send_id"] == send_id
    assert payload_best["url"] == "https://example.com/high"
    assert payload_best["title"] == "Follow-up worthy"
    assert payload_best["clicks"] == 12
    assert payload_best["ctr"] == 0.12
    assert payload_best["score"] == best.score
    assert payload_best["suggested_follow_up_angle"]


def test_min_clicks_excludes_low_click_links(db):
    low_id = _content(db, "Low", url="https://example.com/low")
    high_id = _content(db, "High", url="https://example.com/high")
    _send_with_link(
        db,
        issue_id="issue-low",
        content_ids=[low_id],
        url="https://example.com/low",
        clicks=2,
    )
    _send_with_link(
        db,
        issue_id="issue-high",
        content_ids=[high_id],
        url="https://example.com/high",
        clicks=5,
    )

    summary = NewsletterLinkOpportunityAnalyzer(db).summarize(
        days=30,
        min_clicks=3,
    )

    assert [item.url for item in summary.opportunities] == ["https://example.com/high"]


def test_excluded_domains_filter_before_scoring_and_limit(db):
    utility_id = _content(
        db,
        "Utility",
        url="https://utility.example.com/archive",
    )
    retained_id = _content(db, "Retained", url="https://reader.example.net/post")
    _send_with_link(
        db,
        issue_id="issue-utility",
        content_ids=[utility_id],
        url="https://utility.example.com/archive",
        clicks=20,
    )
    _send_with_link(
        db,
        issue_id="issue-retained",
        content_ids=[retained_id],
        url="https://reader.example.net/post",
        clicks=4,
    )

    summary = NewsletterLinkOpportunityAnalyzer(db).summarize(
        days=30,
        limit=1,
        excluded_domains=["utility.example.com"],
    )

    assert summary.excluded_domains == ["utility.example.com"]
    assert summary.opportunity_count == 1
    assert [item.url for item in summary.opportunities] == [
        "https://reader.example.net/post"
    ]


def test_excluded_domains_match_case_insensitively(db):
    excluded_id = _content(db, "Self promo", url="https://Self.Example.com/post")
    retained_id = _content(db, "Partner", url="https://partner.example.com/post")
    _send_with_link(
        db,
        issue_id="issue-self",
        content_ids=[excluded_id],
        url="https://Self.Example.com/post",
        clicks=8,
    )
    _send_with_link(
        db,
        issue_id="issue-partner",
        content_ids=[retained_id],
        url="https://partner.example.com/post",
        clicks=6,
    )

    summary = NewsletterLinkOpportunityAnalyzer(db).summarize(
        days=30,
        excluded_domains=["SELF.example.COM"],
    )

    assert summary.excluded_domains == ["self.example.com"]
    assert [item.url for item in summary.opportunities] == [
        "https://partner.example.com/post"
    ]


def test_excluded_domains_filter_by_raw_url_fallback(db):
    tracking_id = _content(db, "Tracked", url="https://tracking.example.com/click")
    retained_id = _content(db, "Untracked", url="https://news.example.org/story")
    _send_with_link(
        db,
        issue_id="issue-tracked",
        content_ids=[tracking_id],
        url="https://tracking.example.com/click",
        raw_url="https://Owned.Example.com/post?utm_source=newsletter",
        clicks=9,
    )
    _send_with_link(
        db,
        issue_id="issue-untracked",
        content_ids=[retained_id],
        url="https://news.example.org/story",
        raw_url="https://news.example.org/story?utm_source=newsletter",
        clicks=5,
    )

    summary = NewsletterLinkOpportunityAnalyzer(db).summarize(
        days=30,
        excluded_domains=["owned.example.com"],
    )

    assert [item.url for item in summary.opportunities] == [
        "https://news.example.org/story"
    ]


def test_handles_missing_newsletter_metrics_without_crashing(db):
    content_id = _content(db, "Metric free", url="https://example.com/metric-free")
    send_id = db.insert_newsletter_send(
        issue_id="issue-missing",
        subject="Weekly",
        content_ids=[content_id],
        subscriber_count=0,
    )
    db.insert_newsletter_link_clicks(
        newsletter_send_id=send_id,
        issue_id="issue-missing",
        link_clicks=[{"url": "https://example.com/metric-free", "clicks": 4}],
    )

    summary = NewsletterLinkOpportunityAnalyzer(db).summarize(days=30)

    assert summary.opportunity_count == 1
    assert summary.opportunities[0].ctr is None
    assert summary.opportunities[0].score_components["ctr"] == 0.0


def test_existing_follow_up_reduces_score(db):
    source_id = _content(db, "Already followed", url="https://example.com/source")
    follow_up_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Follow-up post",
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET repurposed_from = ? WHERE id = ?",
        (source_id, follow_up_id),
    )
    db.conn.commit()
    _send_with_link(
        db,
        issue_id="issue-followed",
        content_ids=[source_id],
        url="https://example.com/source",
        clicks=8,
    )

    summary = NewsletterLinkOpportunityAnalyzer(db).summarize(days=30)

    assert summary.opportunities[0].has_follow_up_posts is True
    assert summary.opportunities[0].score_components["no_existing_follow_up"] == 0.0


def test_script_supports_json_flag_and_limit(db, capsys):
    content_id = _content(db, "Script link", url="https://example.com/script")
    _send_with_link(
        db,
        issue_id="issue-script",
        content_ids=[content_id],
        url="https://example.com/script",
        clicks=5,
    )

    with patch.object(
        newsletter_link_opportunities,
        "script_context",
        return_value=_script_context(db),
    ):
        newsletter_link_opportunities.main(["--json", "--limit", "1", "--min-clicks", "1"])

    payload = json.loads(capsys.readouterr().out)
    assert payload["opportunity_count"] == 1
    assert payload["opportunities"][0]["url"] == "https://example.com/script"


def test_script_supports_repeatable_exclude_domain(db, capsys):
    excluded_id = _content(db, "Script excluded", url="https://utility.example.com/post")
    retained_id = _content(db, "Script retained", url="https://reader.example.net/post")
    _send_with_link(
        db,
        issue_id="issue-script-excluded",
        content_ids=[excluded_id],
        url="https://utility.example.com/post",
        clicks=10,
    )
    _send_with_link(
        db,
        issue_id="issue-script-retained",
        content_ids=[retained_id],
        url="https://reader.example.net/post",
        clicks=4,
    )

    with patch.object(
        newsletter_link_opportunities,
        "script_context",
        return_value=_script_context(db),
    ):
        newsletter_link_opportunities.main(
            [
                "--json",
                "--exclude-domain",
                "UTILITY.example.com",
                "--exclude-domain",
                "unused.example.org",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert payload["excluded_domains"] == [
        "unused.example.org",
        "utility.example.com",
    ]
    assert [item["url"] for item in payload["opportunities"]] == [
        "https://reader.example.net/post"
    ]
