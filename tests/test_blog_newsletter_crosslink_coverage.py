"""Tests for blog/newsletter cross-link coverage reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.blog_newsletter_crosslink_coverage import (
    build_blog_newsletter_crosslink_coverage_report,
    build_blog_newsletter_crosslink_coverage_report_from_db,
    format_blog_newsletter_crosslink_coverage_text,
)


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "blog_newsletter_crosslink_coverage.py"
spec = importlib.util.spec_from_file_location("blog_newsletter_crosslink_coverage_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_reports_missing_directions_and_coverage_rates():
    blogs = [
        {
            "id": 10,
            "title": "Evaluation agents need source evidence",
            "url": "https://example.test/blog/source-evidence",
            "published_at": (NOW - timedelta(days=2)).isoformat(),
            "source_commits": json.dumps(["abc"]),
            "content": "Read issue may-15 for the background.",
        },
        {
            "id": 20,
            "title": "Unrelated launch notes",
            "url": "https://example.test/blog/unrelated",
            "published_at": (NOW - timedelta(days=30)).isoformat(),
        },
    ]
    newsletters = [
        {
            "id": 7,
            "issue_id": "may-15",
            "subject": "Source evidence for evaluation agents",
            "sent_at": NOW.isoformat(),
            "source_content_ids": json.dumps([10]),
            "body": "This issue should point to the blog but does not.",
        }
    ]

    report = build_blog_newsletter_crosslink_coverage_report(blogs, newsletters, now=NOW)

    assert report["totals"]["candidate_pair_count"] == 1
    pair = report["pairs"][0]
    assert pair["blog_id"] == "10"
    assert pair["blog_links_newsletter"] is True
    assert pair["newsletter_links_blog"] is False
    assert pair["missing_directions"] == ["newsletter_to_blog"]
    assert "shared_source_ids" in pair["match_reason"]
    assert pair["publication_date_gap_days"] == 2.0
    assert report["totals"]["newsletter_to_blog_coverage_rate"] == 0.0
    assert report["totals"]["blog_to_newsletter_coverage_rate"] == 1.0
    assert "Pairs:" in format_blog_newsletter_crosslink_coverage_text(report)


def test_detects_cross_links_in_both_directions_by_url_and_issue_id():
    blogs = [
        {
            "id": "post-a",
            "title": "Prompt version testing",
            "url": "https://example.test/blog/prompt-version-testing",
            "published_at": NOW.isoformat(),
            "content": "Newsletter archive: issue-44",
        }
    ]
    newsletters = [
        {
            "id": "send-a",
            "issue_id": "issue-44",
            "subject": "Prompt version testing notes",
            "sent_at": NOW.isoformat(),
            "body": "Read https://example.test/blog/prompt-version-testing",
        }
    ]

    report = build_blog_newsletter_crosslink_coverage_report(blogs, newsletters, now=NOW)

    assert report["pairs"][0]["missing_directions"] == []
    assert report["totals"]["newsletter_to_blog_coverage_rate"] == 1.0
    assert report["totals"]["blog_to_newsletter_coverage_rate"] == 1.0


def test_db_loader_and_cli_json_output(db, monkeypatch, capsys):
    content_id = db.insert_generated_content(
        content_type="blog_post",
        source_commits=["abc"],
        source_messages=[],
        content="Evidence coverage report",
        eval_score=8,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET published = 1, published_url = ?, published_at = ? WHERE id = ?",
        ("https://example.test/blog/evidence-coverage", (NOW - timedelta(days=1)).isoformat(), content_id),
    )
    db.conn.execute(
        """INSERT INTO newsletter_sends (issue_id, subject, source_content_ids, metadata, sent_at)
           VALUES (?, ?, ?, ?, ?)""",
        (
            "issue-1",
            "Evidence coverage report",
            json.dumps([content_id]),
            json.dumps({"body": "No blog link yet"}),
            NOW.isoformat(),
        ),
    )
    db.conn.commit()

    report = build_blog_newsletter_crosslink_coverage_report_from_db(db, now=NOW)
    assert report["pairs"][0]["blog_id"] == str(content_id)
    assert report["pairs"][0]["missing_directions"] == ["newsletter_to_blog", "blog_to_newsletter"]

    monkeypatch.setattr(script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        script,
        "build_blog_newsletter_crosslink_coverage_report_from_db",
        lambda db, **kwargs: build_blog_newsletter_crosslink_coverage_report_from_db(db, now=NOW, **kwargs),
    )
    assert script.main(["--limit", "5"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "blog_newsletter_crosslink_coverage"

    assert script.main(["--table"]) == 0
    assert "Blog Newsletter Crosslink Coverage" in capsys.readouterr().out
