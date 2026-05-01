"""Tests for deterministic newsletter section balancing."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from output.newsletter_section_balancer import (
    build_newsletter_section_balance_report,
    format_newsletter_section_balance_json,
    format_newsletter_section_balance_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "balance_newsletter_sections.py"
spec = importlib.util.spec_from_file_location("balance_newsletter_sections_script", SCRIPT_PATH)
balance_newsletter_sections_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(balance_newsletter_sections_script)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(
    db,
    *,
    content: str,
    content_type: str = "x_post",
    content_format: str | None = None,
    eval_score: float = 8.0,
    created_days_ago: int = 1,
    source_commits: list[str] | None = None,
    source_messages: list[str] | None = None,
    source_activity_ids: list[str] | None = None,
    topic: str | None = None,
    engagement_score: float | None = None,
    published: bool = True,
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=source_commits or [],
        source_messages=source_messages or [],
        source_activity_ids=source_activity_ids or [],
        content=content,
        eval_score=eval_score,
        eval_feedback="usable",
        content_format=content_format,
    )
    created_at = (NOW - timedelta(days=created_days_ago)).isoformat()
    db.conn.execute(
        "UPDATE generated_content SET created_at = ?, published = ?, published_at = ? WHERE id = ?",
        (created_at, 1 if published else 0, created_at if published else None, content_id),
    )
    if topic:
        db.insert_content_topics(content_id, [(topic, "", 0.9)])
    if engagement_score is not None:
        db.insert_engagement(
            content_id=content_id,
            tweet_id=f"tweet-{content_id}",
            like_count=1,
            retweet_count=0,
            reply_count=0,
            quote_count=0,
            engagement_score=engagement_score,
        )
        db.conn.execute(
            """UPDATE post_engagement
               SET fetched_at = ?
               WHERE id = (SELECT MAX(id) FROM post_engagement WHERE content_id = ?)""",
            (NOW.isoformat(), content_id),
        )
    db.conn.commit()
    return content_id


def test_balancer_selects_sections_ids_order_and_rationale(db):
    shipped_id = _content(
        db,
        content="Shipped the newsletter section planner with deterministic ordering.",
        content_format="update",
        eval_score=9.0,
        source_commits=["abc1234"],
        topic="newsletter",
        engagement_score=20.0,
    )
    lesson_id = _content(
        db,
        content="Lesson learned: layout variety matters more than one perfect post.",
        content_format="reflection",
        eval_score=8.0,
        source_messages=["msg-1"],
        topic="writing",
        engagement_score=12.0,
    )
    external_id = _content(
        db,
        content="Read more in the release notes: https://example.com/release",
        content_format="link",
        eval_score=7.0,
        source_activity_ids=["presence#7:issue"],
        topic="release",
        engagement_score=8.0,
    )

    report = build_newsletter_section_balance_report(db, days=7, max_items=3, now=NOW)

    assert report.selected_item_ids == (shipped_id, lesson_id, external_id)
    assert [item.section_label for item in report.items] == [
        "shipped work",
        "lesson learned",
        "external link",
    ]
    assert report.items[0].order == 1
    assert report.items[0].source_type == "commit"
    assert any("classified as shipped work" in reason for reason in report.items[0].rationale)


def test_repeated_formats_and_source_types_are_spread_when_alternatives_exist(db):
    first_id = _content(
        db,
        content="Shipped a queue recovery fix.",
        content_format="tip",
        eval_score=9.0,
        source_commits=["aaa1111"],
        topic="queue",
        engagement_score=20.0,
    )
    repeated_id = _content(
        db,
        content="Shipped another queue recovery fix.",
        content_format="tip",
        eval_score=8.8,
        source_commits=["bbb2222"],
        topic="queue",
        engagement_score=18.0,
    )
    alternative_id = _content(
        db,
        content="Lesson learned: keep retry metadata visible before debugging.",
        content_format="reflection",
        eval_score=8.7,
        source_messages=["msg-2"],
        topic="debugging",
        engagement_score=10.0,
    )

    report = build_newsletter_section_balance_report(db, days=7, max_items=3, now=NOW)

    assert report.selected_item_ids == (first_id, alternative_id, repeated_id)
    assert report.items[0].content_format != report.items[1].content_format
    assert report.items[0].source_type != report.items[1].source_type


def test_missing_publishable_text_is_excluded_with_reason(db):
    missing_id = db.conn.execute(
        """INSERT INTO generated_content
           (content_type, source_commits, source_messages, source_activity_ids,
            content, eval_score, eval_feedback, created_at)
           VALUES ('x_post', '[]', '[]', '[]', '', 8.0, 'empty', ?)""",
        ((NOW - timedelta(days=1)).isoformat(),),
    ).lastrowid
    included_id = _content(
        db,
        content="Try this checklist before publishing the newsletter.",
        eval_score=7.0,
        source_messages=["msg-3"],
        published=False,
    )
    db.conn.commit()

    report = build_newsletter_section_balance_report(db, days=7, max_items=5, now=NOW)

    assert report.selected_item_ids == (included_id,)
    assert report.excluded[0].content_id == missing_id
    assert report.excluded[0].reason == "missing_publishable_text"


def test_prefers_newsletter_variant_as_publishable_text(db):
    content_id = _content(
        db,
        content="Original short post.",
        eval_score=7.0,
        source_messages=["msg-4"],
    )
    db.upsert_content_variant(
        content_id,
        platform="newsletter",
        variant_type="summary",
        content="Newsletter-ready summary with more context.",
    )

    report = build_newsletter_section_balance_report(db, days=7, max_items=1, now=NOW)

    assert report.items[0].content_id == content_id
    assert report.items[0].text_source == "content_variants:newsletter:summary"
    assert "Newsletter-ready summary" in report.items[0].content_preview


def test_json_and_text_output_are_stable(db):
    content_id = _content(
        db,
        content="Reply with the section that should come first.",
        eval_score=7.0,
        source_messages=["msg-5"],
        engagement_score=5.0,
    )

    report = build_newsletter_section_balance_report(db, days=7, max_items=2, now=NOW)
    payload = json.loads(format_newsletter_section_balance_json(report))
    text = format_newsletter_section_balance_text(report)

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "newsletter_section_balance"
    assert payload["selected_item_ids"] == [content_id]
    assert payload["items"][0]["section_label"] == "call-to-action"
    assert "Newsletter Section Balance" in text
    assert "section=call-to-action" in text


def test_cli_supports_days_max_items_and_json_format(db, capsys):
    content_id = _content(
        db,
        content="Shipped CLI output for balanced newsletter sections.",
        eval_score=8.0,
        source_commits=["ccc3333"],
    )

    with patch.object(
        balance_newsletter_sections_script,
        "script_context",
        return_value=_script_context(db),
    ):
        exit_code = balance_newsletter_sections_script.main(
            ["--days", "7", "--max-items", "1", "--format", "json"]
        )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["filters"] == {"days": 7, "max_items": 1}
    assert payload["selected_item_ids"] == [content_id]

