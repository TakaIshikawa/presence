"""Tests for newsletter source mix reporting."""

import importlib.util
import json
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from evaluation.newsletter_source_mix import NewsletterSourceMix


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "newsletter_source_mix.py"
)
spec = importlib.util.spec_from_file_location("newsletter_source_mix", SCRIPT_PATH)
newsletter_source_mix = importlib.util.module_from_spec(spec)
spec.loader.exec_module(newsletter_source_mix)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_content(db, content_type="x_post", topic="ai", backed=True):
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=f"{content_type} about {topic}",
        eval_score=8.0,
        eval_feedback="good",
    )
    db.insert_content_topics(content_id, [(topic, None, 1.0)])
    if backed:
        knowledge_id = db.conn.execute(
            """INSERT INTO knowledge (source_type, source_id, content, approved)
               VALUES (?, ?, ?, ?)""",
            ("curated_article", f"source-{content_id}", "source note", 1),
        ).lastrowid
        db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.9)])
    return content_id


def test_analyzer_returns_mixed_send_composition(db):
    x_post = _insert_content(db, "x_post", "ai", backed=True)
    thread = _insert_content(db, "x_thread", "testing", backed=True)
    blog = _insert_content(db, "blog_post", "architecture", backed=False)
    db.insert_newsletter_send(
        issue_id="issue-mixed",
        subject="Mixed sources",
        content_ids=[x_post, thread, blog],
    )

    rows = NewsletterSourceMix(db).summarize(days=30)

    assert len(rows) == 1
    row = rows[0]
    assert row.source_content_ids == [x_post, thread, blog]
    assert row.source_count == 3
    assert row.found_source_count == 3
    assert row.x_post_count == 1
    assert row.thread_count == 1
    assert row.blog_post_count == 1
    assert row.topic_distribution == {"ai": 1, "architecture": 1, "testing": 1}
    assert row.knowledge_backed_item_count == 2
    assert row.warnings == []


def test_single_topic_send_warns_deterministically(db):
    first = _insert_content(db, "x_post", "ai", backed=False)
    second = _insert_content(db, "x_thread", "ai", backed=False)
    third = _insert_content(db, "blog_post", "ai", backed=False)
    db.insert_newsletter_send(
        issue_id="issue-ai",
        subject="One topic",
        content_ids=[first, second, third],
    )

    row = NewsletterSourceMix(db).summarize(days=30)[0]

    assert row.topic_distribution == {"ai": 3}
    assert row.knowledge_backed_item_count == 0
    assert row.warnings == ["no_knowledge_links", "single_topic_heavy"]


def test_missing_source_rows_are_reported(db):
    content_id = _insert_content(db, "x_post", "ai", backed=True)
    db.insert_newsletter_send(
        issue_id="issue-missing",
        subject="Missing source",
        content_ids=[content_id, 9999],
    )

    row = NewsletterSourceMix(db).summarize(days=30)[0]

    assert row.source_content_ids == [content_id, 9999]
    assert row.found_source_count == 1
    assert row.missing_source_ids == [9999]
    assert row.warnings == ["missing_source_rows", "too_few_sources"]


def test_malformed_source_content_ids_do_not_crash_and_warn(db):
    send_id = db.insert_newsletter_send(
        issue_id="issue-bad",
        subject="Bad source ids",
        content_ids=[],
    )
    db.conn.execute(
        "UPDATE newsletter_sends SET source_content_ids = ? WHERE id = ?",
        ("not-json", send_id),
    )
    db.conn.commit()

    row = NewsletterSourceMix(db).summarize(days=30)[0]

    assert row.source_content_ids == []
    assert row.source_count == 0
    assert row.warnings == ["malformed_source_content_ids", "too_few_sources"]


def test_json_and_table_formatting_are_stable(db, capsys):
    content_id = _insert_content(db, "x_post", "ai", backed=True)
    db.insert_newsletter_send(
        issue_id="issue-json",
        subject="JSON source mix",
        content_ids=[content_id],
    )

    with patch.object(
        newsletter_source_mix,
        "script_context",
        return_value=_script_context(db),
    ):
        newsletter_source_mix.main(["--days", "30", "--limit", "1", "--json"])

    data = json.loads(capsys.readouterr().out)
    assert data[0]["issue_id"] == "issue-json"
    assert list(data[0]) == sorted(data[0])
    assert data[0]["warnings"] == ["too_few_sources"]

    rows = NewsletterSourceMix(db).summarize(days=30)
    text = newsletter_source_mix.format_text_report(rows, days=30)
    assert "Newsletter Source Mix (last 30 days)" in text
    assert "Send  Issue        Sources" in text
    assert "issue-json" in text
    assert "too_few_sources" in text
