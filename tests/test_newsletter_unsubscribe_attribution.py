"""Tests for newsletter unsubscribe attribution reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.newsletter_unsubscribe_attribution import (
    MALFORMED_SOURCE_CONTENT_IDS,
    NO_SOURCE_CONTENT_IDS,
    build_newsletter_unsubscribe_attribution_report,
    format_newsletter_unsubscribe_attribution_json,
    format_newsletter_unsubscribe_attribution_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "newsletter_unsubscribe_attribution.py"
)
spec = importlib.util.spec_from_file_location(
    "newsletter_unsubscribe_attribution_script", SCRIPT_PATH
)
newsletter_unsubscribe_attribution_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_unsubscribe_attribution_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, *, content_type: str, topic: str) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=f"{content_type} about {topic}",
        eval_score=8.0,
        eval_feedback="good",
    )
    db.insert_content_topics(content_id, [(topic, None, 1.0)])
    return int(content_id)


def _send(
    db,
    *,
    issue_id: str,
    content_ids: list[int],
    subscribers: int = 100,
    sent_at: datetime | None = None,
    status: str = "sent",
) -> int:
    send_id = db.insert_newsletter_send(
        issue_id=issue_id,
        subject=f"Subject {issue_id}",
        content_ids=content_ids,
        subscriber_count=subscribers,
        status=status,
    )
    db.conn.execute(
        "UPDATE newsletter_sends SET sent_at = ? WHERE id = ?",
        ((sent_at or NOW).isoformat(), send_id),
    )
    db.conn.commit()
    return int(send_id)


def _engagement(
    db,
    send_id: int,
    *,
    issue_id: str,
    opens: int = 50,
    clicks: int = 5,
    unsubscribes: int = 1,
    fetched_at: datetime | None = None,
) -> int:
    return int(
        db.insert_newsletter_engagement(
            send_id,
            issue_id,
            opens=opens,
            clicks=clicks,
            unsubscribes=unsubscribes,
            fetched_at=(fetched_at or NOW).isoformat(),
        )
    )


def test_json_output_ranks_topics_content_types_and_send_details(db):
    ai_post = _content(db, content_type="blog_post", topic="AI")
    ops_thread = _content(db, content_type="x_thread", topic="Ops")
    ai_send = _send(
        db,
        issue_id="ai-spike",
        content_ids=[ai_post],
        subscribers=100,
        sent_at=NOW - timedelta(days=1),
    )
    ops_send = _send(
        db,
        issue_id="ops-spike",
        content_ids=[ops_thread],
        subscribers=200,
        sent_at=NOW - timedelta(days=2),
    )
    ignored = _send(
        db,
        issue_id="ignored",
        content_ids=[ai_post],
        subscribers=100,
        sent_at=NOW - timedelta(days=1),
    )
    _engagement(db, ai_send, issue_id="ai-spike", unsubscribes=4)
    _engagement(db, ops_send, issue_id="ops-spike", unsubscribes=2)
    _engagement(db, ignored, issue_id="ignored", unsubscribes=0)

    report = build_newsletter_unsubscribe_attribution_report(
        db,
        days=14,
        min_unsubscribes=1,
        now=NOW,
    )
    payload = json.loads(format_newsletter_unsubscribe_attribution_json(report))

    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["artifact_type"] == "newsletter_unsubscribe_attribution"
    assert payload["filters"]["days"] == 14
    assert payload["totals"] == {
        "attributed_send_count": 2,
        "send_count": 2,
        "unsubscribe_count": 6,
        "warning_count": 0,
    }
    assert payload["send_details"][0]["issue_id"] == "ai-spike"
    assert payload["send_details"][0]["unsubscribe_rate"] == 0.04
    assert payload["ranked_topics"][0]["topic"] == "AI"
    assert payload["ranked_topics"][0]["unsubscribes"] == 4
    assert payload["ranked_content_types"][0]["content_type"] == "blog_post"


def test_text_output_highlights_sends_topics_and_content_types(db):
    content_id = _content(db, content_type="newsletter_brief", topic="Retention")
    send_id = _send(
        db,
        issue_id="retention",
        content_ids=[content_id],
        subscribers=50,
        sent_at=NOW - timedelta(days=1),
    )
    _engagement(db, send_id, issue_id="retention", unsubscribes=3)

    text = format_newsletter_unsubscribe_attribution_text(
        build_newsletter_unsubscribe_attribution_report(db, now=NOW)
    )

    assert "# Newsletter Unsubscribe Attribution" in text
    assert "## Highest Unsubscribe-Rate Sends" in text
    assert "issue=retention" in text
    assert "rate=6.0%" in text
    assert "topic=Retention" in text
    assert "content_type=newsletter_brief" in text


def test_topic_filter_keeps_matching_attribution_only(db):
    ai = _content(db, content_type="blog_post", topic="AI")
    ops = _content(db, content_type="blog_post", topic="Ops")
    ai_send = _send(db, issue_id="ai", content_ids=[ai], sent_at=NOW - timedelta(days=1))
    ops_send = _send(db, issue_id="ops", content_ids=[ops], sent_at=NOW - timedelta(days=1))
    _engagement(db, ai_send, issue_id="ai", unsubscribes=1)
    _engagement(db, ops_send, issue_id="ops", unsubscribes=5)

    report = build_newsletter_unsubscribe_attribution_report(
        db,
        topic="ai",
        now=NOW,
    )

    assert [detail.issue_id for detail in report.send_details] == ["ai"]
    assert [row.topic for row in report.ranked_topics] == ["AI"]
    assert report.filters["topic"] == "ai"


def test_malformed_and_empty_source_content_ids_warn_without_raising(db):
    malformed = _send(db, issue_id="bad-json", content_ids=[], sent_at=NOW)
    empty = _send(db, issue_id="empty", content_ids=[], sent_at=NOW)
    db.conn.execute(
        "UPDATE newsletter_sends SET source_content_ids = ? WHERE id = ?",
        ("not-json", malformed),
    )
    db.conn.execute(
        "UPDATE newsletter_sends SET source_content_ids = ? WHERE id = ?",
        ("[]", empty),
    )
    db.conn.commit()
    _engagement(db, malformed, issue_id="bad-json", unsubscribes=2)
    _engagement(db, empty, issue_id="empty", unsubscribes=1)

    report = build_newsletter_unsubscribe_attribution_report(db, now=NOW)

    warnings_by_issue = {detail.issue_id: detail.warnings for detail in report.send_details}
    assert warnings_by_issue["bad-json"] == (
        MALFORMED_SOURCE_CONTENT_IDS,
        NO_SOURCE_CONTENT_IDS,
    )
    assert warnings_by_issue["empty"] == (NO_SOURCE_CONTENT_IDS,)
    assert report.warnings == (MALFORMED_SOURCE_CONTENT_IDS, NO_SOURCE_CONTENT_IDS)
    assert report.ranked_topics == ()


def test_missing_optional_tables_are_metadata_not_crashes(monkeypatch, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE newsletter_sends (
            id INTEGER PRIMARY KEY,
            issue_id TEXT,
            subject TEXT,
            source_content_ids TEXT,
            subscriber_count INTEGER,
            sent_at TEXT
        )"""
    )
    conn.execute(
        """CREATE TABLE newsletter_engagement (
            id INTEGER PRIMARY KEY,
            newsletter_send_id INTEGER,
            issue_id TEXT,
            unsubscribes INTEGER,
            fetched_at TEXT
        )"""
    )
    conn.execute(
        """INSERT INTO newsletter_sends
           (id, issue_id, subject, source_content_ids, subscriber_count, sent_at)
           VALUES (1, 'legacy', 'Legacy', '[10]', 100, ?)""",
        (NOW.isoformat(),),
    )
    conn.execute(
        """INSERT INTO newsletter_engagement
           (newsletter_send_id, issue_id, unsubscribes, fetched_at)
           VALUES (1, 'legacy', 2, ?)""",
        (NOW.isoformat(),),
    )
    conn.commit()
    try:
        report = build_newsletter_unsubscribe_attribution_report(conn, now=NOW)
        payload = json.loads(format_newsletter_unsubscribe_attribution_json(report))
        assert payload["missing_tables"] == ["generated_content", "content_topics"]
        assert payload["send_details"][0]["issue_id"] == "legacy"
        assert payload["send_details"][0]["attributed_topics"] == []

        monkeypatch.setattr(
            newsletter_unsubscribe_attribution_script,
            "script_context",
            lambda: _script_context(conn),
        )
        monkeypatch.setattr(
            newsletter_unsubscribe_attribution_script,
            "build_newsletter_unsubscribe_attribution_report",
            lambda db, **kwargs: build_newsletter_unsubscribe_attribution_report(
                db,
                now=NOW,
                **kwargs,
            ),
        )
        assert (
            newsletter_unsubscribe_attribution_script.main(
                ["--days", "7", "--min-unsubscribes", "1", "--format", "json"]
            )
            == 0
        )
        cli_payload = json.loads(capsys.readouterr().out)
        assert cli_payload["send_details"][0]["issue_id"] == "legacy"
        assert newsletter_unsubscribe_attribution_script.main(["--days", "0"]) == 2
        assert "value must be positive" in capsys.readouterr().err
    finally:
        conn.close()
