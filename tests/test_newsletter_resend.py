"""Tests for newsletter resend candidate reporting."""

import importlib.util
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from evaluation.newsletter_resend import NewsletterResendFinder


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_resend.py"
spec = importlib.util.spec_from_file_location("newsletter_resend", SCRIPT_PATH)
newsletter_resend = importlib.util.module_from_spec(spec)
spec.loader.exec_module(newsletter_resend)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _connect_minimal_db(include_metrics: bool = True) -> SimpleNamespace:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            content_type TEXT,
            content TEXT,
            published INTEGER,
            published_at TEXT,
            eval_score REAL
        )"""
    )
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
    if include_metrics:
        conn.execute(
            """CREATE TABLE newsletter_engagement (
                newsletter_send_id INTEGER,
                issue_id TEXT,
                opens INTEGER,
                clicks INTEGER,
                unsubscribes INTEGER,
                fetched_at TEXT
            )"""
        )
    conn.commit()
    return SimpleNamespace(conn=conn)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _insert_content(conn, content_id: int, content_type: str = "blog_post") -> None:
    conn.execute(
        """INSERT INTO generated_content
           (id, content_type, content, published, published_at, eval_score)
           VALUES (?, ?, ?, 1, ?, 7.0)""",
        (content_id, content_type, f"Content {content_id}", _now()),
    )


def _insert_send(conn, content_id: int, subject: str, opens: int, clicks: int) -> None:
    conn.execute(
        """INSERT INTO newsletter_sends
           (id, issue_id, subject, source_content_ids, subscriber_count, sent_at)
           VALUES (?, ?, ?, ?, 100, ?)""",
        (content_id, f"issue-{content_id}", subject, json.dumps([content_id]), _now()),
    )
    conn.execute(
        """INSERT INTO newsletter_engagement
           (newsletter_send_id, issue_id, opens, clicks, unsubscribes, fetched_at)
           VALUES (?, ?, ?, ?, 0, ?)""",
        (content_id, f"issue-{content_id}", opens, clicks, _now()),
    )
    conn.commit()


def test_finder_recommends_resend_for_strong_opens_and_weak_clicks():
    db = _connect_minimal_db()
    _insert_content(db.conn, 1)
    _insert_send(db.conn, 1, "Strong hook, weak follow-through", opens=55, clicks=2)

    report = NewsletterResendFinder(db).find(
        days=30, min_open_rate=0.40, max_click_rate=0.04
    )

    assert report.resend_count == 1
    row = report.rows[0]
    assert row.content_id == 1
    assert row.subject == "Strong hook, weak follow-through"
    assert row.open_rate == 0.55
    assert row.click_rate == 0.02
    assert row.recommendation == "resend"
    assert "meets threshold" in row.reasons[0]


def test_finder_recommends_subject_retest_for_low_open_and_low_click():
    db = _connect_minimal_db()
    _insert_content(db.conn, 2)
    _insert_send(db.conn, 2, "Flat subject", opens=20, clicks=1)

    report = NewsletterResendFinder(db).find(
        days=30, min_open_rate=0.40, max_click_rate=0.04
    )

    row = report.rows[0]
    assert row.recommendation == "subject_retest"
    assert row.open_rate == 0.20
    assert "below threshold" in row.reasons[0]


def test_finder_keeps_no_action_rows_when_click_followthrough_is_healthy():
    db = _connect_minimal_db()
    _insert_content(db.conn, 3)
    _insert_send(db.conn, 3, "Worked well", opens=60, clicks=12)

    report = NewsletterResendFinder(db).find(
        days=30, min_open_rate=0.40, max_click_rate=0.04
    )

    row = report.rows[0]
    assert row.recommendation == "no_action"
    assert row.click_rate == 0.12
    assert "above 0.040" in row.reasons[0]


def test_finder_handles_missing_metrics_table_gracefully():
    db = _connect_minimal_db(include_metrics=False)
    _insert_content(db.conn, 4, content_type="newsletter_digest")
    db.conn.execute(
        """INSERT INTO newsletter_sends
           (id, issue_id, subject, source_content_ids, subscriber_count, sent_at)
           VALUES (4, 'issue-4', 'No metrics yet', '[4]', 100, ?)""",
        (_now(),),
    )
    db.conn.commit()

    report = NewsletterResendFinder(db).find(days=30)

    assert report.rows[0].recommendation == "no_action"
    assert report.rows[0].open_rate is None
    assert report.rows[0].click_rate is None
    assert report.rows[0].reasons == ["newsletter metrics are not available"]


def test_finder_handles_missing_metrics_columns_gracefully():
    db = _connect_minimal_db(include_metrics=False)
    db.conn.execute(
        """CREATE TABLE newsletter_engagement (
            newsletter_send_id INTEGER,
            issue_id TEXT,
            opens INTEGER,
            fetched_at TEXT
        )"""
    )
    _insert_content(db.conn, 7)
    db.conn.execute(
        """INSERT INTO newsletter_sends
           (id, issue_id, subject, source_content_ids, subscriber_count, sent_at)
           VALUES (7, 'issue-7', 'Partial metrics', '[7]', 100, ?)""",
        (_now(),),
    )
    db.conn.execute(
        """INSERT INTO newsletter_engagement
           (newsletter_send_id, issue_id, opens, fetched_at)
           VALUES (7, 'issue-7', 50, ?)""",
        (_now(),),
    )
    db.conn.commit()

    report = NewsletterResendFinder(db).find(days=30)

    assert report.rows[0].recommendation == "no_action"
    assert report.rows[0].reasons == ["newsletter metrics are not available"]


def test_cli_emits_json_report(capsys):
    db = _connect_minimal_db()
    _insert_content(db.conn, 5)
    _insert_send(db.conn, 5, "CLI candidate", opens=50, clicks=2)

    with patch.object(
        newsletter_resend,
        "script_context",
        return_value=_script_context(db),
    ):
        newsletter_resend.main(["--days", "30", "--json", "--limit", "1"])

    payload = json.loads(capsys.readouterr().out)
    assert payload["period_days"] == 30
    assert payload["rows"][0]["recommendation"] == "resend"


def test_cli_formatters_emit_stable_json_and_concise_text():
    db = _connect_minimal_db()
    _insert_content(db.conn, 6)
    _insert_send(db.conn, 6, "Formatter candidate", opens=50, clicks=2)
    report = NewsletterResendFinder(db).find(days=30)

    payload = json.loads(newsletter_resend.format_json_report(report))
    text = newsletter_resend.format_text_report(report)

    assert list(payload.keys()) == [
        "max_click_rate",
        "min_open_rate",
        "no_action_count",
        "period_days",
        "resend_count",
        "rows",
        "subject_retest_count",
    ]
    assert payload["rows"][0]["content_id"] == 6
    assert "Newsletter resend candidates (last 30 days)" in text
    assert "resend: content 6" in text
