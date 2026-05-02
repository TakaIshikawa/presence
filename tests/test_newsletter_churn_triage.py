"""Tests for newsletter churn triage reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

from evaluation.newsletter_churn_triage import (
    build_newsletter_churn_triage_report,
    format_newsletter_churn_triage_json,
    format_newsletter_churn_triage_text,
)


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "newsletter_churn_triage.py"
)
spec = importlib.util.spec_from_file_location("newsletter_churn_triage", SCRIPT_PATH)
newsletter_churn_triage = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_churn_triage)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _send(
    db,
    *,
    issue_id: str,
    days_ago: float,
    subscribers: int = 100,
    clicks: int | None = None,
    unsubscribes: int | None = None,
    complaints: int | None = None,
) -> int:
    send_id = db.insert_newsletter_send(
        issue_id=issue_id,
        subject=f"Issue {issue_id}",
        content_ids=[],
        subscriber_count=subscribers,
        status="sent",
    )
    sent_at = (NOW - timedelta(days=days_ago, hours=1)).isoformat()
    db.conn.execute(
        "UPDATE newsletter_sends SET sent_at = ? WHERE id = ?",
        (sent_at, send_id),
    )
    if clicks is not None and unsubscribes is not None:
        engagement_id = db.insert_newsletter_engagement(
            send_id,
            issue_id,
            opens=50,
            clicks=clicks,
            unsubscribes=unsubscribes,
            fetched_at=(NOW - timedelta(days=days_ago)).isoformat(),
        )
        if complaints is not None:
            db.conn.execute(
                "UPDATE newsletter_engagement SET complaints = ? WHERE id = ?",
                (complaints, engagement_id),
            )
    db.conn.commit()
    return send_id


def _add_complaints_column(db) -> None:
    db.conn.execute("ALTER TABLE newsletter_engagement ADD COLUMN complaints INTEGER")
    db.conn.commit()


def test_report_flags_unsubscribe_complaint_and_low_click_signals(db):
    _add_complaints_column(db)
    _send(db, issue_id="base-1", days_ago=30, clicks=20, unsubscribes=1, complaints=0)
    _send(db, issue_id="base-2", days_ago=20, clicks=18, unsubscribes=1, complaints=0)
    _send(db, issue_id="base-3", days_ago=10, clicks=22, unsubscribes=1, complaints=0)
    _send(db, issue_id="good", days_ago=2, clicks=19, unsubscribes=0, complaints=0)
    bad_id = _send(
        db,
        issue_id="bad",
        days_ago=1,
        clicks=3,
        unsubscribes=4,
        complaints=1,
    )

    report = build_newsletter_churn_triage_report(
        db,
        days=7,
        baseline_days=28,
        min_sends=3,
        now=NOW,
    )

    assert report.baseline_metrics["sufficient"] is True
    assert report.baseline_metrics["average_click_rate"] == 0.2
    assert report.baseline_metrics["average_unsubscribe_rate"] == 0.01
    assert report.totals["recent_send_count"] == 2
    assert report.totals["flagged_send_count"] == 1
    flagged = report.flagged_sends[0]
    assert flagged.newsletter_send_id == bad_id
    assert flagged.click_rate == 0.03
    assert flagged.unsubscribe_rate == 0.04
    assert flagged.complaint_rate == 0.01
    assert {reason.label for reason in flagged.reasons} == {
        "complaint_signal",
        "low_click_rate",
        "unsubscribe_spike",
    }
    assert report.recommended_review_reasons == (
        "complaint_signal",
        "low_click_rate",
        "unsubscribe_spike",
    )

    text = format_newsletter_churn_triage_text(report)
    assert "Newsletter Churn Triage" in text
    assert "Flagged sends:" in text
    assert "issue=bad" in text
    assert "complaint_signal" in text


def test_json_output_is_stable_and_serializable(db):
    _send(db, issue_id="base-1", days_ago=20, clicks=10, unsubscribes=0)
    _send(db, issue_id="base-2", days_ago=15, clicks=10, unsubscribes=0)
    _send(db, issue_id="recent", days_ago=1, clicks=0, unsubscribes=0)

    report = build_newsletter_churn_triage_report(
        db,
        days=7,
        baseline_days=21,
        min_sends=2,
        now=NOW,
    )

    payload = json.loads(format_newsletter_churn_triage_json(report))
    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["filters"] == {"baseline_days": 21, "days": 7, "min_sends": 2}
    assert payload["flagged_sends"][0]["reasons"][0]["label"] == "low_click_rate"
    assert payload["availability"]["complaints"] is False


def test_missing_newsletter_tables_returns_empty_sections():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_newsletter_churn_triage_report(conn, now=NOW)

    assert report.totals["recent_send_count"] == 0
    assert report.flagged_sends == ()
    assert report.baseline_metrics["send_count"] == 0
    assert report.availability["newsletter_sends"] is False
    assert report.availability["newsletter_engagement"] is False
    assert "newsletter_sends" in report.missing_tables
    text = format_newsletter_churn_triage_text(report)
    assert "Missing tables: newsletter_sends" in text
    assert "No recent newsletter sends found" in text


def test_text_output_handles_no_signals(db):
    _send(db, issue_id="base-1", days_ago=25, clicks=20, unsubscribes=1)
    _send(db, issue_id="base-2", days_ago=15, clicks=20, unsubscribes=1)
    _send(db, issue_id="recent", days_ago=1, clicks=18, unsubscribes=0)

    report = build_newsletter_churn_triage_report(
        db,
        days=7,
        baseline_days=28,
        min_sends=2,
        now=NOW,
    )

    assert report.flagged_sends == ()
    assert "No newsletter churn triage signals detected." in (
        format_newsletter_churn_triage_text(report)
    )


def test_cli_supports_days_baseline_min_sends_and_json(db, capsys):
    _send(db, issue_id="base-1", days_ago=20, clicks=10, unsubscribes=0)
    _send(db, issue_id="base-2", days_ago=15, clicks=10, unsubscribes=0)
    _send(db, issue_id="recent", days_ago=1, clicks=0, unsubscribes=0)
    fixed_report = build_newsletter_churn_triage_report(
        db,
        days=7,
        baseline_days=21,
        min_sends=2,
        now=NOW,
    )

    with patch.object(
        newsletter_churn_triage,
        "script_context",
        wraps=lambda: _script_context(db),
    ), patch.object(
        newsletter_churn_triage,
        "build_newsletter_churn_triage_report",
        return_value=fixed_report,
    ):
        result = newsletter_churn_triage.main(
            ["--days", "7", "--baseline-days", "21", "--min-sends", "2", "--json"]
        )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"]["days"] == 7
    assert payload["filters"]["baseline_days"] == 21
    assert payload["filters"]["min_sends"] == 2
