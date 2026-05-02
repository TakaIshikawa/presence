"""Tests for newsletter churn spike detection."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

from evaluation.newsletter_churn_spikes import (
    build_newsletter_churn_spike_report,
    format_newsletter_churn_spike_json,
    format_newsletter_churn_spike_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_churn_spikes.py"
spec = importlib.util.spec_from_file_location("newsletter_churn_spikes", SCRIPT_PATH)
newsletter_churn_spikes = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_churn_spikes)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _snapshot(
    db,
    *,
    days_ago: float,
    subscribers: int,
    active: int,
    unsubscribes: int,
    churn_rate: float,
) -> int:
    snapshot_id = db.insert_newsletter_subscriber_metrics(
        subscriber_count=subscribers,
        active_subscriber_count=active,
        unsubscribes=unsubscribes,
        churn_rate=churn_rate,
    )
    db.conn.execute(
        "UPDATE newsletter_subscriber_metrics SET fetched_at = ? WHERE id = ?",
        ((NOW - timedelta(days=days_ago)).isoformat(), snapshot_id),
    )
    db.conn.commit()
    return snapshot_id


def _send(db, *, issue_id: str, unsubscribes: int, days_ago: float) -> int:
    send_id = db.insert_newsletter_send(
        issue_id=issue_id,
        subject=f"Issue {issue_id}",
        content_ids=[],
        subscriber_count=100,
        status="sent",
    )
    db.conn.execute(
        "UPDATE newsletter_sends SET sent_at = ? WHERE id = ?",
        ((NOW - timedelta(days=days_ago, hours=1)).isoformat(), send_id),
    )
    db.conn.commit()
    return db.insert_newsletter_engagement(
        send_id,
        issue_id,
        opens=40,
        clicks=4,
        unsubscribes=unsubscribes,
        fetched_at=(NOW - timedelta(days=days_ago)).isoformat(),
    )


def test_recent_unsubscribe_spike_includes_baseline_and_contributing_sends(db):
    _snapshot(db, days_ago=30, subscribers=120, active=118, unsubscribes=10, churn_rate=0.004)
    _snapshot(db, days_ago=10, subscribers=125, active=122, unsubscribes=12, churn_rate=0.005)
    _snapshot(db, days_ago=6, subscribers=126, active=123, unsubscribes=12, churn_rate=0.008)
    _snapshot(db, days_ago=0, subscribers=119, active=114, unsubscribes=20, churn_rate=0.06)
    _send(db, issue_id="issue-42", unsubscribes=5, days_ago=1)
    _send(db, issue_id="issue-43", unsubscribes=0, days_ago=2)

    report = build_newsletter_churn_spike_report(
        db,
        days=7,
        baseline_days=28,
        min_unsubscribes=3,
        now=NOW,
    )

    assert report.empty_reason is None
    assert report.totals["recent_unsubscribes"] == 8
    assert report.totals["baseline_unsubscribes"] == 2
    assert report.totals["spike_count"] == 1
    spike = report.spikes[0]
    assert spike.severity == "high"
    assert spike.baseline_daily_unsubscribes == round(2 / 28, 6)
    assert spike.recent_daily_unsubscribes == round(8 / 7, 6)
    assert spike.daily_unsubscribe_ratio == round((8 / 7) / (2 / 28), 4)
    assert "recent 8 unsubscribes" in spike.comparison
    assert len(spike.contributing_sends) == 1
    assert spike.contributing_sends[0].issue_id == "issue-42"
    assert spike.recommendation == "pause_or_review_next_send"
    assert report.recommendations == ("pause_or_review_next_send",)

    payload = json.loads(format_newsletter_churn_spike_json(report))
    assert sorted(payload) == [
        "availability",
        "empty_reason",
        "filters",
        "generated_at",
        "missing_columns",
        "missing_tables",
        "recommendations",
        "spikes",
        "totals",
        "windows",
    ]
    assert payload["spikes"][0]["contributing_sends"][0]["issue_id"] == "issue-42"

    text = format_newsletter_churn_spike_text(report)
    assert "Newsletter Churn Spikes" in text
    assert "high unsubscribe_spike" in text
    assert "issue=issue-42" in text


def test_no_spike_when_recent_unsubscribes_below_minimum(db):
    _snapshot(db, days_ago=20, subscribers=100, active=98, unsubscribes=10, churn_rate=0.01)
    _snapshot(db, days_ago=10, subscribers=101, active=99, unsubscribes=11, churn_rate=0.01)
    _snapshot(db, days_ago=5, subscribers=102, active=100, unsubscribes=11, churn_rate=0.01)
    _snapshot(db, days_ago=0, subscribers=102, active=99, unsubscribes=12, churn_rate=0.02)

    report = build_newsletter_churn_spike_report(
        db,
        days=7,
        baseline_days=21,
        min_unsubscribes=3,
        now=NOW,
    )

    assert report.empty_reason is None
    assert report.spikes == ()
    assert "No newsletter churn spikes detected." in format_newsletter_churn_spike_text(report)


def test_sparse_or_missing_data_returns_empty_report_with_reason():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_newsletter_churn_spike_report(conn, now=NOW)

    assert report.spikes == ()
    assert report.empty_reason == "newsletter_subscriber_metrics table is not available"
    assert report.availability["newsletter_subscriber_metrics"] is False
    assert "newsletter_subscriber_metrics" in report.missing_tables
    assert "Empty report:" in format_newsletter_churn_spike_text(report)


def test_sparse_baseline_is_empty_even_when_recent_has_data(db):
    _snapshot(db, days_ago=3, subscribers=100, active=98, unsubscribes=1, churn_rate=0.01)
    _snapshot(db, days_ago=0, subscribers=98, active=95, unsubscribes=5, churn_rate=0.04)

    report = build_newsletter_churn_spike_report(db, days=7, baseline_days=14, now=NOW)

    assert report.spikes == ()
    assert report.empty_reason == "baseline window has fewer than 2 subscriber metric snapshots"


def test_cli_fail_on_spike_controls_exit_code(db, capsys):
    _snapshot(db, days_ago=30, subscribers=120, active=118, unsubscribes=10, churn_rate=0.004)
    _snapshot(db, days_ago=10, subscribers=125, active=122, unsubscribes=12, churn_rate=0.005)
    _snapshot(db, days_ago=6, subscribers=126, active=123, unsubscribes=12, churn_rate=0.008)
    _snapshot(db, days_ago=0, subscribers=119, active=114, unsubscribes=20, churn_rate=0.06)
    fixed_report = build_newsletter_churn_spike_report(
        db,
        days=7,
        baseline_days=28,
        min_unsubscribes=3,
        now=NOW,
    )

    with patch.object(
        newsletter_churn_spikes,
        "script_context",
        wraps=lambda: _script_context(db),
    ), patch.object(
        newsletter_churn_spikes,
        "build_newsletter_churn_spike_report",
        return_value=fixed_report,
    ):
        ok_code = newsletter_churn_spikes.main(
            ["--days", "7", "--baseline-days", "28", "--format", "json"]
        )
        fail_code = newsletter_churn_spikes.main(
            [
                "--days",
                "7",
                "--baseline-days",
                "28",
                "--format",
                "json",
                "--fail-on-spike",
            ]
        )

    assert ok_code == 0
    assert fail_code == 2
    decoder = json.JSONDecoder()
    first_payload, _end = decoder.raw_decode(capsys.readouterr().out)
    assert first_payload["totals"]["spike_count"] == 1
