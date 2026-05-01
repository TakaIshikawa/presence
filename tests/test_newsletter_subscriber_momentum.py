"""Tests for newsletter subscriber momentum reporting."""

from __future__ import annotations

import json
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from newsletter_subscriber_momentum import main  # noqa: E402
from evaluation.newsletter_subscriber_momentum import (  # noqa: E402
    build_newsletter_subscriber_momentum_report,
    format_newsletter_subscriber_momentum_text,
)


BASE_TIME = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)


def _snapshot(
    db,
    *,
    subscriber_count: int,
    active_subscriber_count: int | None = None,
    unsubscribes: int | None = None,
    churn_rate: float | None = None,
    new_subscribers: int | None = None,
    net_subscriber_change: int | None = None,
    fetched_days_ago: float,
) -> int:
    snapshot_id = db.insert_newsletter_subscriber_metrics(
        subscriber_count=subscriber_count,
        active_subscriber_count=active_subscriber_count,
        unsubscribes=unsubscribes,
        churn_rate=churn_rate,
        new_subscribers=new_subscribers,
        net_subscriber_change=net_subscriber_change,
    )
    db.conn.execute(
        "UPDATE newsletter_subscriber_metrics SET fetched_at = ? WHERE id = ?",
        ((BASE_TIME - timedelta(days=fetched_days_ago)).isoformat(), snapshot_id),
    )
    db.conn.commit()
    return snapshot_id


def test_empty_snapshots_report_stable_empty_state():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_newsletter_subscriber_momentum_report(conn, now=BASE_TIME)
    text = format_newsletter_subscriber_momentum_text(report)

    assert report["totals"] == {"snapshots": 0, "warnings": 0}
    assert report["summary"]["snapshots_used"] == 0
    assert report["summary"]["subscriber_delta"] is None
    assert report["empty_state"]["schema_present"] is False
    assert "No newsletter subscriber metrics found" in text


def test_report_compares_first_and_latest_snapshots_in_window(db):
    _snapshot(
        db,
        subscriber_count=50,
        active_subscriber_count=45,
        unsubscribes=2,
        churn_rate=0.02,
        net_subscriber_change=5,
        fetched_days_ago=10,
    )
    _snapshot(
        db,
        subscriber_count=100,
        active_subscriber_count=90,
        unsubscribes=4,
        churn_rate=0.03,
        net_subscriber_change=8,
        fetched_days_ago=4,
    )
    _snapshot(
        db,
        subscriber_count=112,
        active_subscriber_count=103,
        unsubscribes=7,
        churn_rate=0.04,
        net_subscriber_change=14,
        fetched_days_ago=0,
    )

    report = build_newsletter_subscriber_momentum_report(
        db,
        days=7,
        now=BASE_TIME,
    )

    summary = report["summary"]
    assert summary["first_subscriber_count"] == 100
    assert summary["latest_subscriber_count"] == 112
    assert summary["subscriber_delta"] == 12
    assert summary["first_active_subscriber_count"] == 90
    assert summary["latest_active_subscriber_count"] == 103
    assert summary["active_subscriber_delta"] == 13
    assert summary["net_subscriber_change"] == 6
    assert summary["unsubscribe_total"] == 3
    assert summary["average_churn_rate"] == 0.035
    assert summary["snapshots_used"] == 2
    assert report["warnings"] == []


def test_negative_growth_and_high_churn_warnings(db):
    _snapshot(
        db,
        subscriber_count=120,
        active_subscriber_count=100,
        unsubscribes=5,
        churn_rate=0.06,
        net_subscriber_change=2,
        fetched_days_ago=2,
    )
    _snapshot(
        db,
        subscriber_count=118,
        active_subscriber_count=94,
        unsubscribes=8,
        churn_rate=0.08,
        net_subscriber_change=-3,
        fetched_days_ago=0,
    )

    report = build_newsletter_subscriber_momentum_report(
        db,
        churn_warning_rate=0.05,
        now=BASE_TIME,
    )

    assert {
        warning["label"] for warning in report["warnings"]
    } == {"negative_growth", "high_churn"}
    assert report["summary"]["subscriber_delta"] == -2
    assert report["summary"]["active_subscriber_delta"] == -6
    assert report["summary"]["net_subscriber_change"] == -5
    assert report["summary"]["average_churn_rate"] == 0.07


def test_text_output_includes_required_fields_and_warnings(db):
    _snapshot(
        db,
        subscriber_count=80,
        active_subscriber_count=70,
        unsubscribes=1,
        churn_rate=0.07,
        net_subscriber_change=1,
        fetched_days_ago=1,
    )
    _snapshot(
        db,
        subscriber_count=79,
        active_subscriber_count=68,
        unsubscribes=3,
        churn_rate=0.09,
        net_subscriber_change=-1,
        fetched_days_ago=0,
    )

    report = build_newsletter_subscriber_momentum_report(
        db,
        churn_warning_rate=0.05,
        now=BASE_TIME,
    )
    text = format_newsletter_subscriber_momentum_text(report)

    assert "Newsletter subscriber momentum" in text
    assert "Subscribers:" in text
    assert "Active subscribers:" in text
    assert "Net subscriber change:" in text
    assert "Unsubscribe total:" in text
    assert "Average churn:" in text
    assert "negative_growth" in text
    assert "high_churn" in text


def test_cli_supports_json_format_and_threshold_flags(db, capsys):
    _snapshot(
        db,
        subscriber_count=80,
        active_subscriber_count=70,
        unsubscribes=1,
        churn_rate=0.02,
        net_subscriber_change=1,
        fetched_days_ago=0,
    )
    fixed_report = build_newsletter_subscriber_momentum_report(
        db,
        days=14,
        churn_warning_rate=0.04,
        now=BASE_TIME,
    )

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch(
        "newsletter_subscriber_momentum.script_context",
        fake_script_context,
    ), patch(
        "newsletter_subscriber_momentum.build_newsletter_subscriber_momentum_report",
        return_value=fixed_report,
    ):
        result = main(
            [
                "--days",
                "14",
                "--churn-warning-rate",
                "0.04",
                "--format",
                "json",
            ]
        )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["lookback_days"] == 14
    assert payload["thresholds"]["churn_warning_rate"] == 0.04
