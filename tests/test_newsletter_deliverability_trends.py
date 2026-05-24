"""Tests for newsletter deliverability trends."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.newsletter_deliverability_trends import (
    build_newsletter_deliverability_trends_report_from_db,
    format_newsletter_deliverability_trends_json,
    format_newsletter_deliverability_trends_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_deliverability_trends.py"
spec = importlib.util.spec_from_file_location("newsletter_deliverability_trends_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE newsletter_send_events (
            id INTEGER PRIMARY KEY,
            provider TEXT,
            campaign_id TEXT,
            status TEXT,
            occurred_at TEXT
        )"""
    )
    return conn


def _event(conn: sqlite3.Connection, event_id: int, provider: str | None, campaign: str, status: str | None, days_ago: int) -> None:
    conn.execute(
        "INSERT INTO newsletter_send_events VALUES (?, ?, ?, ?, ?)",
        (event_id, provider, campaign, status, (NOW - timedelta(days=days_ago)).isoformat()),
    )
    conn.commit()


def test_trend_findings_and_deterministic_output(tmp_path, capsys):
    conn = _conn()
    for i in range(1, 11):
        _event(conn, i, "ses", "launch", "delivered", 1)
    _event(conn, 11, "ses", "launch", "bounced", 1)
    _event(conn, 12, "ses", "launch", "complaint", 1)
    _event(conn, 13, "ses", "launch", "deferred", 5)
    _event(conn, 14, "ses", "launch", "deferred", 1)
    _event(conn, 15, "ses", "launch", "deferred", 1)
    _event(conn, 16, None, "launch", None, 1)

    report = build_newsletter_deliverability_trends_report_from_db(conn, bounce_threshold=0.05, complaint_threshold=0.01, now=NOW)
    assert report["artifact_type"] == "newsletter_deliverability_trends"
    kinds = {f["finding_type"] for f in report["findings"]}
    assert {"bounce_rate_spike", "complaint_rate_spike", "deferred_backlog_growth", "missing_provider_payload", "missing_status_payload"} <= kinds
    payload = json.loads(format_newsletter_deliverability_trends_json(report))
    assert payload["filters"]["lookback_days"] == 30
    assert "Newsletter Deliverability Trends" in format_newsletter_deliverability_trends_text(report)

    db_path = tmp_path / "deliverability.sqlite"
    conn.backup(sqlite3.connect(db_path))
    assert script.main(["--db", str(db_path), "--format", "json", "--lookback-days", "30", "--bounce-threshold", "0.01", "--complaint-threshold", "0.01", "--limit", "20"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "newsletter_deliverability_trends"
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Totals:" in capsys.readouterr().out


def test_schema_gaps_empty_state_and_validation():
    missing = build_newsletter_deliverability_trends_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["newsletter_send_events|newsletter_sends"]

    bad = sqlite3.connect(":memory:")
    bad.execute("CREATE TABLE newsletter_send_events (id INTEGER PRIMARY KEY, provider TEXT)")
    gaps = build_newsletter_deliverability_trends_report_from_db(bad, now=NOW)
    assert "newsletter_send_events" in gaps["missing_columns"]

    empty = build_newsletter_deliverability_trends_report_from_db(_conn(), now=NOW)
    assert empty["empty_state"]["is_empty"] is True
    assert "No newsletter deliverability events found" in format_newsletter_deliverability_trends_text(empty)
    with pytest.raises(SystemExit):
        script.parse_args(["--lookback-days", "0"])
