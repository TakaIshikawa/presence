"""Tests for newsletter send cadence anomaly reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.newsletter_send_cadence_anomalies import (
    build_newsletter_send_cadence_anomalies_report,
    format_newsletter_send_cadence_anomalies_json,
    format_newsletter_send_cadence_anomalies_text,
)


NOW = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_send_cadence_anomalies.py"
spec = importlib.util.spec_from_file_location("newsletter_send_cadence_anomalies_script", SCRIPT_PATH)
newsletter_send_cadence_anomalies_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_send_cadence_anomalies_script)


@contextmanager
def _script_context(conn):
    yield SimpleNamespace(), conn


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE newsletter_sends (
            id INTEGER PRIMARY KEY,
            issue_id TEXT,
            subject TEXT,
            status TEXT,
            sent_at TEXT
        )"""
    )
    return conn


def _send(conn: sqlite3.Connection, send_id: int, when: datetime, issue: str | None = None) -> None:
    conn.execute(
        "INSERT INTO newsletter_sends (id, issue_id, subject, status, sent_at) VALUES (?, ?, ?, 'sent', ?)",
        (send_id, issue or f"issue-{send_id}", f"Subject {send_id}", when.isoformat()),
    )
    conn.commit()


def test_normal_cadence_has_no_anomalies():
    conn = _conn()
    _send(conn, 1, NOW - timedelta(days=21))
    _send(conn, 2, NOW - timedelta(days=14))
    _send(conn, 3, NOW - timedelta(days=7))

    report = build_newsletter_send_cadence_anomalies_report(conn, now=NOW, target_days=7, tolerance_hours=2)

    assert report["totals"]["send_count"] == 3
    assert report["totals"]["anomaly_count"] == 0
    assert report["missing_tables"] == []


def test_long_and_short_gaps_are_flagged_deterministically():
    conn = _conn()
    _send(conn, 1, NOW - timedelta(days=20))
    _send(conn, 2, NOW - timedelta(days=12))
    _send(conn, 3, NOW - timedelta(days=11))

    report = build_newsletter_send_cadence_anomalies_report(conn, now=NOW, target_days=7, tolerance_hours=12)

    assert [row["anomaly_type"] for row in report["anomalies"]] == ["short_gap", "long_gap"]
    assert report["totals"]["long_gap_count"] == 1
    assert report["totals"]["short_gap_count"] == 1


def test_weekday_hour_drift_summary_includes_examples():
    conn = _conn()
    _send(conn, 1, datetime(2026, 5, 4, 9, tzinfo=timezone.utc))
    _send(conn, 2, datetime(2026, 5, 11, 9, tzinfo=timezone.utc))
    _send(conn, 3, datetime(2026, 5, 18, 15, tzinfo=timezone.utc))

    report = build_newsletter_send_cadence_anomalies_report(conn, now=datetime(2026, 5, 19, 12, tzinfo=timezone.utc))
    drift = report["weekday_hour_drift"]

    assert drift["primary_weekday"] == "Monday"
    assert drift["primary_hour"] == 9
    assert drift["examples"][0]["newsletter_send_id"] == 3


def test_json_text_cli_and_invalid_args(monkeypatch, capsys):
    conn = _conn()
    _send(conn, 1, NOW - timedelta(days=8))
    _send(conn, 2, NOW - timedelta(days=1))
    report = build_newsletter_send_cadence_anomalies_report(conn, now=NOW)

    payload = json.loads(format_newsletter_send_cadence_anomalies_json(report))
    assert payload["artifact_type"] == "newsletter_send_cadence_anomalies"
    assert "Newsletter Send Cadence Anomalies" in format_newsletter_send_cadence_anomalies_text(report)

    monkeypatch.setattr(newsletter_send_cadence_anomalies_script, "script_context", lambda: _script_context(conn))
    monkeypatch.setattr(
        newsletter_send_cadence_anomalies_script,
        "build_newsletter_send_cadence_anomalies_report",
        lambda db, **kwargs: build_newsletter_send_cadence_anomalies_report(db, now=NOW, **kwargs),
    )
    assert newsletter_send_cadence_anomalies_script.main(["--format", "text", "--days", "30"]) == 0
    assert "Totals: sends=2" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        newsletter_send_cadence_anomalies_script.parse_args(["--days", "0"])


def test_missing_schema_handling():
    missing = build_newsletter_send_cadence_anomalies_report(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["newsletter_sends"]

    partial = sqlite3.connect(":memory:")
    partial.execute("CREATE TABLE newsletter_sends (id INTEGER PRIMARY KEY)")
    report = build_newsletter_send_cadence_anomalies_report(partial, now=NOW)
    assert report["missing_columns"] == {"newsletter_sends": ["sent_at"]}
