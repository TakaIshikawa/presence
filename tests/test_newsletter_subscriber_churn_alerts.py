"""Tests for newsletter subscriber churn alerts."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
from pathlib import Path
import sqlite3

from evaluation.newsletter_subscriber_churn_alerts import (
    build_newsletter_subscriber_churn_alerts_report_from_db,
    format_newsletter_subscriber_churn_alerts_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_subscriber_churn_alerts.py"
spec = importlib.util.spec_from_file_location("newsletter_subscriber_churn_alerts_script", SCRIPT_PATH)
newsletter_subscriber_churn_alerts_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_subscriber_churn_alerts_script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE newsletter_subscriber_metrics (
            id INTEGER PRIMARY KEY,
            fetched_at TEXT,
            churn_rate REAL,
            unsubscribes INTEGER,
            net_subscriber_change INTEGER,
            subscriber_count INTEGER
        );
        """
    )
    return conn


def test_report_flags_churn_unsubscribe_and_net_loss_alerts():
    conn = _conn()
    conn.executemany(
        "INSERT INTO newsletter_subscriber_metrics VALUES (?, ?, ?, ?, ?, ?)",
        [
            (1, "2026-05-20T10:00:00+00:00", 0.08, 5, 2, 1000),
            (2, "2026-05-20T09:00:00+00:00", 0.03, 15, -30, 900),
            (3, "2026-05-20T08:00:00+00:00", 0.01, 1, 5, 950),
            (4, "2026-05-01T08:00:00+00:00", 0.20, 100, -100, 850),
        ],
    )

    report = build_newsletter_subscriber_churn_alerts_report_from_db(
        conn,
        now=NOW,
        lookback_days=7,
        churn_threshold=0.05,
        unsubscribe_threshold=10,
        net_loss_threshold=-25,
    )

    assert report["artifact_type"] == "newsletter_subscriber_churn_alerts"
    assert report["summary"]["rows_scanned"] == 3
    assert {item["metric_id"] for item in report["alert_items"]} == {1, 2}
    assert report["severity_summary"] == {"critical": 1, "warning": 1}
    assert "Alert items" in format_newsletter_subscriber_churn_alerts_text(report)


def test_missing_table_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    report = build_newsletter_subscriber_churn_alerts_report_from_db(conn, now=NOW)
    assert report["missing_tables"] == ["newsletter_subscriber_metrics"]
    assert report["summary"]["rows_scanned"] == 0


def test_cli_outputs_json(tmp_path, capsys):
    db_path = tmp_path / "db.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE newsletter_subscriber_metrics (
            id INTEGER PRIMARY KEY,
            fetched_at TEXT,
            churn_rate REAL,
            unsubscribes INTEGER,
            net_subscriber_change INTEGER
        );
        INSERT INTO newsletter_subscriber_metrics VALUES (1, '2026-05-20T10:00:00+00:00', 0.1, 1, 0);
        """
    )
    conn.close()
    assert newsletter_subscriber_churn_alerts_script.main(["--db", str(db_path), "--format", "json"]) == 0
    assert '"artifact_type": "newsletter_subscriber_churn_alerts"' in capsys.readouterr().out
