"""Tests for newsletter send source status drift reporting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.newsletter_send_source_status_drift import (
    build_newsletter_send_source_status_drift_report,
    build_newsletter_send_source_status_drift_report_from_db,
    format_newsletter_send_source_status_drift_json,
    format_newsletter_send_source_status_drift_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_send_source_status_drift.py"
spec = importlib.util.spec_from_file_location("newsletter_send_source_status_drift_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE newsletter_sends (
               id INTEGER PRIMARY KEY,
               status TEXT,
               sent_at TEXT,
               source_content_ids TEXT
           );
           CREATE TABLE generated_content (
               id INTEGER PRIMARY KEY,
               status TEXT,
               published INTEGER,
               published_at TEXT
           );"""
    )
    return conn


def test_builder_reports_ordered_source_drift_items():
    report = build_newsletter_send_source_status_drift_report(
        [
            {"newsletter_send_id": 1, "status": "sent", "sent_at": NOW.isoformat(), "source_content_ids": "[10, 11, 11, 99]"},
            {"newsletter_send_id": 2, "status": "draft", "source_content_ids": "[not-json"},
        ],
        [
            {"id": 10, "status": "abandoned"},
            {"id": 11, "status": "draft"},
        ],
        now=NOW,
    )

    assert report["artifact_type"] == "newsletter_send_source_status_drift"
    assert [item["issue_type"] for item in report["drift_items"]] == [
        "duplicate_source_id",
        "missing_generated_content",
        "source_content_abandoned",
        "source_content_unpublished_at_send_time",
        "source_content_unpublished_at_send_time",
        "malformed_source_content_ids_json",
    ]
    assert all("issue_id" in item and "newsletter_send_id" in item for item in report["drift_items"])


def test_db_loader_filters_status_window_and_handles_malformed_json():
    conn = _conn()
    recent = (NOW - timedelta(days=2)).isoformat()
    old = (NOW - timedelta(days=60)).isoformat()
    conn.execute("INSERT INTO generated_content VALUES (10, 'draft', 0, NULL)")
    conn.execute("INSERT INTO newsletter_sends VALUES (1, 'sent', ?, '[10]')", (recent,))
    conn.execute("INSERT INTO newsletter_sends VALUES (2, 'draft', ?, '[bad')", (recent,))
    conn.execute("INSERT INTO newsletter_sends VALUES (3, 'sent', ?, '[10]')", (old,))

    report = build_newsletter_send_source_status_drift_report_from_db(conn, status="sent", window_days=30, now=NOW)

    assert [item["newsletter_send_id"] for item in report["drift_items"]] == [1]
    assert report["drift_items"][0]["source_content_id"] == 10

    malformed = build_newsletter_send_source_status_drift_report_from_db(conn, status="draft", window_days=30, now=NOW)
    assert malformed["drift_items"][0]["issue_type"] == "malformed_source_content_ids_json"


def test_missing_tables_and_cli_validation(tmp_path, capsys):
    missing = build_newsletter_send_source_status_drift_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["generated_content", "newsletter_sends"]

    conn = _conn()
    conn.execute("INSERT INTO newsletter_sends VALUES (1, 'sent', ?, '[42]')", (NOW.isoformat(),))
    conn.commit()
    db_path = tmp_path / "newsletter.sqlite"
    with sqlite3.connect(db_path) as target:
        conn.backup(target)

    assert script.main(["--db", str(db_path), "--format", "json", "--status", "sent"]) == 0
    assert json.loads(capsys.readouterr().out)["drift_items"][0]["issue_type"] == "missing_generated_content"
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Newsletter Send Source Status Drift" in capsys.readouterr().out
    assert script.main(["--db", str(db_path), "--window-days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
    assert script.main(["--db", str(db_path), "--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_formatters_and_invalid_thresholds():
    report = build_newsletter_send_source_status_drift_report([], [], now=NOW)
    assert json.loads(format_newsletter_send_source_status_drift_json(report))["artifact_type"] == "newsletter_send_source_status_drift"
    assert "No newsletter source status drift found" in format_newsletter_send_source_status_drift_text(report)
    with pytest.raises(ValueError, match="window_days must be positive"):
        build_newsletter_send_source_status_drift_report([], [], window_days=0)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_newsletter_send_source_status_drift_report([], [], limit=0)
