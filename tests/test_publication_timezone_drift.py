"""Tests for publication timezone drift reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.publication_timezone_drift import (
    build_publication_timezone_drift_report,
    format_publication_timezone_drift_json,
    format_publication_timezone_drift_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_timezone_drift.py"
spec = importlib.util.spec_from_file_location("publication_timezone_drift_script", SCRIPT_PATH)
publication_timezone_drift_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(publication_timezone_drift_script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE publish_queue (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            scheduled_at TEXT,
            platform TEXT,
            status TEXT,
            published_at TEXT,
            created_at TEXT
        );
        CREATE TABLE publication_attempts (
            id INTEGER PRIMARY KEY,
            queue_id INTEGER,
            content_id INTEGER,
            platform TEXT,
            attempted_at TEXT,
            success INTEGER
        );
        CREATE TABLE content_publications (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            platform TEXT,
            status TEXT,
            published_at TEXT
        );
        """
    )
    return conn


def test_before_scheduled_drift_is_flagged():
    conn = _conn()
    conn.execute(
        """INSERT INTO publish_queue
           (id, content_id, scheduled_at, platform, status, published_at, created_at)
           VALUES (1, 10, '2026-05-01T10:00:00+00:00', 'x', 'published',
                   '2026-05-01T09:00:00+00:00', '2026-04-30T12:00:00+00:00')"""
    )

    report = build_publication_timezone_drift_report(conn, now=NOW)

    assert report.to_dict()["artifact_type"] == "publication_timezone_drift"
    item = report.drift_items[0]
    assert item.drift_types == ("before_scheduled", "whole_hour_offset")
    assert item.recommended_action.startswith("Inspect scheduler")
    assert report.totals["by_drift_type"]["before_scheduled"] == 1
    assert "before_scheduled" in format_publication_timezone_drift_text(report)


def test_local_day_mismatch_uses_configured_timezone_offset():
    conn = _conn()
    conn.execute(
        """INSERT INTO publish_queue
           (id, content_id, scheduled_at, platform, status, published_at, created_at)
           VALUES (2, 20, '2026-05-01T14:30:00+00:00', 'bluesky', 'published',
                   '2026-05-01T16:30:00+00:00', '2026-04-30T12:00:00+00:00')"""
    )

    report = build_publication_timezone_drift_report(
        conn,
        timezone_offset_hours=9,
        now=NOW,
    )

    item = report.drift_items[0]
    assert "local_day_mismatch" in item.drift_types
    assert item.local_scheduled_day == "2026-05-01"
    assert item.local_published_day == "2026-05-02"
    assert item.whole_hour_offset == 2


def test_platform_filter_limits_drift_items():
    conn = _conn()
    conn.execute(
        """INSERT INTO publish_queue
           (id, content_id, scheduled_at, platform, status, published_at, created_at)
           VALUES (1, 10, '2026-05-01T10:00:00+00:00', 'x', 'published',
                   '2026-05-01T09:00:00+00:00', '2026-04-30T12:00:00+00:00')"""
    )
    conn.execute(
        """INSERT INTO publish_queue
           (id, content_id, scheduled_at, platform, status, published_at, created_at)
           VALUES (2, 20, '2026-05-01T10:00:00+00:00', 'bluesky', 'published',
                   '2026-05-01T08:00:00+00:00', '2026-04-30T12:00:00+00:00')"""
    )

    report = build_publication_timezone_drift_report(conn, platform="x", now=NOW)

    assert [item.platform for item in report.drift_items] == ["x"]
    assert report.filters["platform"] == "x"


def test_json_cli_output(capsys, tmp_path):
    db_path = tmp_path / "publication.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE publish_queue (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            scheduled_at TEXT,
            platform TEXT,
            status TEXT,
            published_at TEXT,
            created_at TEXT
        );
        INSERT INTO publish_queue
          (id, content_id, scheduled_at, platform, status, published_at, created_at)
        VALUES
          (1, 10, '2026-05-01T10:00:00+00:00', 'x', 'published',
           '2026-05-01T09:00:00+00:00', '2026-04-30T12:00:00+00:00');
        """
    )
    conn.close()

    exit_code = publication_timezone_drift_script.main(
        [
            "--db",
            str(db_path),
            "--days",
            "30",
            "--platform",
            "x",
            "--timezone-offset-hours",
            "9",
            "--format",
            "json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["artifact_type"] == "publication_timezone_drift"
    assert payload["filters"]["timezone_offset_hours"] == 9
    assert payload["drift_items"][0]["platform"] == "x"


def test_invalid_options_are_rejected(capsys):
    assert publication_timezone_drift_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err

    with pytest.raises(ValueError, match="invalid platform"):
        build_publication_timezone_drift_report(_conn(), platform="threads", now=NOW)

    assert publication_timezone_drift_script.main(["--timezone-offset-hours", "24"]) == 2
    assert "between -23 and 23" in capsys.readouterr().err


def test_empty_state_for_missing_schema():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_publication_timezone_drift_report(conn, now=NOW)
    payload = json.loads(format_publication_timezone_drift_json(report))

    assert payload["empty_state"]["is_empty"] is True
    assert payload["missing_tables"] == ["publish_queue"]
