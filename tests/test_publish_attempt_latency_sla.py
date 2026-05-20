"""Tests for publish attempt latency SLA reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.publish_attempt_latency_sla import (
    build_publish_attempt_latency_sla_report_from_db,
    format_publish_attempt_latency_sla_json,
    format_publish_attempt_latency_sla_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publish_attempt_latency_sla.py"
spec = importlib.util.spec_from_file_location("publish_attempt_latency_sla_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE generated_content (
             id INTEGER PRIMARY KEY,
             content_type TEXT
           );
           CREATE TABLE publish_queue (
             id INTEGER PRIMARY KEY,
             content_id INTEGER,
             platform TEXT,
             status TEXT,
             scheduled_at TEXT,
             published_at TEXT
           );
           CREATE TABLE publication_attempts (
             id INTEGER PRIMARY KEY,
             queue_id INTEGER,
             content_id INTEGER,
             platform TEXT,
             attempted_at TEXT,
             success INTEGER,
             status TEXT,
             failed_at TEXT,
             succeeded_at TEXT
           );"""
    )
    return conn


def test_successful_publish_breaches_first_attempt_and_success_sla():
    conn = _conn()
    conn.execute("INSERT INTO generated_content VALUES (10, 'x_post')")
    conn.execute("INSERT INTO publish_queue VALUES (1, 10, 'x', 'published', '2026-05-20T08:00:00+00:00', NULL)")
    conn.executemany(
        "INSERT INTO publication_attempts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (1, 1, 10, "x", "2026-05-20T08:20:00+00:00", 0, "failed", "2026-05-20T08:20:00+00:00", None),
            (2, 1, 10, "x", "2026-05-20T10:30:00+00:00", 1, "success", None, "2026-05-20T10:30:00+00:00"),
        ],
    )

    report = build_publish_attempt_latency_sla_report_from_db(conn, now=NOW)

    stages = [finding["breached_stage"] for finding in report["findings"]]
    assert stages == ["success", "first_attempt"]
    success = report["findings"][0]
    assert success["queue_item_id"] == "1"
    assert success["content_id"] == "10"
    assert success["platform"] == "x"
    assert success["content_type"] == "x_post"
    assert success["latency_bucket"] == "1h-6h"
    assert success["threshold_minutes"] == 120


def test_terminal_failure_breach_is_reported():
    conn = _conn()
    conn.execute("INSERT INTO generated_content VALUES (20, 'thread')")
    conn.execute("INSERT INTO publish_queue VALUES (2, 20, 'bluesky', 'failed', '2026-05-20T01:00:00+00:00', NULL)")
    conn.executemany(
        "INSERT INTO publication_attempts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (1, 2, 20, "bluesky", "2026-05-20T01:20:00+00:00", 0, "failed", "2026-05-20T01:20:00+00:00", None),
            (2, 2, 20, "bluesky", "2026-05-20T05:00:00+00:00", 0, "failed", "2026-05-20T05:00:00+00:00", None),
        ],
    )

    report = build_publish_attempt_latency_sla_report_from_db(conn, now=NOW)

    terminal = next(finding for finding in report["findings"] if finding["breached_stage"] == "terminal_failure")
    assert terminal["latency_minutes"] == 240
    assert terminal["threshold_minutes"] == 180


def test_missing_timestamps_are_counted_not_reported():
    conn = _conn()
    conn.execute("INSERT INTO publish_queue VALUES (3, 30, 'x', 'queued', NULL, NULL)")
    conn.execute("INSERT INTO publication_attempts VALUES (1, 3, 30, 'x', '2026-05-20T08:00:00+00:00', 1, 'success', NULL, NULL)")

    report = build_publish_attempt_latency_sla_report_from_db(conn, now=NOW)

    assert report["findings"] == []
    assert report["summary"]["missing_ready_timestamp_count"] == 1


def test_platform_specific_thresholds_and_no_breach_cases():
    conn = _conn()
    conn.executemany("INSERT INTO generated_content VALUES (?, ?)", [(40, "post"), (41, "post")])
    conn.executemany(
        "INSERT INTO publish_queue VALUES (?, ?, ?, ?, ?, ?)",
        [
            (4, 40, "x", "published", "2026-05-20T08:00:00+00:00", None),
            (5, 41, "bluesky", "published", "2026-05-20T08:00:00+00:00", None),
        ],
    )
    conn.executemany(
        "INSERT INTO publication_attempts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (1, 4, 40, "x", "2026-05-20T08:20:00+00:00", 1, "success", None, None),
            (2, 5, 41, "bluesky", "2026-05-20T08:20:00+00:00", 1, "success", None, None),
        ],
    )

    report = build_publish_attempt_latency_sla_report_from_db(conn, now=NOW)

    assert [(item["platform"], item["breached_stage"]) for item in report["findings"]] == [("x", "first_attempt")]
    assert report["findings"][0]["threshold_minutes"] == 15


def test_formatters_cli_schema_and_argument_validation(tmp_path, monkeypatch, capsys):
    conn = _conn()
    conn.execute("INSERT INTO publish_queue VALUES (1, 10, 'x', 'published', '2026-05-20T08:00:00+00:00', NULL)")
    conn.execute("INSERT INTO publication_attempts VALUES (1, 1, 10, 'x', '2026-05-20T10:00:00+00:00', 1, 'success', NULL, NULL)")
    conn.commit()
    report = build_publish_attempt_latency_sla_report_from_db(conn, now=NOW)
    assert json.loads(format_publish_attempt_latency_sla_json(report))["artifact_type"] == "publish_attempt_latency_sla"
    assert "Publish Attempt Latency SLA" in format_publish_attempt_latency_sla_text(report)

    db_path = tmp_path / "publish.sqlite"
    dest = sqlite3.connect(db_path)
    conn.backup(dest)
    dest.close()
    assert script.main(["--db", str(db_path), "--format", "json", "--limit", "5"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["breach_count"] == 1
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Publish Attempt Latency SLA" in capsys.readouterr().out

    monkeypatch.setattr(script, "script_context", lambda: _script_context(sqlite3.connect(":memory:")))
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["missing_tables"] == ["publication_attempts", "publish_queue"]
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])
