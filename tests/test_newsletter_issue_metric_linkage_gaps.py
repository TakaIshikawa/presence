"""Tests for newsletter issue metric linkage gap reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.newsletter_issue_metric_linkage_gaps import (
    build_newsletter_issue_metric_linkage_gaps_report,
    format_newsletter_issue_metric_linkage_gaps_json,
    format_newsletter_issue_metric_linkage_gaps_text,
)


NOW = datetime(2026, 5, 1, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_issue_metric_linkage_gaps.py"
spec = importlib.util.spec_from_file_location("newsletter_issue_metric_linkage_gaps_script", SCRIPT_PATH)
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
        """
        CREATE TABLE newsletter_sends (
            id INTEGER PRIMARY KEY,
            issue_id TEXT,
            sent_at TEXT
        );
        CREATE TABLE newsletter_engagement (
            id INTEGER PRIMARY KEY,
            newsletter_send_id INTEGER,
            issue_id TEXT,
            fetched_at TEXT
        );
        """
    )
    return conn


def test_report_flags_missing_orphan_mismatch_and_duplicate_snapshots():
    conn = _conn()
    conn.executescript(
        """
        INSERT INTO newsletter_sends VALUES (1, 'issue-a', '2026-05-01T08:00:00+00:00');
        INSERT INTO newsletter_sends VALUES (2, 'issue-b', '2026-05-01T09:00:00+00:00');
        INSERT INTO newsletter_sends VALUES (3, 'issue-c', '2026-05-01T10:00:00+00:00');
        INSERT INTO newsletter_sends VALUES (4, 'issue-d', '2026-05-01T11:00:00+00:00');
        INSERT INTO newsletter_engagement VALUES (10, 1, 'issue-a', '2026-05-01T12:00:00+00:00');
        INSERT INTO newsletter_engagement VALUES (11, NULL, 'issue-b', '2026-05-01T12:01:00+00:00');
        INSERT INTO newsletter_engagement VALUES (12, 3, 'issue-x', '2026-05-01T12:02:00+00:00');
        INSERT INTO newsletter_engagement VALUES (13, 4, 'issue-d', '2026-05-01T12:03:00+00:00');
        INSERT INTO newsletter_engagement VALUES (14, 4, 'issue-d', '2026-05-01T12:03:00+00:00');
        """
    )

    report = build_newsletter_issue_metric_linkage_gaps_report(conn, now=NOW)

    assert report["artifact_type"] == "newsletter_issue_metric_linkage_gaps"
    gap_types = [item["gap_type"] for item in report["items"]]
    assert gap_types.count("missing_metrics") == 0
    assert gap_types.count("orphan_metric") == 1
    assert gap_types.count("issue_id_mismatch") == 1
    assert gap_types.count("duplicate_metric_snapshot") == 2
    assert set(report["items"][0]) == {"send_id", "metric_id", "issue_id", "sent_at", "fetched_at", "gap_type"}


def test_missing_metrics_limit_and_schema_gaps():
    conn = _conn()
    conn.execute("INSERT INTO newsletter_sends VALUES (1, 'issue-a', '2026-05-01T08:00:00+00:00')")
    conn.execute("INSERT INTO newsletter_sends VALUES (2, 'issue-b', '2026-05-01T09:00:00+00:00')")

    report = build_newsletter_issue_metric_linkage_gaps_report(conn, limit=1, now=NOW)

    assert report["summary"]["gap_count"] == 2
    assert report["items"] == [
        {
            "send_id": 1,
            "metric_id": None,
            "issue_id": "issue-a",
            "sent_at": "2026-05-01T08:00:00+00:00",
            "fetched_at": None,
            "gap_type": "missing_metrics",
        }
    ]

    missing = build_newsletter_issue_metric_linkage_gaps_report(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["newsletter_engagement", "newsletter_sends"]

    bad = sqlite3.connect(":memory:")
    bad.execute("CREATE TABLE newsletter_sends (id INTEGER, issue_id TEXT, sent_at TEXT)")
    bad.execute("CREATE TABLE newsletter_engagement (id INTEGER, newsletter_send_id INTEGER)")
    schema_report = build_newsletter_issue_metric_linkage_gaps_report(bad, now=NOW)
    assert schema_report["missing_columns"] == {"newsletter_engagement": ["fetched_at", "issue_id"]}


def test_formatters_and_cli(tmp_path, monkeypatch, capsys):
    conn = _conn()
    conn.execute("INSERT INTO newsletter_sends VALUES (1, 'issue-a', '2026-05-01T08:00:00+00:00')")
    report = build_newsletter_issue_metric_linkage_gaps_report(conn, now=NOW)

    assert json.loads(format_newsletter_issue_metric_linkage_gaps_json(report))["artifact_type"] == "newsletter_issue_metric_linkage_gaps"
    assert "send_id | metric_id | issue_id" in format_newsletter_issue_metric_linkage_gaps_text(report)

    db_path = tmp_path / "newsletter.sqlite"
    disk = sqlite3.connect(db_path)
    disk.executescript(
        """
        CREATE TABLE newsletter_sends (id INTEGER PRIMARY KEY, issue_id TEXT, sent_at TEXT);
        CREATE TABLE newsletter_engagement (id INTEGER PRIMARY KEY, newsletter_send_id INTEGER, issue_id TEXT, fetched_at TEXT);
        INSERT INTO newsletter_sends VALUES (1, 'issue-a', '2026-05-01T08:00:00+00:00');
        """
    )
    disk.close()

    assert script.main(["--db", str(db_path), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["items"][0]["gap_type"] == "missing_metrics"
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Newsletter Issue Metric Linkage Gaps" in capsys.readouterr().out

    monkeypatch.setattr(script, "script_context", lambda: _script_context(sqlite3.connect(":memory:")))
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["missing_tables"] == ["newsletter_engagement", "newsletter_sends"]
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])
