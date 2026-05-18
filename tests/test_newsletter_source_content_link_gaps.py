"""Tests for newsletter source content link gap reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.newsletter_source_content_link_gaps import (
    build_newsletter_source_content_link_gaps_report,
    build_newsletter_source_content_link_gaps_report_from_db,
    format_newsletter_source_content_link_gaps_json,
    format_newsletter_source_content_link_gaps_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_source_content_link_gaps.py"
spec = importlib.util.spec_from_file_location("newsletter_source_content_link_gaps_script", SCRIPT_PATH)
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
            status TEXT,
            sent_at TEXT,
            source_content_ids TEXT
        );
        CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            published INTEGER,
            status TEXT
        );
        """
    )
    return conn


def test_builder_flags_malformed_missing_duplicate_abandoned_and_unpublished_sources():
    report = build_newsletter_source_content_link_gaps_report(
        [
            {"id": 1, "issue_id": "bad-json", "status": "sent", "source_content_ids": "{bad"},
            {"id": 2, "issue_id": "gaps", "status": "sent", "source_content_ids": json.dumps([1, 2, 3, 3, 999, "x"])},
        ],
        [
            {"id": 1, "published": -1, "status": "abandoned"},
            {"id": 2, "published": 0, "status": "draft"},
            {"id": 3, "published": 1, "status": "published"},
        ],
        now=NOW,
    )

    counts = report["summary"]["by_issue_type"]
    assert report["artifact_type"] == "newsletter_source_content_link_gaps"
    assert counts["malformed_source_content_ids"] == 2
    assert counts["missing_generated_content"] == 1
    assert counts["duplicate_source_content_id"] == 1
    assert counts["abandoned_source_content"] == 1
    assert counts["unpublished_source_content"] == 1


def test_unpublished_and_abandoned_sources_only_flag_for_sent_newsletters():
    report = build_newsletter_source_content_link_gaps_report(
        [{"id": 1, "status": "draft", "source_content_ids": json.dumps([1, 2])}],
        [{"id": 1, "published": -1}, {"id": 2, "published": 0}],
        status="all",
        now=NOW,
    )

    assert report["summary"]["finding_count"] == 0
    assert report["summary"]["referenced_source_count"] == 2


def test_db_adapter_reads_sends_and_content_and_handles_missing_tables():
    conn = _conn()
    conn.execute("INSERT INTO generated_content VALUES (1, 1, 'published')")
    conn.execute("INSERT INTO generated_content VALUES (2, -1, 'abandoned')")
    conn.execute(
        "INSERT INTO newsletter_sends VALUES (10, 'issue', 'sent', ?, ?)",
        (NOW.isoformat(), json.dumps([1, 2, 42])),
    )

    report = build_newsletter_source_content_link_gaps_report_from_db(conn, days=7, now=NOW)

    assert report["summary"]["send_count"] == 1
    assert report["summary"]["by_issue_type"]["missing_generated_content"] == 1
    assert report["summary"]["by_issue_type"]["abandoned_source_content"] == 1

    empty = build_newsletter_source_content_link_gaps_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert empty["summary"]["send_count"] == 0
    assert empty["findings"] == []


def test_status_filter_and_limit_are_applied():
    report = build_newsletter_source_content_link_gaps_report(
        [
            {"id": 1, "status": "draft", "source_content_ids": "[99]"},
            {"id": 2, "status": "sent", "source_content_ids": "[98]"},
            {"id": 3, "status": "sent", "source_content_ids": "[97]"},
        ],
        [],
        limit=1,
        now=NOW,
    )

    assert report["summary"]["send_count"] == 2
    assert report["summary"]["finding_count"] == 2
    assert report["summary"]["shown_count"] == 1
    assert len(report["findings"]) == 1


def test_json_and_text_formatters_are_stable():
    report = build_newsletter_source_content_link_gaps_report(
        [{"id": 1, "issue_id": "json", "status": "sent", "source_content_ids": "[123]"}],
        [],
        now=NOW,
    )

    payload = json.loads(format_newsletter_source_content_link_gaps_json(report))
    assert payload["artifact_type"] == "newsletter_source_content_link_gaps"
    assert list(payload) == sorted(payload)
    text = format_newsletter_source_content_link_gaps_text(report)
    assert "Newsletter Source Content Link Gaps" in text
    assert "send_id | issue_id | status" in text


def test_cli_supports_db_json_text_and_invalid_numbers(tmp_path, monkeypatch, capsys):
    db_path = tmp_path / "newsletter.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE newsletter_sends (
            id INTEGER PRIMARY KEY,
            issue_id TEXT,
            status TEXT,
            sent_at TEXT,
            source_content_ids TEXT
        );
        CREATE TABLE generated_content (id INTEGER PRIMARY KEY, published INTEGER, status TEXT);
        """
    )
    conn.execute("INSERT INTO newsletter_sends VALUES (1, 'cli', 'sent', ?, '[123]')", (NOW.isoformat(),))
    conn.close()

    assert script.main(["--db", str(db_path), "--days", "7", "--status", "sent", "--limit", "5", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "newsletter_source_content_link_gaps"
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "missing_generated_content" in capsys.readouterr().out

    monkeypatch.setattr(script, "script_context", lambda: _script_context(sqlite3.connect(":memory:")))
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["findings"] == []
    with pytest.raises(SystemExit):
        script.parse_args(["--days", "0"])
