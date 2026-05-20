"""Tests for GitHub activity content link orphan reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.github_activity_content_link_orphans import (
    build_github_activity_content_link_orphans_report_from_db,
    format_github_activity_content_link_orphans_json,
    format_github_activity_content_link_orphans_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "github_activity_content_link_orphans.py"
spec = importlib.util.spec_from_file_location("github_activity_content_link_orphans_script", SCRIPT_PATH)
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
             source_activity_ids TEXT
           );
           CREATE TABLE github_activity (
             id INTEGER PRIMARY KEY,
             activity_type TEXT,
             state TEXT,
             closed_at TEXT
           );"""
    )
    return conn


def test_report_flags_link_integrity_and_recent_unlinked_activity():
    conn = _conn()
    conn.executemany(
        "INSERT INTO github_activity (id, activity_type, state, closed_at) VALUES (?, ?, ?, ?)",
        [
            (1, "issue", "closed", "2026-05-19T12:00:00+00:00"),
            (2, "pull_request", "merged", "2026-04-18T12:00:00+00:00"),
            (3, "issue", "closed", "2026-05-19T12:00:00+00:00"),
        ],
    )
    conn.executemany(
        "INSERT INTO generated_content (id, source_activity_ids) VALUES (?, ?)",
        [
            (10, "[1]"),
            (11, "{bad"),
            (12, "[\"2\"]"),
            (13, "[99]"),
        ],
    )

    report = build_github_activity_content_link_orphans_report_from_db(conn, now=NOW, days=7)

    assert report["artifact_type"] == "github_activity_content_link_orphans"
    assert [finding["issue_type"] for finding in report["findings"]] == [
        "malformed_source_activity_ids",
        "invalid_activity_id",
        "missing_activity",
        "unlinked_recent_activity",
    ]
    assert report["totals"]["issue_counts"]["malformed_source_activity_ids"] == 1
    assert report["totals"]["issue_counts"]["invalid_activity_id"] == 1
    assert report["totals"]["issue_counts"]["missing_activity"] == 1
    assert report["totals"]["issue_counts"]["unlinked_recent_activity"] == 1
    assert report["totals"]["issue_counts_by_activity_type"]["issue"]["unlinked_recent_activity"] == 1


def test_activity_type_filter_limits_resolved_activity_findings():
    conn = _conn()
    conn.executemany(
        "INSERT INTO github_activity (id, activity_type, state, closed_at) VALUES (?, ?, ?, ?)",
        [
            (1, "issue", "closed", "2026-05-19T12:00:00+00:00"),
            (2, "pull_request", "closed", "2026-05-19T12:00:00+00:00"),
        ],
    )
    report = build_github_activity_content_link_orphans_report_from_db(conn, now=NOW, activity_type="issue")

    assert [finding["activity_id"] for finding in report["findings"]] == [1]
    assert report["filters"]["activity_type"] == "issue"


def test_formatters_and_cli(tmp_path, monkeypatch, capsys):
    conn = _conn()
    conn.execute("INSERT INTO generated_content (id, source_activity_ids) VALUES (1, '[4]')")
    conn.commit()
    report = build_github_activity_content_link_orphans_report_from_db(conn, now=NOW)

    assert json.loads(format_github_activity_content_link_orphans_json(report))["artifact_type"] == "github_activity_content_link_orphans"
    assert "missing_activity=1" in format_github_activity_content_link_orphans_text(report)

    db_path = tmp_path / "activity.sqlite"
    conn.backup(sqlite3.connect(db_path))
    assert script.main(["--db", str(db_path), "--format", "json", "--days", "7"]) == 0
    assert json.loads(capsys.readouterr().out)["totals"]["finding_count"] == 1
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "GitHub Activity Content Link Orphans" in capsys.readouterr().out

    memory = _conn()
    monkeypatch.setattr(script, "script_context", lambda: _script_context(memory))
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["findings"] == []


def test_missing_schema_and_invalid_days():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE generated_content (id INTEGER PRIMARY KEY)")
    report = build_github_activity_content_link_orphans_report_from_db(conn, now=NOW)
    assert report["missing_tables"] == ["github_activity"]
    assert report["missing_columns"] == {"generated_content": ["source_activity_ids"]}
    with pytest.raises(ValueError, match="days must be positive"):
        build_github_activity_content_link_orphans_report_from_db(_conn(), days=0)
