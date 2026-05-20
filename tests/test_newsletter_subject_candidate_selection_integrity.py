"""Tests for newsletter subject candidate selection integrity reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.newsletter_subject_candidate_selection_integrity import (
    build_newsletter_subject_candidate_selection_integrity_report,
    format_newsletter_subject_candidate_selection_integrity_json,
    format_newsletter_subject_candidate_selection_integrity_text,
)


NOW = datetime(2026, 5, 1, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_subject_candidate_selection_integrity.py"
spec = importlib.util.spec_from_file_location("newsletter_subject_candidate_selection_integrity_script", SCRIPT_PATH)
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
        CREATE TABLE newsletter_subject_candidates (
            id INTEGER PRIMARY KEY,
            newsletter_send_id INTEGER,
            issue_id TEXT,
            week_start TEXT,
            week_end TEXT,
            subject TEXT,
            selected INTEGER,
            rank INTEGER,
            score REAL,
            created_at TEXT
        );
        """
    )
    return conn


def test_report_flags_all_selection_integrity_gaps_and_grouping_fallbacks():
    conn = _conn()
    conn.executescript(
        """
        INSERT INTO newsletter_subject_candidates VALUES (1, 10, 'issue-a', NULL, NULL, 'A', 0, 1, 0.9, '2026-05-01T08:00:00+00:00');
        INSERT INTO newsletter_subject_candidates VALUES (2, 10, 'issue-a', NULL, NULL, 'B', 0, 2, 0.8, '2026-05-01T08:01:00+00:00');
        INSERT INTO newsletter_subject_candidates VALUES (3, 11, 'issue-b', NULL, NULL, 'C', 1, 1, 0.9, '2026-05-01T08:02:00+00:00');
        INSERT INTO newsletter_subject_candidates VALUES (4, 11, 'issue-b', NULL, NULL, 'D', 1, 2, 0.7, '2026-05-01T08:03:00+00:00');
        INSERT INTO newsletter_subject_candidates VALUES (5, NULL, 'issue-c', NULL, NULL, 'E', 1, 2, 0.9, '2026-05-01T08:04:00+00:00');
        INSERT INTO newsletter_subject_candidates VALUES (6, NULL, NULL, '2026-05-04', '2026-05-10', 'F', 1, 1, 0.4, '2026-05-01T08:05:00+00:00');
        """
    )

    report = build_newsletter_subject_candidate_selection_integrity_report(conn, score_threshold=0.5, now=NOW)

    assert report["artifact_type"] == "newsletter_subject_candidate_selection_integrity"
    gap_types = [item["gap_type"] for item in report["items"]]
    assert gap_types == [
        "no_selected_candidate",
        "multiple_selected_candidates",
        "selected_not_top_ranked",
        "selected_not_top_ranked",
        "selected_below_threshold",
    ]
    assert {item["group_type"] for item in report["items"]} == {"newsletter_send_id", "issue_id", "week_window"}


def test_limit_schema_gaps_and_formatters():
    conn = _conn()
    conn.execute("INSERT INTO newsletter_subject_candidates VALUES (1, 1, 'issue-a', NULL, NULL, 'A', 0, 1, 0.9, ?)", (NOW.isoformat(),))
    report = build_newsletter_subject_candidate_selection_integrity_report(conn, limit=1, now=NOW)

    assert report["summary"]["gap_count"] == 1
    assert len(report["items"]) == 1
    assert json.loads(format_newsletter_subject_candidate_selection_integrity_json(report))["artifact_type"] == "newsletter_subject_candidate_selection_integrity"
    assert "group_key | candidate_id | issue_id" in format_newsletter_subject_candidate_selection_integrity_text(report)

    missing = build_newsletter_subject_candidate_selection_integrity_report(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["newsletter_subject_candidates"]

    bad = sqlite3.connect(":memory:")
    bad.execute("CREATE TABLE newsletter_subject_candidates (id INTEGER, selected INTEGER)")
    schema_report = build_newsletter_subject_candidate_selection_integrity_report(bad, now=NOW)
    assert schema_report["missing_columns"] == {"newsletter_subject_candidates": ["rank", "score"]}


def test_cli_supports_db_context_and_invalid_limit(tmp_path, monkeypatch, capsys):
    db_path = tmp_path / "subjects.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE newsletter_subject_candidates (
            id INTEGER PRIMARY KEY, newsletter_send_id INTEGER, issue_id TEXT, week_start TEXT, week_end TEXT,
            subject TEXT, selected INTEGER, rank INTEGER, score REAL, created_at TEXT
        );
        INSERT INTO newsletter_subject_candidates VALUES (1, 1, 'issue-a', NULL, NULL, 'A', 0, 1, 0.9, '2026-05-01T08:00:00+00:00');
        """
    )
    conn.close()

    assert script.main(["--db", str(db_path), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["items"][0]["gap_type"] == "no_selected_candidate"
    assert script.main(["--db", str(db_path), "--format", "text", "--score-threshold", "0.8"]) == 0
    assert "Newsletter Subject Candidate Selection Integrity" in capsys.readouterr().out

    monkeypatch.setattr(script, "script_context", lambda: _script_context(sqlite3.connect(":memory:")))
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["missing_tables"] == ["newsletter_subject_candidates"]
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])
