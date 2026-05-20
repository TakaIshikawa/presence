"""Tests for content feedback reviewer load reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.content_feedback_reviewer_load import (
    build_content_feedback_reviewer_load_report,
    build_content_feedback_reviewer_load_report_from_db,
    format_content_feedback_reviewer_load_json,
    format_content_feedback_reviewer_load_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_feedback_reviewer_load.py"
spec = importlib.util.spec_from_file_location("content_feedback_reviewer_load_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _ts(days_ago: int) -> str:
    return (NOW - timedelta(days=days_ago)).strftime("%Y-%m-%d %H:%M:%S")


def _conn(path: str | Path = ":memory:") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE content_feedback (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            feedback_type TEXT,
            notes TEXT,
            status TEXT,
            severity TEXT,
            reviewer TEXT,
            created_at TEXT
        )"""
    )
    return conn


def _feedback(
    conn: sqlite3.Connection,
    *,
    feedback_id: int,
    status: str = "open",
    severity: str = "medium",
    reviewer: str | None = "alex",
    days_ago: int = 1,
) -> None:
    conn.execute(
        """INSERT INTO content_feedback
           (id, content_id, feedback_type, notes, status, severity, reviewer, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (feedback_id, feedback_id + 100, "revise", "notes", status, severity, reviewer, _ts(days_ago)),
    )
    conn.commit()


def test_builder_groups_unresolved_load_by_reviewer_status_and_severity():
    report = build_content_feedback_reviewer_load_report(
        [
            {"feedback_id": 1, "content_id": 10, "reviewer": "alex", "status": "open", "severity": "high", "created_at": _ts(10)},
            {"feedback_id": 2, "content_id": 11, "reviewer": "alex", "status": "open", "severity": "high", "created_at": _ts(2)},
            {"feedback_id": 3, "content_id": 12, "reviewer": "sam", "status": "triage", "severity": "low", "created_at": _ts(35)},
            {"feedback_id": 4, "content_id": 13, "reviewer": "sam", "status": "resolved", "severity": "high", "created_at": _ts(40)},
        ],
        now=NOW,
    )

    assert report["artifact_type"] == "content_feedback_reviewer_load"
    assert report["totals"]["rows_scanned"] == 4
    assert report["totals"]["unresolved_count"] == 3
    assert report["aging_buckets"]["31d+"] == 1
    assert report["reviewer_summary"][0]["reviewer"] == "alex"
    assert report["reviewer_summary"][0]["unresolved_count"] == 2
    assert [item["feedback_id"] for item in report["unresolved_examples"]] == [3, 1, 2]


def test_custom_unresolved_status_filter_excludes_other_open_statuses():
    report = build_content_feedback_reviewer_load_report(
        [
            {"feedback_id": 1, "status": "open", "reviewer": "alex", "created_at": _ts(1)},
            {"feedback_id": 2, "status": "triage", "reviewer": "sam", "created_at": _ts(1)},
        ],
        unresolved_statuses="triage",
        now=NOW,
    )

    assert report["filters"]["unresolved_statuses"] == ["triage"]
    assert report["totals"]["unresolved_count"] == 1
    assert report["unresolved_examples"][0]["feedback_id"] == 2


def test_db_loader_falls_back_to_unassigned_and_partial_schema():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE content_feedback (id INTEGER PRIMARY KEY, content_id INTEGER, status TEXT, created_at TEXT)")
    conn.execute("INSERT INTO content_feedback VALUES (?, ?, ?, ?)", (1, 42, "open", _ts(5)))
    conn.commit()

    report = build_content_feedback_reviewer_load_report_from_db(conn, now=NOW)

    assert report["missing_schema"]["missing_columns"]["content_feedback"] == [
        "assigned_to",
        "assignee",
        "owner",
        "reviewer",
        "reviewer_id",
        "severity",
    ]
    assert report["reviewer_summary"][0]["reviewer"] == "unassigned"
    assert report["unresolved_examples"][0]["status"] == "open"


def test_missing_content_feedback_table_returns_empty_report_with_metadata():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_content_feedback_reviewer_load_report_from_db(conn, now=NOW)

    assert report["missing_schema"]["missing_tables"] == ["content_feedback"]
    assert report["reviewer_summary"] == []
    assert report["unresolved_examples"] == []


def test_formatters_are_stable_and_text_is_readable():
    report = build_content_feedback_reviewer_load_report(
        [{"feedback_id": 1, "reviewer": "", "status": "open", "severity": "", "created_at": _ts(4)}],
        now=NOW,
    )

    payload = json.loads(format_content_feedback_reviewer_load_json(report))
    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "content_feedback_reviewer_load"
    text = format_content_feedback_reviewer_load_text(report)
    assert "Content Feedback Reviewer Load" in text
    assert "reviewer=unassigned" in text
    assert "Aging buckets:" in text


def test_cli_supports_db_json_text_and_argument_validation(tmp_path, monkeypatch, capsys):
    db_path = tmp_path / "feedback.sqlite"
    conn = _conn(db_path)
    _feedback(conn, feedback_id=1, status="triage", reviewer="")
    conn.close()

    assert script.main(["--db", str(db_path), "--format", "json", "--unresolved-statuses", "triage"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "content_feedback_reviewer_load"
    assert script.main(["--db", str(db_path), "--format", "text", "--limit", "5"]) == 0
    assert "Content Feedback Reviewer Load" in capsys.readouterr().out

    memory = _conn()
    monkeypatch.setattr(script, "script_context", lambda: _script_context(memory))
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["reviewer_summary"] == []
    assert script.main(["--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_builder_rejects_invalid_limit():
    with pytest.raises(ValueError, match="limit must be positive"):
        build_content_feedback_reviewer_load_report([], limit=0)
