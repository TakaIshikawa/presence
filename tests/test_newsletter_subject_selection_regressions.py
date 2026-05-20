"""Tests for newsletter subject selection regression reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.newsletter_subject_selection_regressions import (
    build_newsletter_subject_selection_regressions_report,
    build_newsletter_subject_selection_regressions_report_from_db,
    format_newsletter_subject_selection_regressions_json,
    format_newsletter_subject_selection_regressions_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_subject_selection_regressions.py"
spec = importlib.util.spec_from_file_location("newsletter_subject_selection_regressions_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _ts(days_ago: float) -> str:
    return (NOW - timedelta(days=days_ago)).strftime("%Y-%m-%d %H:%M:%S")


def _conn(path: str | Path = ":memory:") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE newsletter_subject_candidates (
            id INTEGER PRIMARY KEY,
            newsletter_send_id INTEGER,
            issue_id TEXT,
            subject TEXT,
            score REAL,
            rank INTEGER,
            source TEXT,
            selected INTEGER,
            created_at TEXT
        )"""
    )
    return conn


def _candidate(
    conn: sqlite3.Connection,
    *,
    newsletter_send_id: int | None = 1,
    issue_id: str | None = "issue-a",
    subject: str = "Subject",
    score: float = 8.0,
    rank: int = 1,
    selected: int = 0,
    days_ago: float = 1,
) -> None:
    conn.execute(
        """INSERT INTO newsletter_subject_candidates
           (newsletter_send_id, issue_id, subject, score, rank, source, selected, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (newsletter_send_id, issue_id, subject, score, rank, "model", selected, _ts(days_ago)),
    )
    conn.commit()


def test_clean_selection_has_no_regression():
    report = build_newsletter_subject_selection_regressions_report(
        [
            {"candidate_id": 1, "newsletter_send_id": 1, "issue_id": "a", "subject": "Best", "score": 9, "selected": 1},
            {"candidate_id": 2, "newsletter_send_id": 1, "issue_id": "a", "subject": "Other", "score": 8, "selected": 0},
        ],
        now=NOW,
    )

    assert report["artifact_type"] == "newsletter_subject_selection_regressions"
    assert report["total_regressions"] == 0
    assert report["summary"]["selected_pool_count"] == 1


def test_regressed_selection_reports_higher_scored_candidate():
    report = build_newsletter_subject_selection_regressions_report(
        [
            {"candidate_id": 1, "newsletter_send_id": 7, "issue_id": "a", "subject": "Chosen", "score": 6, "selected": True},
            {"candidate_id": 2, "newsletter_send_id": 7, "issue_id": "a", "subject": "Winner", "score": 9, "selected": False},
            {"candidate_id": 3, "newsletter_send_id": 7, "issue_id": "a", "subject": "Middle", "score": 8, "selected": False},
        ],
        min_score_gap=1.0,
        now=NOW,
    )

    assert report["total_regressions"] == 1
    item = report["regression_items"][0]
    assert item["newsletter_send_id"] == 7
    assert item["group_type"] == "newsletter_send_id"
    assert item["score_gap"] == 3
    assert item["best_candidate"]["candidate_id"] == 2
    assert item["higher_scored_candidate_count"] == 2
    assert report["issue_examples"] == report["regression_items"]


def test_db_adapter_filters_lookback_and_falls_back_to_issue_id_grouping():
    conn = _conn()
    _candidate(conn, newsletter_send_id=None, issue_id="fallback", subject="Selected", score=4, rank=2, selected=1, days_ago=1)
    _candidate(conn, newsletter_send_id=None, issue_id="fallback", subject="Better", score=7, rank=1, selected=0, days_ago=1)
    _candidate(conn, newsletter_send_id=2, issue_id="old", subject="Old selected", score=1, rank=2, selected=1, days_ago=20)
    _candidate(conn, newsletter_send_id=2, issue_id="old", subject="Old better", score=10, rank=1, selected=0, days_ago=20)

    report = build_newsletter_subject_selection_regressions_report_from_db(
        conn,
        lookback_days=7,
        min_score_gap=0.5,
        now=NOW,
    )

    assert report["summary"]["candidate_count"] == 2
    assert report["total_regressions"] == 1
    assert report["regression_items"][0]["group_type"] == "issue_id"
    assert report["regression_items"][0]["issue_id"] == "fallback"


def test_missing_table_returns_empty_report_with_metadata():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_newsletter_subject_selection_regressions_report_from_db(conn, now=NOW)

    assert report["missing_tables"] == ["newsletter_subject_candidates"]
    assert report["summary"]["candidate_count"] == 0
    assert report["regression_items"] == []


def test_formatters_are_stable_and_text_is_readable():
    report = build_newsletter_subject_selection_regressions_report(
        [
            {"candidate_id": 1, "newsletter_send_id": 1, "issue_id": "a", "subject": "Chosen", "score": 4, "selected": 1},
            {"candidate_id": 2, "newsletter_send_id": 1, "issue_id": "a", "subject": "Better", "score": 5, "selected": 0},
        ],
        now=NOW,
    )

    payload = json.loads(format_newsletter_subject_selection_regressions_json(report))
    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "newsletter_subject_selection_regressions"
    text = format_newsletter_subject_selection_regressions_text(report)
    assert "Newsletter Subject Selection Regressions" in text
    assert "selected=#1" in text
    assert "best=#2" in text


def test_cli_supports_db_text_json_context_and_validation(tmp_path, monkeypatch, capsys):
    db_path = tmp_path / "subjects.sqlite"
    conn = _conn(db_path)
    _candidate(conn, subject="Chosen", score=3, rank=2, selected=1)
    _candidate(conn, subject="Better", score=8, rank=1, selected=0)
    conn.close()

    assert script.main(["--db", str(db_path), "--min-score-gap", "1", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["total_regressions"] == 1
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Newsletter Subject Selection Regressions" in capsys.readouterr().out

    memory = _conn()
    monkeypatch.setattr(script, "script_context", lambda: _script_context(memory))
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["regression_items"] == []
    assert script.main(["--lookback-days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_builder_rejects_invalid_thresholds():
    with pytest.raises(ValueError, match="lookback_days must be positive"):
        build_newsletter_subject_selection_regressions_report([], lookback_days=0)
    with pytest.raises(ValueError, match="min_score_gap must be non-negative"):
        build_newsletter_subject_selection_regressions_report([], min_score_gap=-1)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_newsletter_subject_selection_regressions_report([], limit=0)
