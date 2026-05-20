"""Tests for newsletter subject score calibration reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.newsletter_subject_score_calibration import (
    build_newsletter_subject_score_calibration_report,
    build_newsletter_subject_score_calibration_report_from_db,
    format_newsletter_subject_score_calibration_json,
    format_newsletter_subject_score_calibration_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_subject_score_calibration.py"
spec = importlib.util.spec_from_file_location("newsletter_subject_score_calibration_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn(path: Path | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(path or ":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE newsletter_sends (
            id INTEGER PRIMARY KEY,
            issue_id TEXT,
            subscriber_count INTEGER
        );
        CREATE TABLE newsletter_subject_candidates (
            id INTEGER PRIMARY KEY,
            newsletter_send_id INTEGER,
            issue_id TEXT,
            subject TEXT,
            score REAL,
            selected INTEGER,
            source TEXT,
            rank INTEGER,
            created_at TEXT
        );
        CREATE TABLE newsletter_engagement (
            id INTEGER PRIMARY KEY,
            newsletter_send_id INTEGER,
            issue_id TEXT,
            opens INTEGER,
            clicks INTEGER,
            unsubscribes INTEGER,
            fetched_at TEXT
        );
        """
    )
    return conn


def test_builder_flags_over_scored_selected_and_under_scored_unselected():
    rows = [
        _row(1, 1, "issue-a", "Weak selected", 9.0, 1, "llm", 10, 1, 2, 100),
        _row(2, 1, "issue-a", "Alternative", 4.0, 0, "heuristic", 10, 1, 2, 100),
        _row(3, 2, "issue-b", "Strong selected", 9.0, 1, "llm", 50, 10, 0, 100),
        _row(4, 2, "issue-b", "Under-rated alt", 5.0, 0, "heuristic", 50, 10, 0, 100),
    ]

    report = build_newsletter_subject_score_calibration_report(rows, now=NOW, min_calibration_gap=0.01)

    assert report["artifact_type"] == "newsletter_subject_score_calibration"
    assert report["summary"]["over_scored_selected_count"] == 1
    assert report["over_scored_selected"][0]["candidate_id"] == 1
    assert report["over_scored_selected"][0]["score_outcome_gap"] == 0.84
    assert report["summary"]["under_scored_unselected_count"] == 1
    assert report["under_scored_unselected"][0]["candidate_id"] == 4
    assert report["under_scored_unselected"][0]["selected_score_gap"] == 0.4
    assert {item["source"] for item in report["source_calibration_gap"]} == {"heuristic", "llm"}


def test_db_adapter_joins_latest_engagement_by_send_or_issue():
    conn = _conn()
    conn.executemany("INSERT INTO newsletter_sends VALUES (?, ?, ?)", [(1, "issue-a", 100), (2, "issue-b", 200)])
    conn.executemany(
        "INSERT INTO newsletter_subject_candidates VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (1, 1, "issue-a", "Selected", 8.0, 1, "llm", 1, "2026-05-19T00:00:00+00:00"),
            (2, None, "issue-b", "Issue fallback", 2.0, 0, "heuristic", 2, "2026-05-19T00:00:00+00:00"),
        ],
    )
    conn.executemany(
        "INSERT INTO newsletter_engagement VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (1, 1, "issue-a", 5, 1, 0, "2026-05-19T01:00:00+00:00"),
            (2, 1, "issue-a", 60, 8, 0, "2026-05-20T01:00:00+00:00"),
            (3, None, "issue-b", 100, 20, 0, "2026-05-20T02:00:00+00:00"),
        ],
    )

    report = build_newsletter_subject_score_calibration_report_from_db(
        conn,
        now=NOW,
        high_outcome_threshold=0.3,
        min_calibration_gap=0.01,
    )

    assert report["summary"]["candidate_count"] == 2
    assert report["summary"]["outcome_pool_count"] == 2
    selected = next(item for item in report["source_calibration_gap"] if item["source"] == "llm")
    assert selected["avg_outcome_metric"] == 0.76


def test_missing_tables_returns_empty_metadata():
    report = build_newsletter_subject_score_calibration_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert report["missing_tables"] == ["newsletter_subject_candidates", "newsletter_engagement"]
    assert report["summary"]["candidate_count"] == 0


def test_formatters_cli_and_validation(tmp_path, capsys):
    report = build_newsletter_subject_score_calibration_report(
        [_row(1, 1, "issue-a", "Weak selected", 9.0, 1, "llm", 10, 1, 2, 100)],
        now=NOW,
    )
    payload = json.loads(format_newsletter_subject_score_calibration_json(report))
    assert list(payload) == sorted(payload)
    assert "Newsletter Subject Score Calibration" in format_newsletter_subject_score_calibration_text(report)

    db_path = tmp_path / "newsletter.sqlite"
    conn = _conn(db_path)
    conn.execute("INSERT INTO newsletter_sends VALUES (1, 'issue-a', 100)")
    conn.execute(
        "INSERT INTO newsletter_subject_candidates VALUES (1, 1, 'issue-a', 'Selected', 9.0, 1, 'llm', 1, '2026-05-19T00:00:00+00:00')"
    )
    conn.execute("INSERT INTO newsletter_engagement VALUES (1, 1, 'issue-a', 10, 1, 2, '2026-05-20T00:00:00+00:00')")
    conn.commit()
    conn.close()

    assert script.main(["--db", str(db_path), "--format", "json", "--limit", "5"]) == 0
    assert '"artifact_type": "newsletter_subject_score_calibration"' in capsys.readouterr().out
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Newsletter Subject Score Calibration" in capsys.readouterr().out
    assert script.main(["--lookback-days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_invalid_builder_arguments_raise():
    with pytest.raises(ValueError, match="lookback_days must be positive"):
        build_newsletter_subject_score_calibration_report([], lookback_days=0, now=NOW)
    with pytest.raises(ValueError, match="min_score_gap must be non-negative"):
        build_newsletter_subject_score_calibration_report([], min_score_gap=-0.1, now=NOW)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_newsletter_subject_score_calibration_report([], limit=0, now=NOW)


def _row(
    candidate_id: int,
    newsletter_send_id: int | None,
    issue_id: str,
    subject: str,
    score: float,
    selected: int,
    source: str,
    opens: int,
    clicks: int,
    unsubscribes: int,
    subscriber_count: int,
) -> dict:
    return {
        "candidate_id": candidate_id,
        "newsletter_send_id": newsletter_send_id,
        "issue_id": issue_id,
        "subject": subject,
        "score": score,
        "selected": selected,
        "source": source,
        "rank": candidate_id,
        "candidate_created_at": "2026-05-19T00:00:00+00:00",
        "engagement_fetched_at": "2026-05-20T00:00:00+00:00",
        "opens": opens,
        "clicks": clicks,
        "unsubscribes": unsubscribes,
        "subscriber_count": subscriber_count,
    }
