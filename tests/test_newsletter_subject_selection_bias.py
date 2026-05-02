"""Tests for newsletter subject selection-bias reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.newsletter_subject_selection_bias import (
    SELECTED_BELOW_MEDIAN_SCORE,
    SELECTED_NOT_RANK_1,
    build_newsletter_subject_selection_bias_report,
    format_newsletter_subject_selection_bias_json,
    format_newsletter_subject_selection_bias_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "newsletter_subject_selection_bias.py"
)
spec = importlib.util.spec_from_file_location(
    "newsletter_subject_selection_bias_script",
    SCRIPT_PATH,
)
newsletter_subject_selection_bias_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_subject_selection_bias_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _candidate(
    db,
    send_id: int,
    subject: str,
    *,
    issue_id: str,
    score: float,
    source: str = "heuristic",
    rank: int = 1,
    selected: bool = False,
    created_at: datetime | None = None,
) -> int:
    cursor = db.conn.execute(
        """INSERT INTO newsletter_subject_candidates
           (newsletter_send_id, issue_id, subject, score, rationale, source, rank, selected, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            send_id,
            issue_id,
            subject,
            score,
            "ok",
            source,
            rank,
            1 if selected else 0,
            (created_at or NOW).isoformat(),
        ),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def test_selection_bias_flags_non_rank_1_and_below_median_candidates(db):
    selected_id = _candidate(
        db,
        10,
        "Try a quieter launch",
        issue_id="issue-1",
        score=4.0,
        source="archive",
        rank=3,
        selected=True,
    )
    _candidate(db, 10, "Launch without the scramble", issue_id="issue-1", score=9.0)
    _candidate(db, 10, "Better release notes", issue_id="issue-1", score=7.0, rank=2)

    _candidate(
        db,
        11,
        "A clean publish path",
        issue_id="issue-2",
        score=8.5,
        source="llm",
        selected=True,
    )
    _candidate(db, 11, "Publish with less drag", issue_id="issue-2", score=7.0, rank=2)

    report = build_newsletter_subject_selection_bias_report(db, now=NOW)

    payload = report.to_dict()
    assert payload["artifact_type"] == "newsletter_subject_selection_bias"
    assert {
        "artifact_type",
        "generated_at",
        "window_days",
        "totals",
        "source_bias",
        "rank_distribution",
        "flagged_issues",
    }.issubset(payload)
    assert report.totals["selected_pool_count"] == 2
    assert report.totals["flagged_issue_count"] == 1
    assert report.totals["average_selected_score_delta_vs_best"] == pytest.approx(-2.5)
    assert report.source_bias["selected_source_counts"] == {"archive": 1, "llm": 1}
    assert report.rank_distribution["selected_rank_distribution"] == {"1": 1, "3": 1}

    issue = report.flagged_issues[0]
    assert issue.selected_candidate_id == selected_id
    assert issue.issue_id == "issue-1"
    assert issue.issue_codes == (
        SELECTED_NOT_RANK_1,
        SELECTED_BELOW_MEDIAN_SCORE,
    )
    assert issue.median_score == 7.0
    assert issue.score_delta_vs_best == -5.0


def test_filters_text_json_cli_and_invalid_args_are_stable(db, monkeypatch, capsys):
    old_at = NOW - timedelta(days=90)
    _candidate(db, 20, "Old selected", issue_id="old", score=1, selected=True, created_at=old_at)
    _candidate(db, 20, "Old best", issue_id="old", score=9, rank=2, created_at=old_at)
    _candidate(db, 21, "Filtered selected", issue_id="filtered", score=5, selected=True)
    _candidate(db, 21, "Filtered best", issue_id="filtered", score=9, rank=2)
    _candidate(db, 22, "Too small", issue_id="small", score=3, selected=True)

    report = build_newsletter_subject_selection_bias_report(
        db,
        days=14,
        min_candidates_per_issue=2,
        now=NOW,
    )
    payload = json.loads(format_newsletter_subject_selection_bias_json(report))
    text = format_newsletter_subject_selection_bias_text(report)

    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["window_days"] == 14
    assert payload["filters"]["min_candidates_per_issue"] == 2
    assert payload["totals"]["candidate_pool_count"] == 1
    assert payload["flagged_issues"][0]["issue_id"] == "filtered"
    assert "Newsletter Subject Selection Bias Report" in text
    assert "issue=filtered" in text
    assert "issue=old" not in text
    assert "issue=small" not in text

    monkeypatch.setattr(
        newsletter_subject_selection_bias_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        newsletter_subject_selection_bias_script,
        "build_newsletter_subject_selection_bias_report",
        lambda db, **kwargs: build_newsletter_subject_selection_bias_report(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    assert newsletter_subject_selection_bias_script.main(
        ["--days", "14", "--min-candidates-per-issue", "2", "--format", "json"]
    ) == 0
    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["flagged_issues"][0]["issue_id"] == "filtered"

    assert newsletter_subject_selection_bias_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
    assert newsletter_subject_selection_bias_script.main(
        ["--min-candidates-per-issue", "0"]
    ) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_empty_and_legacy_schema_return_valid_empty_reports():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_newsletter_subject_selection_bias_report(conn, now=NOW)

    assert report.flagged_issues == ()
    assert report.missing_tables == ("newsletter_subject_candidates",)
    assert report.totals["candidate_count"] == 0
    assert "Missing tables: newsletter_subject_candidates" in (
        format_newsletter_subject_selection_bias_text(report)
    )

    conn.execute(
        """CREATE TABLE newsletter_subject_candidates (
               id INTEGER PRIMARY KEY,
               newsletter_send_id INTEGER,
               issue_id TEXT,
               subject TEXT NOT NULL
           )"""
    )
    legacy_report = build_newsletter_subject_selection_bias_report(conn, now=NOW)

    assert legacy_report.flagged_issues == ()
    assert legacy_report.missing_columns == {
        "newsletter_subject_candidates": (
            "created_at",
            "rank",
            "score",
            "selected",
            "source",
        )
    }
    assert legacy_report.source_bias["selected_source_counts"] == {}

    with pytest.raises(ValueError, match="days must be positive"):
        build_newsletter_subject_selection_bias_report(conn, days=0, now=NOW)
    with pytest.raises(ValueError, match="min_candidates_per_issue must be positive"):
        build_newsletter_subject_selection_bias_report(
            conn,
            min_candidates_per_issue=0,
            now=NOW,
        )
    conn.close()
