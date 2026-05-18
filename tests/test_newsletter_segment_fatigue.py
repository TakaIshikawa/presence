"""Tests for newsletter segment fatigue reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from evaluation.newsletter_segment_fatigue import (
    build_newsletter_segment_fatigue_report,
    build_newsletter_segment_fatigue_report_from_db,
    format_newsletter_segment_fatigue_json,
    format_newsletter_segment_fatigue_text,
)


NOW = datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_segment_fatigue.py"
spec = importlib.util.spec_from_file_location("newsletter_segment_fatigue_script", SCRIPT_PATH)
newsletter_segment_fatigue_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_segment_fatigue_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_detects_repeated_segments_openings_and_source_clusters_from_rows():
    rows = [
        {
            "issue_id": "n1",
            "sent_at": (NOW - timedelta(days=1)).isoformat(),
            "segments": ["Build Notes", "Launches"],
            "opening": "This week in product",
            "source_content_ids": ["1", "2"],
        },
        {
            "issue_id": "n2",
            "sent_at": (NOW - timedelta(days=2)).isoformat(),
            "segments": ["Build Notes", "Reads"],
            "opening": "This week in product",
            "source_content_ids": ["2", "1"],
        },
        {
            "issue_id": "n3",
            "sent_at": (NOW - timedelta(days=3)).isoformat(),
            "segments": ["Dispatch"],
            "opening": "A different intro",
            "source_content_ids": ["9"],
        },
    ]

    report = build_newsletter_segment_fatigue_report(rows, lookback=3, min_repeat=2, now=NOW)

    assert report["fatigue_score"] == 1.0
    assert report["repeated_segments"][0]["segment"] == "build notes"
    assert report["repeated_openings"][0]["opening"] == "this week in product"
    assert report["repeated_source_clusters"][0]["sources"] == ["1", "2"]
    assert report["recommendations"]
    assert set(json.loads(format_newsletter_segment_fatigue_json(report))) >= {
        "fatigue_score",
        "repeated_segments",
        "repeated_openings",
        "repeated_source_clusters",
        "recommendations",
    }


def test_lookback_limits_recent_issues():
    rows = [
        {"issue_id": "new", "sent_at": NOW.isoformat(), "segments": ["Fresh"], "opening": "Fresh open"},
        {
            "issue_id": "old",
            "sent_at": (NOW - timedelta(days=30)).isoformat(),
            "segments": ["Fresh"],
            "opening": "Fresh open",
        },
    ]

    report = build_newsletter_segment_fatigue_report(rows, lookback=1, now=NOW)

    assert report["totals"]["issue_count"] == 1
    assert report["repeated_segments"] == []
    assert "No newsletter segment fatigue found." in format_newsletter_segment_fatigue_text(report)


def test_db_loader_reads_newsletter_sends_metadata(db):
    for offset, issue in enumerate(("a", "b")):
        db.conn.execute(
            """INSERT INTO newsletter_sends
               (issue_id, subject, source_content_ids, metadata, sent_at)
               VALUES (?, ?, ?, ?, ?)""",
            (
                issue,
                "Weekly",
                json.dumps([1, 2]),
                json.dumps({"segments": ["Build Notes"], "opening": "This week in product"}),
                (NOW - timedelta(days=offset)).isoformat(),
            ),
        )
    db.conn.commit()

    report = build_newsletter_segment_fatigue_report_from_db(db, lookback=5, now=NOW)

    assert report["repeated_segments"][0]["issue_ids"] == ["a", "b"]
    assert report["repeated_source_clusters"][0]["sources"] == ["1", "2"]


def test_cli_outputs_json_and_text(db, file_db, capsys):
    for database, issue in ((db, "configured"), (file_db, "file-1"), (file_db, "file-2")):
        database.conn.execute(
            """INSERT INTO newsletter_sends
               (issue_id, subject, source_content_ids, metadata, sent_at)
               VALUES (?, 'Weekly', ?, ?, ?)""",
            (
                issue,
                json.dumps([3, 4]),
                json.dumps({"segments": ["Signals"], "opening": "Opening line"}),
                NOW.isoformat(),
            ),
        )
        database.conn.commit()

    with patch.object(newsletter_segment_fatigue_script, "script_context", wraps=lambda: _script_context(db)):
        assert newsletter_segment_fatigue_script.main(["--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["totals"]["issue_count"] == 1

    assert newsletter_segment_fatigue_script.main(["--db", str(file_db.db_path), "--format", "text"]) == 0
    text = capsys.readouterr().out
    assert "Newsletter Segment Fatigue" in text
    assert "signals" in text
    assert newsletter_segment_fatigue_script.main(["--min-repeat", "1"]) == 2
