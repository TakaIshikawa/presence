"""Tests for content topic assignment gap reporting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.content_topic_assignment_gaps import (
    build_content_topic_assignment_gaps_report,
    build_content_topic_assignment_gaps_report_from_db,
    format_content_topic_assignment_gaps_json,
    format_content_topic_assignment_gaps_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_topic_assignment_gaps.py"
spec = importlib.util.spec_from_file_location("content_topic_assignment_gaps_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _ts(days_ago: int) -> str:
    return (NOW - timedelta(days=days_ago)).isoformat()


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            content_type TEXT,
            published INTEGER,
            published_at TEXT,
            status TEXT,
            eval_score REAL,
            created_at TEXT
        )"""
    )
    conn.execute(
        """CREATE TABLE content_topics (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            topic TEXT,
            subtopic TEXT,
            confidence REAL,
            created_at TEXT
        )"""
    )
    return conn


def test_builder_flags_missing_low_confidence_and_duplicate_assignments():
    report = build_content_topic_assignment_gaps_report(
        [
            {"id": 1, "content_type": "x_post", "published": 1, "created_at": _ts(1)},
            {"id": 2, "content_type": "blog_post", "published": 0, "eval_score": 0.9, "created_at": _ts(1)},
            {"id": 3, "content_type": "x_thread", "published": 0, "eval_score": 0.2, "created_at": _ts(1)},
        ],
        [
            {"id": 10, "content_id": 2, "topic": "ai", "subtopic": "evals", "confidence": 0.4},
            {"id": 11, "content_id": 2, "topic": "ai", "subtopic": "evals", "confidence": 0.8},
        ],
        confidence_threshold=0.6,
        now=NOW,
    )

    assert report["artifact_type"] == "content_topic_assignment_gaps"
    assert report["summary"]["by_issue_type"] == {
        "duplicate_topic_assignment": 1,
        "low_confidence_topic_assignment": 1,
        "missing_topic_assignment": 1,
    }
    assert [item["issue_type"] for item in report["gap_items"]] == [
        "missing_topic_assignment",
        "duplicate_topic_assignment",
        "low_confidence_topic_assignment",
    ]


def test_db_loader_filters_by_created_at_window_and_collapses_duplicates():
    conn = _conn()
    conn.executemany(
        "INSERT INTO generated_content VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (1, "x_post", 1, None, None, 0.1, _ts(1)),
            (2, "x_post", 1, None, None, 0.1, _ts(90)),
            (3, "blog_post", 0, None, None, 0.9, _ts(1)),
        ],
    )
    conn.executemany(
        "INSERT INTO content_topics VALUES (?, ?, ?, ?, ?, ?)",
        [
            (10, 3, "ai", "evals", 0.4, _ts(1)),
            (11, 3, "ai", "evals", 0.5, _ts(1)),
            (12, 2, "old", "topic", 0.1, _ts(1)),
        ],
    )

    report = build_content_topic_assignment_gaps_report_from_db(conn, window_days=30, confidence_threshold=0.6, now=NOW)
    assert report["summary"]["by_issue_type"] == {
        "duplicate_topic_assignment": 1,
        "low_confidence_topic_assignment": 2,
        "missing_topic_assignment": 1,
    }
    assert {item["content_id"] for item in report["gap_items"]} == {1, 3}


def test_missing_content_topics_table_is_reported():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE generated_content (id INTEGER PRIMARY KEY, created_at TEXT)")
    report = build_content_topic_assignment_gaps_report_from_db(conn, now=NOW)
    assert report["missing_tables"] == ["content_topics"]
    assert report["gap_items"] == []


def test_cli_json_text_and_validation(tmp_path, capsys):
    conn = _conn()
    conn.execute("INSERT INTO generated_content VALUES (1, 'x_post', 1, NULL, NULL, 0.1, ?)", (_ts(1),))
    conn.commit()
    db_path = tmp_path / "topics.sqlite"
    with sqlite3.connect(db_path) as target:
        conn.backup(target)

    assert script.main(["--db", str(db_path), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["gap_count"] == 1
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Content Topic Assignment Gaps" in capsys.readouterr().out
    assert script.main(["--db", str(db_path), "--window-days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
    assert script.main(["--db", str(db_path), "--confidence-threshold", "-0.1"]) == 2
    assert "value must be non-negative" in capsys.readouterr().err


def test_formatters_and_invalid_thresholds():
    report = build_content_topic_assignment_gaps_report([], [], now=NOW)
    assert json.loads(format_content_topic_assignment_gaps_json(report))["artifact_type"] == "content_topic_assignment_gaps"
    assert "No content topic assignment gaps found" in format_content_topic_assignment_gaps_text(report)
    with pytest.raises(ValueError, match="window_days must be positive"):
        build_content_topic_assignment_gaps_report([], [], window_days=0)
    with pytest.raises(ValueError, match="confidence_threshold must be non-negative"):
        build_content_topic_assignment_gaps_report([], [], confidence_threshold=-0.1)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_content_topic_assignment_gaps_report([], [], limit=0)
