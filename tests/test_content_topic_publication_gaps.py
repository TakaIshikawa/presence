"""Tests for content topic publication gaps."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
from pathlib import Path
import sqlite3

from evaluation.content_topic_publication_gaps import (
    build_content_topic_publication_gaps_report_from_db,
    format_content_topic_publication_gaps_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_topic_publication_gaps.py"
spec = importlib.util.spec_from_file_location("content_topic_publication_gaps_script", SCRIPT_PATH)
content_topic_publication_gaps_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(content_topic_publication_gaps_script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            published INTEGER,
            created_at TEXT,
            published_at TEXT
        );
        CREATE TABLE content_topics (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            topic TEXT,
            subtopic TEXT,
            confidence REAL,
            created_at TEXT
        );
        """
    )
    return conn


def test_report_separates_orphan_topics_from_unpublished_content():
    conn = _conn()
    conn.executemany(
        "INSERT INTO generated_content VALUES (?, ?, ?, ?)",
        [
            (10, 1, "2026-05-19T00:00:00+00:00", "2026-05-20T00:00:00+00:00"),
            (11, 0, "2026-05-19T00:00:00+00:00", None),
        ],
    )
    conn.executemany(
        "INSERT INTO content_topics VALUES (?, ?, ?, ?, ?, ?)",
        [
            (1, 10, "AI", "Agents", 0.9, "2026-05-19T01:00:00+00:00"),
            (2, 11, "AI", "Agents", 0.8, "2026-05-19T02:00:00+00:00"),
            (3, 99, "Infra", "SQLite", 0.7, "2026-05-19T03:00:00+00:00"),
            (4, 11, "Low", "Skip", 0.1, "2026-05-19T04:00:00+00:00"),
        ],
    )

    report = build_content_topic_publication_gaps_report_from_db(
        conn, now=NOW, lookback_days=7, min_confidence=0.5, limit=10
    )

    assert report["artifact_type"] == "content_topic_publication_gaps"
    assert [item["topic_id"] for item in report["orphan_topic_rows"]] == [3]
    assert [item["topic_id"] for item in report["unpublished_topic_rows"]] == [2]
    assert report["grouped_topic_summaries"][0]["topic"] == "ai"
    assert "Unpublished topic rows" in format_content_topic_publication_gaps_text(report)


def test_missing_tables_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    report = build_content_topic_publication_gaps_report_from_db(conn, now=NOW)
    assert report["missing_tables"] == ["content_topics", "generated_content"]
    assert report["summary"]["topic_rows_scanned"] == 0


def test_cli_outputs_json_and_text(tmp_path, capsys):
    db_path = tmp_path / "db.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE generated_content (id INTEGER PRIMARY KEY, published INTEGER);
        CREATE TABLE content_topics (id INTEGER PRIMARY KEY, content_id INTEGER, topic TEXT, subtopic TEXT, confidence REAL);
        INSERT INTO content_topics VALUES (1, 99, 'AI', 'Agents', 1.0);
        """
    )
    conn.close()

    assert content_topic_publication_gaps_script.main(["--db", str(db_path), "--format", "json"]) == 0
    assert '"artifact_type": "content_topic_publication_gaps"' in capsys.readouterr().out
    assert content_topic_publication_gaps_script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Content Topic Publication Gaps" in capsys.readouterr().out
