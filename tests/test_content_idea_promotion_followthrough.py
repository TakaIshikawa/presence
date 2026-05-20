"""Tests for content idea promotion followthrough reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
from pathlib import Path
import sqlite3

from evaluation.content_idea_promotion_followthrough import (
    build_content_idea_promotion_followthrough_report,
    build_content_idea_promotion_followthrough_report_from_db,
    format_content_idea_promotion_followthrough_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_idea_promotion_followthrough.py"
spec = importlib.util.spec_from_file_location("content_idea_promotion_followthrough_script", SCRIPT_PATH)
content_idea_promotion_followthrough_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(content_idea_promotion_followthrough_script)


def test_builder_matches_topic_and_metadata_and_limits_results():
    rows = [
        {"row_type": "idea", "content_idea_id": 1, "topic": "Launch plan", "status": "promoted", "promoted_at": "2026-05-01T00:00:00+00:00", "source_metadata": '{"planned_topic_id": 10}'},
        {"row_type": "idea", "content_idea_id": 2, "topic": "Missing", "status": "promoted", "promoted_at": "2026-05-01T00:00:00+00:00"},
        {"row_type": "idea", "content_idea_id": 3, "topic": "Bad json", "status": "promoted", "promoted_at": "2026-05-19T00:00:00+00:00", "source_metadata": "{"},
        {"row_type": "idea", "content_idea_id": 4, "topic": "Draft", "status": "draft", "promoted_at": None},
        {"row_type": "planned", "planned_topic_id": 10, "topic": "Other"},
        {"row_type": "generated", "content_id": 20, "topic": "launch plan"},
    ]

    report = build_content_idea_promotion_followthrough_report(rows, now=NOW, max_age_days=14, limit=4)

    assert report["artifact_type"] == "content_idea_promotion_followthrough"
    assert report["summary"]["promoted_ideas_scanned"] == 3
    issue_types = [item["issue_type"] for item in report["issue_items"]]
    assert issue_types == [
        "malformed_source_metadata",
        "stale_promoted_idea",
        "missing_planned_topic",
        "missing_planned_topic",
    ]
    assert report["summary"]["issue_count"] == 6
    assert report["issue_items"][1]["content_idea_id"] == 2


def test_db_loader_reads_available_tables_and_missing_metadata():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE content_ideas (
            id INTEGER PRIMARY KEY, topic TEXT, status TEXT, promoted_at TEXT, source_metadata TEXT
        );
        CREATE TABLE planned_topics (id INTEGER PRIMARY KEY, topic TEXT);
        CREATE TABLE generated_content (id INTEGER PRIMARY KEY, topic TEXT);
        INSERT INTO content_ideas VALUES (1, 'Topic A', 'promoted', '2026-05-01T00:00:00+00:00', NULL);
        INSERT INTO planned_topics VALUES (10, 'Topic A');
        """
    )

    report = build_content_idea_promotion_followthrough_report_from_db(conn, now=NOW, max_age_days=7)

    assert report["missing_tables"] == []
    assert report["issue_items"][0]["issue_type"] == "stale_promoted_idea"
    assert "Content Idea Promotion Followthrough" in format_content_idea_promotion_followthrough_text(report)


def test_missing_tables_are_reported_gracefully():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    report = build_content_idea_promotion_followthrough_report_from_db(conn, now=NOW)
    assert report["missing_tables"] == ["content_ideas", "generated_content", "planned_topics"]
    assert report["summary"]["ideas_scanned"] == 0


def test_cli_outputs_json_and_text(tmp_path, capsys):
    db_path = tmp_path / "db.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE content_ideas (id INTEGER PRIMARY KEY, topic TEXT, status TEXT, promoted_at TEXT);
        INSERT INTO content_ideas VALUES (1, 'Topic A', 'promoted', '2026-05-01T00:00:00+00:00');
        """
    )
    conn.close()

    assert content_idea_promotion_followthrough_script.main(["--db", str(db_path), "--format", "json"]) == 0
    assert '"artifact_type": "content_idea_promotion_followthrough"' in capsys.readouterr().out
    assert content_idea_promotion_followthrough_script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Content Idea Promotion Followthrough" in capsys.readouterr().out
