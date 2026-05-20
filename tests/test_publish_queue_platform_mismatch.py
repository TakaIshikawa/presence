"""Tests for publish queue platform mismatch reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
from pathlib import Path
import sqlite3

from evaluation.publish_queue_platform_mismatch import (
    build_publish_queue_platform_mismatch_report,
    build_publish_queue_platform_mismatch_report_from_db,
    format_publish_queue_platform_mismatch_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publish_queue_platform_mismatch.py"
spec = importlib.util.spec_from_file_location("publish_queue_platform_mismatch_script", SCRIPT_PATH)
publish_queue_platform_mismatch_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(publish_queue_platform_mismatch_script)


def test_builder_expands_all_and_sorts_highest_risk():
    rows = [
        {"queue_id": 1, "content_id": 10, "queue_platform": "all", "queue_status": "queued"},
        {
            "queue_id": 1,
            "content_id": 10,
            "queue_platform": "all",
            "queue_status": "queued",
            "publication_id": 101,
            "publication_platform": "x",
            "publication_status": "published",
        },
        {
            "queue_id": 2,
            "content_id": 20,
            "queue_platform": "x",
            "queue_status": "completed",
            "publication_id": 201,
            "publication_platform": "x",
            "publication_status": "failed",
        },
        {
            "queue_id": 3,
            "content_id": 30,
            "queue_platform": "x",
            "queue_status": "queued",
            "publication_id": 301,
            "publication_platform": "bluesky",
            "publication_status": "published",
        },
    ]

    report = build_publish_queue_platform_mismatch_report(rows, now=NOW, all_platforms=("x", "bluesky"), limit=10)

    assert report["artifact_type"] == "publish_queue_platform_mismatch"
    assert report["summary"]["issue_group_count"] == 3
    assert report["issue_groups"][0]["queue_id"] == 2
    assert report["issue_groups"][0]["issues"][0]["issue_type"] == "failed_required_platform"
    assert report["issue_groups"][1]["queue_id"] == 1
    assert {issue["issue_type"] for issue in report["issue_groups"][1]["issues"]} == {
        "missing_publication_record",
        "published_without_queue_completion",
    }


def test_db_loader_reads_tables_and_generated_content_when_available():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE publish_queue (id INTEGER PRIMARY KEY, content_id INTEGER, platform TEXT, status TEXT);
        CREATE TABLE content_publications (id INTEGER PRIMARY KEY, content_id INTEGER, platform TEXT, status TEXT);
        CREATE TABLE generated_content (id INTEGER PRIMARY KEY, status TEXT);
        INSERT INTO publish_queue VALUES (1, 10, 'all', 'queued');
        INSERT INTO content_publications VALUES (100, 10, 'x', 'published');
        INSERT INTO generated_content VALUES (10, 'ready');
        """
    )

    report = build_publish_queue_platform_mismatch_report_from_db(conn, now=NOW)

    assert report["missing_tables"] == []
    assert report["issue_groups"][0]["queue_id"] == 1
    assert "Publish Queue Platform Mismatch" in format_publish_queue_platform_mismatch_text(report)


def test_missing_tables_are_reported_gracefully():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_publish_queue_platform_mismatch_report_from_db(conn, now=NOW)

    assert report["missing_tables"] == ["content_publications", "generated_content", "publish_queue"]
    assert report["summary"]["queue_rows_scanned"] == 0


def test_cli_outputs_json_and_text(tmp_path, capsys):
    db_path = tmp_path / "db.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE publish_queue (id INTEGER PRIMARY KEY, content_id INTEGER, platform TEXT, status TEXT);
        CREATE TABLE content_publications (id INTEGER PRIMARY KEY, content_id INTEGER, platform TEXT, status TEXT);
        INSERT INTO publish_queue VALUES (1, 10, 'all', 'queued');
        INSERT INTO content_publications VALUES (100, 10, 'x', 'published');
        """
    )
    conn.close()

    assert publish_queue_platform_mismatch_script.main(["--db", str(db_path), "--format", "json"]) == 0
    assert '"artifact_type": "publish_queue_platform_mismatch"' in capsys.readouterr().out
    assert publish_queue_platform_mismatch_script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Publish Queue Platform Mismatch" in capsys.readouterr().out
