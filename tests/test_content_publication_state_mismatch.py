"""Tests for content publication state mismatch reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.content_publication_state_mismatch import (
    build_content_publication_state_mismatch_report,
    build_content_publication_state_mismatch_report_from_db,
    format_content_publication_state_mismatch_json,
    format_content_publication_state_mismatch_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_publication_state_mismatch.py"
spec = importlib.util.spec_from_file_location("content_publication_state_mismatch_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn(path: Path | str = ":memory:") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            content_type TEXT,
            published INTEGER,
            published_url TEXT,
            tweet_id TEXT,
            published_at TEXT
        );
        CREATE TABLE content_publications (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            platform TEXT,
            status TEXT,
            platform_post_id TEXT,
            platform_url TEXT,
            published_at TEXT
        );
        CREATE TABLE publish_queue (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            platform TEXT,
            status TEXT,
            published_at TEXT
        );
        """
    )
    return conn


def test_builder_flags_each_mismatch_type():
    report = build_content_publication_state_mismatch_report(
        [
            {"id": 1, "content_type": "x_post", "published": 1, "tweet_id": "tw-1"},
            {"id": 2, "content_type": "x_post", "published": 0},
            {"id": 3, "content_type": "x_post", "published": 1},
            {"id": 4, "content_type": "x_post", "published": 1},
        ],
        [
            {"id": 20, "content_id": 2, "platform": "x", "status": "published", "platform_post_id": "p2", "platform_url": "https://x/2", "published_at": "2026-05-20T10:00:00+00:00"},
            {"id": 30, "content_id": 3, "platform": "x", "status": "published"},
            {"id": 40, "content_id": 4, "platform": "x", "status": "queued"},
        ],
        [
            {"id": 400, "content_id": 4, "platform": "x", "status": "published"},
        ],
        now=NOW,
    )

    payload = json.loads(format_content_publication_state_mismatch_json(report))
    assert payload["artifact_type"] == "content_publication_state_mismatch"
    assert payload["summary"]["by_issue_type"] == {
        "generated_published_flag_disagreement": 2,
        "missing_publication_record": 1,
        "published_missing_platform_metadata": 1,
        "queue_publication_status_disagreement": 1,
    }
    assert payload["findings"]["missing_publication_record"][0]["content_id"] == 1
    assert payload["findings"]["published_missing_platform_metadata"][0]["missing_fields"] == [
        "platform_post_id",
        "platform_url",
        "published_at",
    ]
    assert payload["findings"]["queue_publication_status_disagreement"][0]["queue_id"] == 400


def test_db_loader_schema_gaps_and_empty_state():
    missing = build_content_publication_state_mismatch_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["content_publications", "generated_content", "publish_queue"]
    assert missing["summary"]["finding_count"] == 0

    bad = sqlite3.connect(":memory:")
    bad.executescript(
        """
        CREATE TABLE generated_content (id INTEGER PRIMARY KEY);
        CREATE TABLE content_publications (id INTEGER PRIMARY KEY, content_id INTEGER);
        CREATE TABLE publish_queue (id INTEGER PRIMARY KEY, content_id INTEGER);
        """
    )
    gaps = build_content_publication_state_mismatch_report_from_db(bad, now=NOW)
    assert gaps["missing_columns"] == {
        "content_publications": ["platform", "status"],
        "publish_queue": ["status"],
    }

    conn = _conn()
    conn.execute("INSERT INTO generated_content VALUES (1, 'x_post', 0, NULL, NULL, NULL)")
    clean = build_content_publication_state_mismatch_report_from_db(conn, now=NOW)
    assert clean["empty_state"]["is_empty"] is True


def test_cli_json_text_and_limit_validation(tmp_path, capsys):
    db_path = tmp_path / "publication.sqlite"
    conn = _conn(db_path)
    conn.execute("INSERT INTO generated_content VALUES (1, 'x_post', 1, NULL, 'tw-1', NULL)")
    conn.commit()
    conn.close()

    assert script.main(["--db", str(db_path), "--format", "json", "--limit", "5"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"] == {"limit": 5}
    assert payload["findings"]["missing_publication_record"][0]["content_id"] == 1

    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    text = capsys.readouterr().out
    assert "Content Publication State Mismatch" in text
    assert "content_id | platform | issue_type" in text
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])


def test_text_formatter_and_invalid_limit():
    report = build_content_publication_state_mismatch_report([], [], [], now=NOW)
    assert "No content publication state mismatches found" in format_content_publication_state_mismatch_text(report)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_content_publication_state_mismatch_report([], [], [], limit=0)
