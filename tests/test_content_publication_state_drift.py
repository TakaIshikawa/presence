"""Tests for content publication state drift reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.content_publication_state_drift import (
    build_content_publication_state_drift_report,
    build_content_publication_state_drift_report_from_db,
    format_content_publication_state_drift_json,
    format_content_publication_state_drift_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_publication_state_drift.py"
spec = importlib.util.spec_from_file_location("content_publication_state_drift_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            content_type TEXT,
            published INTEGER,
            published_url TEXT,
            tweet_id TEXT,
            published_at TEXT
        )"""
    )
    conn.execute(
        """CREATE TABLE content_publications (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            platform TEXT,
            status TEXT,
            platform_post_id TEXT,
            platform_url TEXT,
            published_at TEXT
        )"""
    )
    return conn


def test_builder_detects_publication_state_drift():
    report = build_content_publication_state_drift_report(
        [
            {"id": 1, "content_type": "x_post", "published": 1, "tweet_id": "tw-1", "published_url": "https://x/1"},
            {
                "id": 2,
                "content_type": "x_post",
                "published": 1,
                "tweet_id": "legacy-2",
                "published_url": "https://x/legacy-2",
                "published_at": "2026-05-01T10:00:00+00:00",
            },
        ],
        [
            {
                "id": 20,
                "content_id": 2,
                "platform": "x",
                "status": "published",
                "platform_post_id": "platform-2",
                "platform_url": "https://x/platform-2",
                "published_at": "2026-05-01T11:00:00+00:00",
            },
            {"id": 21, "content_id": 3, "platform": "bluesky", "status": "published"},
        ],
        now=NOW,
    )

    assert report["artifact_type"] == "content_publication_state_drift"
    assert report["summary"]["by_issue_type"] == {
        "conflicting_published_at": 1,
        "legacy_published_missing_platform_row": 1,
        "legacy_published_url_mismatch": 1,
        "legacy_tweet_id_mismatch": 1,
        "published_missing_platform_post_id": 1,
        "published_missing_platform_url": 1,
    }
    assert [item["content_id"] for item in report["drift_items"][:4]] == [1, 2, 2, 2]


def test_db_loader_tolerates_missing_tables():
    conn = sqlite3.connect(":memory:")
    report = build_content_publication_state_drift_report_from_db(conn, now=NOW)
    assert report["drift_items"] == []
    assert report["missing_tables"] == ["content_publications", "generated_content"]
    assert report["summary"]["drift_count"] == 0


def test_db_loader_and_cli_json_text_validation(tmp_path, capsys):
    conn = _conn()
    conn.execute(
        "INSERT INTO generated_content VALUES (1, 'x_post', 1, 'https://x/legacy', 'tw-legacy', '2026-05-01T10:00:00+00:00')"
    )
    conn.execute(
        "INSERT INTO content_publications VALUES (10, 1, 'x', 'published', 'tw-platform', NULL, '2026-05-01T10:00:00+00:00')"
    )
    conn.commit()
    db_path = tmp_path / "publication.sqlite"
    with sqlite3.connect(db_path) as target:
        conn.backup(target)

    assert script.main(["--db", str(db_path), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["drift_count"] == 2
    assert script.main(["--db", str(db_path), "--format", "text", "--limit", "1"]) == 0
    text = capsys.readouterr().out
    assert "Content Publication State Drift" in text
    assert "content_id | platform | issue_type" in text
    assert script.main(["--db", str(db_path), "--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_formatters_and_invalid_limit():
    report = build_content_publication_state_drift_report([], [], now=NOW)
    assert json.loads(format_content_publication_state_drift_json(report))["artifact_type"] == "content_publication_state_drift"
    assert "No content publication state drift found" in format_content_publication_state_drift_text(report)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_content_publication_state_drift_report([], [], limit=0)
