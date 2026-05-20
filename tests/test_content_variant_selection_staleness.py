"""Tests for content variant selection staleness reporting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.content_variant_selection_staleness import (
    build_content_variant_selection_staleness_report,
    build_content_variant_selection_staleness_report_from_db,
    format_content_variant_selection_staleness_json,
    format_content_variant_selection_staleness_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_variant_selection_staleness.py"
spec = importlib.util.spec_from_file_location("content_variant_selection_staleness_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE content_variants (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            platform TEXT,
            content_type TEXT,
            selection_status TEXT,
            publish_status TEXT,
            selected_at TEXT,
            queue_id INTEGER
        )"""
    )
    return conn


def test_builder_flags_old_selected_unpublished_variants():
    rows = [
        {"variant_id": 1, "content_id": 10, "platform": "x", "content_type": "thread", "selection_status": "selected", "publish_status": "queued", "selected_at": (NOW - timedelta(days=10)).isoformat(), "queue_id": 3},
        {"variant_id": 2, "platform": "x", "selection_status": "selected", "publish_status": "published", "selected_at": (NOW - timedelta(days=10)).isoformat()},
        {"variant_id": 3, "platform": "bluesky", "selection_status": "selected", "selected_at": (NOW - timedelta(days=2)).isoformat()},
    ]
    report = build_content_variant_selection_staleness_report(rows, days=7, now=NOW)
    assert report["artifact_type"] == "content_variant_selection_staleness"
    assert report["summary"]["finding_count"] == 1
    assert report["findings"][0]["variant_id"] == 1
    assert report["findings"][0]["queue_id"] == 3


def test_db_adapter_cli_and_missing_schema(tmp_path, capsys):
    conn = _conn()
    conn.execute("INSERT INTO content_variants VALUES (1, 10, 'x', 'post', 'selected', 'queued', ?, 99)", ((NOW - timedelta(days=20)).isoformat(),))
    conn.execute("INSERT INTO content_variants VALUES (2, 11, 'x', 'post', 'selected', 'published', ?, NULL)", ((NOW - timedelta(days=20)).isoformat(),))
    conn.commit()
    report = build_content_variant_selection_staleness_report_from_db(conn, days=7, platform="x", now=NOW)
    assert report["summary"]["finding_count"] == 1
    assert report["findings"][0]["content_id"] == 10
    assert json.loads(format_content_variant_selection_staleness_json(report))["artifact_type"] == "content_variant_selection_staleness"
    assert "Content Variant Selection Staleness" in format_content_variant_selection_staleness_text(report)

    db_path = tmp_path / "variants.sqlite"
    conn.backup(sqlite3.connect(db_path))
    assert script.main(["--db", str(db_path), "--days", "30", "--platform", "x", "--limit", "5"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["finding_count"] == 1
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Content Variant Selection Staleness" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])

    missing = build_content_variant_selection_staleness_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["content_variants|generated_content_variants|generated_content"]
