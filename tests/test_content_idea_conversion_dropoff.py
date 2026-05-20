"""Tests for content idea conversion dropoff reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.content_idea_conversion_dropoff import (
    build_content_idea_conversion_dropoff_report_from_db,
    format_content_idea_conversion_dropoff_json,
    format_content_idea_conversion_dropoff_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_idea_conversion_dropoff.py"
spec = importlib.util.spec_from_file_location("content_idea_conversion_dropoff_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE content_ideas (
             id INTEGER PRIMARY KEY,
             topic TEXT,
             source TEXT,
             priority TEXT,
             status TEXT,
             created_at TEXT,
             promoted_at TEXT,
             dismissed_at TEXT
           );"""
    )
    return conn


def test_report_groups_dropoff_and_aging():
    conn = _conn()
    conn.executemany(
        """INSERT INTO content_ideas
           (id, topic, source, priority, status, created_at, promoted_at, dismissed_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            (1, "AI", "backlog", "high", "open", "2026-05-01T00:00:00+00:00", None, None),
            (2, "ai", "backlog", "high", "promoted", "2026-05-05T00:00:00+00:00", "2026-05-06T00:00:00+00:00", None),
            (3, "ai", "backlog", "high", "dismissed", "2026-05-07T00:00:00+00:00", None, "2026-05-08T00:00:00+00:00"),
            (4, "ops", "search", "low", "open", "2026-05-18T00:00:00+00:00", None, None),
        ],
    )

    report = build_content_idea_conversion_dropoff_report_from_db(conn, now=NOW, stale_days=14)
    ai = next(row for row in report["grouped_summaries"] if row["topic"] == "ai")

    assert report["artifact_type"] == "content_idea_conversion_dropoff"
    assert ai["open_count"] == 1
    assert ai["promoted_count"] == 1
    assert ai["dismissed_count"] == 1
    assert ai["stale_open_count"] == 1
    assert ai["promotion_rate"] == 0.3333
    assert ai["dismissal_rate"] == 0.3333
    assert ai["max_open_age_days"] == 19


def test_filters_formatters_cli_and_schema(tmp_path, capsys):
    conn = _conn()
    conn.executemany(
        "INSERT INTO content_ideas (id, topic, source, priority, status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        [
            (1, "ai", "backlog", "high", "open", "2026-05-01T00:00:00+00:00"),
            (2, "ops", "search", "low", "open", "2026-05-01T00:00:00+00:00"),
        ],
    )
    report = build_content_idea_conversion_dropoff_report_from_db(conn, now=NOW, priority="high", source="backlog")
    assert report["summary"]["eligible_rows"] == 1
    assert json.loads(format_content_idea_conversion_dropoff_json(report))["artifact_type"] == "content_idea_conversion_dropoff"
    assert "open=1" in format_content_idea_conversion_dropoff_text(report)

    db_path = tmp_path / "ideas.sqlite"
    conn.commit()
    dest = sqlite3.connect(db_path)
    conn.backup(dest)
    dest.close()
    assert script.main(["--db", str(db_path), "--format", "json", "--now", NOW.isoformat(), "--priority", "high"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["eligible_rows"] == 1
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Content Idea Conversion Dropoff" in capsys.readouterr().out

    missing = sqlite3.connect(":memory:")
    assert build_content_idea_conversion_dropoff_report_from_db(missing)["missing_tables"] == ["content_ideas"]
    with pytest.raises(ValueError, match="lookback_days must be positive"):
        build_content_idea_conversion_dropoff_report_from_db(conn, lookback_days=0)
