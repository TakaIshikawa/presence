"""Tests for campaign topic source freshness reporting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.campaign_topic_source_freshness import (
    build_campaign_topic_source_freshness_report_from_db,
    format_campaign_topic_source_freshness_json,
    format_campaign_topic_source_freshness_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "campaign_topic_source_freshness.py"
spec = importlib.util.spec_from_file_location("campaign_topic_source_freshness_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _ts(days_ago: int) -> str:
    return (NOW - timedelta(days=days_ago)).isoformat()


def _conn(path: Path | str = ":memory:") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE planned_topics (
            id INTEGER PRIMARY KEY,
            campaign_id INTEGER,
            topic TEXT,
            target_date TEXT,
            status TEXT,
            source_material TEXT,
            content_id INTEGER
        );
        CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            planned_topic_id INTEGER,
            created_at TEXT
        );
        CREATE TABLE content_knowledge_links (
            content_id INTEGER,
            knowledge_id INTEGER,
            relevance_score REAL
        );
        CREATE TABLE knowledge (
            id INTEGER PRIMARY KEY,
            created_at TEXT,
            published_at TEXT
        );
        """
    )
    return conn


def test_report_distinguishes_source_freshness_buckets():
    conn = _conn()
    conn.executemany(
        "INSERT INTO planned_topics VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (1, 10, "Missing", "2026-05-21", "planned", None, None),
            (2, 10, "No content", "2026-05-22", "planned", "notes", None),
            (3, 10, "Stale", "2026-05-23", "generated", "notes", 30),
            (4, 10, "Fresh", "2026-05-24", "generated", "notes", 40),
            (5, 11, "Other", "2026-05-24", "generated", "notes", 50),
        ],
    )
    conn.executemany("INSERT INTO generated_content VALUES (?, ?, ?)", [(30, 3, _ts(2)), (40, 4, _ts(2)), (50, 5, _ts(2))])
    conn.executemany("INSERT INTO knowledge VALUES (?, ?, ?)", [(300, _ts(90), _ts(90)), (400, _ts(1), _ts(1)), (500, _ts(1), _ts(1))])
    conn.executemany("INSERT INTO content_knowledge_links VALUES (?, ?, ?)", [(30, 300, 0.9), (40, 400, 0.9), (50, 500, 0.9)])

    report = build_campaign_topic_source_freshness_report_from_db(conn, campaign_id=10, days_ahead=10, stale_days=30, now=NOW)
    payload = json.loads(format_campaign_topic_source_freshness_json(report))

    assert payload["artifact_type"] == "campaign_topic_source_freshness"
    assert payload["freshness_buckets"] == {
        "fresh_supported": 1,
        "missing_source_material": 1,
        "no_linked_generated_content": 1,
        "stale_knowledge_support": 1,
    }
    assert [item["freshness_bucket"] for item in payload["findings"]] == [
        "missing_source_material",
        "no_linked_generated_content",
        "stale_knowledge_support",
        "fresh_supported",
    ]
    assert payload["findings"][2]["knowledge_ids"] == [300]
    assert payload["campaign_summary"][0]["campaign_id"] == 10


def test_cli_text_limit_and_schema_gaps(tmp_path, capsys):
    db_path = tmp_path / "campaign.sqlite"
    conn = _conn(db_path)
    conn.execute("INSERT INTO planned_topics VALUES (1, 10, 'Fresh', '2026-05-21', 'generated', 'notes', 100)")
    conn.execute("INSERT INTO generated_content VALUES (100, 1, ?)", (_ts(1),))
    conn.execute("INSERT INTO knowledge VALUES (1, ?, ?)", (_ts(1), _ts(1)))
    conn.execute("INSERT INTO content_knowledge_links VALUES (100, 1, 0.9)")
    conn.commit()
    conn.close()

    original_builder = script.build_campaign_topic_source_freshness_report_from_db

    def fixed_now(conn, **kwargs):
        return original_builder(conn, now=NOW, **kwargs)

    import pytest

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(script, "build_campaign_topic_source_freshness_report_from_db", fixed_now)
        assert script.main(["--db", str(db_path), "--campaign-id", "10", "--days-ahead", "5", "--stale-days", "30", "--limit", "1", "--format", "json"]) == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["filters"]["limit"] == 1
        assert payload["findings"][0]["freshness_bucket"] == "fresh_supported"

        assert script.main(["--db", str(db_path), "--format", "text"]) == 0
        assert "Campaign Topic Source Freshness" in capsys.readouterr().out

    report = build_campaign_topic_source_freshness_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert report["missing_tables"] == ["planned_topics"]
    assert report["findings"] == []
    assert "Missing tables: planned_topics" in format_campaign_topic_source_freshness_text(report)

    partial = sqlite3.connect(":memory:")
    partial.execute("CREATE TABLE planned_topics (id INTEGER PRIMARY KEY, topic TEXT)")
    gaps = build_campaign_topic_source_freshness_report_from_db(partial, now=NOW)
    assert "source_material" in gaps["missing_columns"]["planned_topics"]
