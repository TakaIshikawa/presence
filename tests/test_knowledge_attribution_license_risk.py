"""Tests for knowledge attribution license risk reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.knowledge_attribution_license_risk import (
    build_knowledge_attribution_license_risk_report,
    build_knowledge_attribution_license_risk_report_from_db,
    format_knowledge_attribution_license_risk_json,
    format_knowledge_attribution_license_risk_text,
)


NOW = datetime(2026, 5, 20, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "knowledge_attribution_license_risk.py"
spec = importlib.util.spec_from_file_location("knowledge_attribution_license_risk_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn(path: Path | str = ":memory:") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE knowledge (
            id INTEGER PRIMARY KEY,
            source_type TEXT,
            source_id TEXT,
            author TEXT,
            source_url TEXT,
            license TEXT,
            attribution_required INTEGER
        );
        CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            content_type TEXT,
            content TEXT
        );
        CREATE TABLE content_knowledge_links (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            knowledge_id INTEGER,
            relevance_score REAL,
            created_at TEXT
        );
        CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            status TEXT
        );
        CREATE TABLE reply_knowledge_links (
            id INTEGER PRIMARY KEY,
            reply_queue_id INTEGER,
            knowledge_id INTEGER,
            relevance_score REAL,
            created_at TEXT
        );
        """
    )
    return conn


def test_builder_groups_risky_content_and_reply_usage_by_reason():
    report = build_knowledge_attribution_license_risk_report(
        [
            {
                "usage_type": "content",
                "content_id": 10,
                "knowledge_id": 1,
                "source_type": "curated_x",
                "source_id": "x1",
                "author": "Ada",
                "source_url": "https://x/1",
                "license": "restricted",
                "attribution_required": 1,
                "relevance_score": 0.9,
            },
            {
                "usage_type": "reply",
                "reply_queue_id": 20,
                "knowledge_id": 2,
                "source_type": "curated_article",
                "source_id": "a1",
                "author": "",
                "source_url": "",
                "license": "open",
                "attribution_required": 1,
                "relevance_score": 0.3,
            },
        ],
        min_relevance=0.5,
        now=NOW,
    )
    payload = json.loads(format_knowledge_attribution_license_risk_json(report))

    assert payload["artifact_type"] == "knowledge_attribution_license_risk"
    assert payload["totals"]["by_reason"] == {
        "low_relevance": 1,
        "missing_attribution": 1,
        "restricted_license": 1,
    }
    assert [group["reason"] for group in payload["findings"]] == [
        "restricted_license",
        "missing_attribution",
        "low_relevance",
    ]
    low_relevance = payload["findings"][2]["items"][0]
    assert low_relevance["reply_queue_id"] == 20
    assert low_relevance["source_type"] == "curated_article"
    assert "reason | usage_type | content_id | reply_queue_id" in format_knowledge_attribution_license_risk_text(report)


def test_from_db_loads_content_and_reply_risks_with_schema_gaps_and_limit():
    conn = _conn()
    conn.execute("INSERT INTO knowledge VALUES (1, 'curated_x', 'x1', 'Ada', 'https://x/1', 'restricted', 1)")
    conn.execute("INSERT INTO knowledge VALUES (2, 'curated_article', 'a1', NULL, NULL, 'open', 1)")
    conn.execute("INSERT INTO knowledge VALUES (3, 'own_post', 'p1', 'Taka', NULL, 'open', 0)")
    conn.execute("INSERT INTO generated_content VALUES (10, 'post', 'draft')")
    conn.execute("INSERT INTO reply_queue VALUES (20, 'drafted')")
    conn.execute("INSERT INTO content_knowledge_links VALUES (100, 10, 1, 0.9, '2026-05-20T01:00:00+00:00')")
    conn.execute("INSERT INTO reply_knowledge_links VALUES (200, 20, 2, 0.2, '2026-05-20T02:00:00+00:00')")
    conn.execute("INSERT INTO content_knowledge_links VALUES (101, 10, 3, 0.9, '2026-05-20T03:00:00+00:00')")

    report = build_knowledge_attribution_license_risk_report_from_db(conn, min_relevance=0.5, limit=2, now=NOW)
    assert report["totals"]["usage_count"] == 3
    assert report["totals"]["finding_count"] == 3
    assert report["totals"]["shown_count"] == 2
    assert report["findings"][0]["items"][0]["content_id"] == 10
    assert report["findings"][1]["items"][0]["reply_queue_id"] == 20

    missing = sqlite3.connect(":memory:")
    gaps = build_knowledge_attribution_license_risk_report_from_db(missing, now=NOW)
    assert gaps["missing_tables"] == ["content_knowledge_links", "knowledge", "reply_knowledge_links"]

    no_reply = sqlite3.connect(":memory:")
    no_reply.row_factory = sqlite3.Row
    no_reply.executescript(
        """
        CREATE TABLE knowledge (id INTEGER PRIMARY KEY, source_type TEXT);
        CREATE TABLE content_knowledge_links (content_id INTEGER, knowledge_id INTEGER);
        """
    )
    partial = build_knowledge_attribution_license_risk_report_from_db(no_reply, now=NOW)
    assert partial["missing_tables"] == ["reply_knowledge_links"]
    assert "license" in partial["missing_columns"]["knowledge"]
    assert "relevance_score" in partial["missing_columns"]["content_knowledge_links"]


def test_cli_db_json_text_and_validation(tmp_path, capsys):
    db_path = tmp_path / "knowledge.sqlite"
    conn = _conn(db_path)
    conn.execute("INSERT INTO knowledge VALUES (1, 'curated_x', 'x1', 'Ada', 'https://x/1', 'restricted', 1)")
    conn.execute("INSERT INTO generated_content VALUES (10, 'post', 'draft')")
    conn.execute("INSERT INTO content_knowledge_links VALUES (100, 10, 1, 0.8, '2026-05-20T01:00:00+00:00')")
    conn.commit()
    conn.close()

    assert script.main(["--db", str(db_path), "--format", "json", "--limit", "5", "--min-relevance", "0.4"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"]["min_relevance"] == 0.4
    assert payload["totals"]["by_reason"]["restricted_license"] == 1
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Knowledge Attribution License Risk" in capsys.readouterr().out
    assert script.main(["--limit", "0"]) == 2
    assert script.main(["--min-relevance", "-0.1"]) == 2
    with pytest.raises(ValueError, match="min_relevance must be non-negative"):
        build_knowledge_attribution_license_risk_report([], min_relevance=-0.1)
