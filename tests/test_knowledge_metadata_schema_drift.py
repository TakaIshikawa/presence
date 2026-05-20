"""Tests for knowledge metadata schema drift reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.knowledge_metadata_schema_drift import (
    build_knowledge_metadata_schema_drift_report,
    build_knowledge_metadata_schema_drift_report_from_db,
    format_knowledge_metadata_schema_drift_json,
    format_knowledge_metadata_schema_drift_text,
)


NOW = datetime(2026, 5, 20, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "knowledge_metadata_schema_drift.py"
spec = importlib.util.spec_from_file_location("knowledge_metadata_schema_drift_script", SCRIPT_PATH)
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
            source_url TEXT,
            license TEXT,
            metadata TEXT
        );
        """
    )
    return conn


def test_builder_flags_metadata_drift_reasons_and_totals():
    report = build_knowledge_metadata_schema_drift_report(
        [
            {"knowledge_id": 1, "source_type": "curated_x", "source_id": "x1", "source_url": "https://x/1", "license": "open", "metadata": "{bad"},
            {"knowledge_id": 2, "source_type": "curated_article", "source_id": "a1", "source_url": "https://a/1", "license": "open", "metadata": "[]"},
            {"knowledge_id": 3, "source_type": "curated_newsletter", "source_id": "n1", "source_url": "https://n/1", "license": "restricted", "metadata": {"title": "Issue"}},
            {"knowledge_id": 4, "source_type": "own_conversation", "source_id": "c1", "source_url": "", "license": "attribution_required", "metadata": {"conversation_id": "c1", "participant": "Ada"}},
            {"knowledge_id": 5, "source_type": "curated_article", "source_id": "a2", "source_url": "https://a/2", "license": "open", "metadata": {"title": "T", "author": "Ada", "published_at": "2026-05-01"}},
        ],
        now=NOW,
    )
    payload = json.loads(format_knowledge_metadata_schema_drift_json(report))

    assert payload["artifact_type"] == "knowledge_metadata_schema_drift"
    assert payload["totals"]["by_source_type"] == {
        "curated_article": 2,
        "curated_newsletter": 1,
        "curated_x": 1,
        "own_conversation": 1,
    }
    assert payload["totals"]["by_drift_reason"] == {
        "malformed_metadata": 1,
        "missing_expected_source_fields": 1,
        "non_object_metadata": 1,
        "restricted_license_missing_provenance": 2,
    }
    assert [item["knowledge_id"] for item in payload["findings"]] == [1, 2, 3, 4]
    assert "knowledge_id | source_type | source_id" in format_knowledge_metadata_schema_drift_text(report)


def test_from_db_schema_gaps_source_filter_empty_state_and_limit_ordering():
    missing = build_knowledge_metadata_schema_drift_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["knowledge"]

    bad = sqlite3.connect(":memory:")
    bad.execute("CREATE TABLE knowledge (id INTEGER PRIMARY KEY)")
    gaps = build_knowledge_metadata_schema_drift_report_from_db(bad, now=NOW)
    assert gaps["missing_columns"]["knowledge"] == ["license", "metadata", "source_id", "source_type", "source_url"]

    conn = _conn()
    conn.execute("INSERT INTO knowledge VALUES (1, 'curated_x', 'x1', 'https://x/1', 'open', '{bad')")
    conn.execute("INSERT INTO knowledge VALUES (2, 'curated_article', 'a1', 'https://a/1', 'open', '{}')")
    conn.execute("INSERT INTO knowledge VALUES (3, 'own_conversation', 'c1', '', 'open', '{}')")
    report = build_knowledge_metadata_schema_drift_report_from_db(conn, source_types=["curated_article", "own_conversation"], limit=1, now=NOW)
    assert report["totals"]["knowledge_count"] == 2
    assert report["totals"]["finding_count"] == 2
    assert [item["knowledge_id"] for item in report["findings"]] == [2]

    clean = _conn()
    clean.execute(
        "INSERT INTO knowledge VALUES (1, 'curated_x', 'x1', 'https://x/1', 'open', ?)",
        (json.dumps({"post_id": "p1", "author": "Ada"}),),
    )
    clean_report = build_knowledge_metadata_schema_drift_report_from_db(clean, now=NOW)
    assert clean_report["empty_state"]["is_empty"] is True
    assert "No knowledge metadata schema drift found" in format_knowledge_metadata_schema_drift_text(clean_report)


def test_cli_db_source_type_json_text_and_validation(tmp_path, capsys):
    db_path = tmp_path / "knowledge.sqlite"
    conn = _conn(db_path)
    conn.execute("INSERT INTO knowledge VALUES (1, 'curated_x', 'x1', 'https://x/1', 'open', '{}')")
    conn.commit()
    conn.close()

    assert script.main(["--db", str(db_path), "--source-type", "curated_x", "--format", "json", "--limit", "5"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "knowledge_metadata_schema_drift"
    assert payload["filters"]["source_types"] == ["curated_x"]
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Knowledge Metadata Schema Drift" in capsys.readouterr().out
    assert script.main(["--limit", "0"]) == 2
    with pytest.raises(ValueError, match="limit must be positive"):
        build_knowledge_metadata_schema_drift_report([], limit=0)
