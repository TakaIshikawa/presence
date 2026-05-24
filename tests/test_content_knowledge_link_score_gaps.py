"""Tests for content knowledge link score gap reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.content_knowledge_link_score_gaps import (
    build_content_knowledge_link_score_gaps_report,
    build_content_knowledge_link_score_gaps_report_from_db,
    format_content_knowledge_link_score_gaps_json,
    format_content_knowledge_link_score_gaps_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_knowledge_link_score_gaps.py"
spec = importlib.util.spec_from_file_location("content_knowledge_link_score_gaps_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_builder_normalizes_scores_duplicates_and_weak_published_grounding():
    report = build_content_knowledge_link_score_gaps_report(
        [
            {"link_id": 1, "content_id": 1, "knowledge_id": 10, "resolved_content_id": 1, "resolved_knowledge_id": 10, "relevance_score": 0.2, "content_published": 1},
            {"link_id": 2, "content_id": 1, "knowledge_id": 10, "resolved_content_id": 1, "resolved_knowledge_id": 10, "relevance_score": 1.2, "content_published": 1},
            {"link_id": 3, "content_id": 2, "knowledge_id": 20, "resolved_content_id": None, "resolved_knowledge_id": None, "relevance_score": None},
            {"link_id": 4, "content_id": 4, "knowledge_id": 40, "resolved_content_id": 4, "resolved_knowledge_id": 40, "relevance_score": 0.1, "content_status": "draft"},
        ],
        content_rows=[
            {"content_id": 3, "status": "published"},
            {"content_id": 4, "status": "draft"},
        ],
        weak_threshold=0.5,
        min_links=2,
        now=NOW,
    )

    assert report["artifact_type"] == "content_knowledge_link_score_gaps"
    reasons = {finding["reason"] for finding in report["findings"]}
    assert {"missing_content", "missing_knowledge", "invalid_relevance_score", "duplicate_link", "weak_published_grounding"} <= reasons
    weak_content_ids = {
        finding["content_id"]
        for finding in report["findings"]
        if finding["reason"] == "weak_published_grounding"
    }
    assert 3 in weak_content_ids
    assert 4 not in weak_content_ids


def test_db_loader_reads_joins_and_reports_missing_schema():
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE generated_content (id INTEGER PRIMARY KEY, published INTEGER);
        CREATE TABLE knowledge (id INTEGER PRIMARY KEY);
        CREATE TABLE content_knowledge_links (content_id INTEGER, knowledge_id INTEGER, relevance_score REAL);
        INSERT INTO generated_content VALUES (1, 1);
        INSERT INTO generated_content VALUES (2, 1);
        INSERT INTO knowledge VALUES (10);
        INSERT INTO content_knowledge_links VALUES (1, 10, 0.1);
        INSERT INTO content_knowledge_links VALUES (9, 99, -0.1);
        """
    )
    report = build_content_knowledge_link_score_gaps_report_from_db(conn, weak_threshold=0.5, now=NOW)
    assert report["summary"]["rows_scanned"] == 2
    assert report["summary"]["by_reason"]["weak_published_grounding"] == 2
    assert report["summary"]["by_reason"]["missing_content"] == 1
    assert any(
        finding["reason"] == "weak_published_grounding" and finding["content_id"] == 2
        for finding in report["findings"]
    )

    missing = build_content_knowledge_link_score_gaps_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["content_knowledge_links", "generated_content", "knowledge"]

    partial = sqlite3.connect(":memory:")
    partial.executescript("CREATE TABLE content_knowledge_links (content_id INTEGER); CREATE TABLE generated_content (id INTEGER); CREATE TABLE knowledge (id INTEGER);")
    gaps = build_content_knowledge_link_score_gaps_report_from_db(partial, now=NOW)
    assert gaps["missing_columns"] == {"content_knowledge_links": ["knowledge_id"]}


def test_formatters_and_cli_validation(tmp_path, monkeypatch, capsys):
    report = build_content_knowledge_link_score_gaps_report([], now=NOW)
    assert json.loads(format_content_knowledge_link_score_gaps_json(report))["artifact_type"] == "content_knowledge_link_score_gaps"
    assert "Content Knowledge Link Score Gaps" in format_content_knowledge_link_score_gaps_text(report)

    db_path = tmp_path / "links.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE generated_content (id INTEGER PRIMARY KEY, published INTEGER);
        CREATE TABLE knowledge (id INTEGER PRIMARY KEY);
        CREATE TABLE content_knowledge_links (content_id INTEGER, knowledge_id INTEGER, relevance_score REAL);
        INSERT INTO content_knowledge_links VALUES (1, 2, NULL);
        """
    )
    conn.close()

    assert script.main(["--db", str(db_path), "--weak-threshold", "0.4", "--min-links", "1", "--limit", "5"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "content_knowledge_link_score_gaps"
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "invalid_relevance_score" in capsys.readouterr().out
    monkeypatch.setattr(script, "script_context", lambda: _script_context(sqlite3.connect(":memory:")))
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["missing_tables"]
    with pytest.raises(SystemExit):
        script.parse_args(["--weak-threshold", "2"])
