"""Tests for content knowledge link orphan reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.content_knowledge_link_orphans import (
    build_content_knowledge_link_orphans_report,
    build_content_knowledge_link_orphans_report_from_db,
    format_content_knowledge_link_orphans_json,
    format_content_knowledge_link_orphans_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_knowledge_link_orphans.py"
spec = importlib.util.spec_from_file_location("content_knowledge_link_orphans_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE generated_content (id INTEGER PRIMARY KEY, content_type TEXT);
        CREATE TABLE knowledge (id INTEGER PRIMARY KEY, license TEXT, approved INTEGER);
        CREATE TABLE content_knowledge_links (content_id INTEGER, knowledge_id INTEGER, relevance_score REAL);
        """
    )
    return conn


def test_builder_flags_missing_content_missing_knowledge_and_invalid_scores():
    report = build_content_knowledge_link_orphans_report(
        [
            {
                "link_id": 1,
                "content_id": 100,
                "resolved_content_id": None,
                "knowledge_id": 200,
                "resolved_knowledge_id": 200,
                "relevance_score": None,
            },
            {
                "link_id": 2,
                "content_id": 101,
                "resolved_content_id": 101,
                "knowledge_id": 201,
                "resolved_knowledge_id": None,
                "relevance_score": 1.4,
            },
        ],
        now=NOW,
    )

    assert report["artifact_type"] == "content_knowledge_link_orphans"
    assert report["summary"]["by_issue_type"]["missing_content"] == 1
    assert report["summary"]["by_issue_type"]["missing_knowledge"] == 1
    assert report["summary"]["by_issue_type"]["invalid_relevance_score"] == 2


def test_builder_flags_restricted_and_unapproved_linked_knowledge():
    report = build_content_knowledge_link_orphans_report(
        [
            {
                "link_id": 1,
                "content_id": 1,
                "resolved_content_id": 1,
                "knowledge_id": 2,
                "resolved_knowledge_id": 2,
                "relevance_score": 0.9,
                "knowledge_license": "restricted",
                "knowledge_approved": 0,
            }
        ],
        now=NOW,
    )

    assert report["findings"][0]["issue_types"] == ["restricted_knowledge", "unapproved_knowledge"]
    assert report["summary"]["by_issue_type"]["restricted_knowledge"] == 1
    assert report["summary"]["by_issue_type"]["unapproved_knowledge"] == 1


def test_db_adapter_reads_joined_links_and_handles_missing_required_tables():
    conn = _conn()
    conn.execute("INSERT INTO generated_content VALUES (1, 'x_post')")
    conn.execute("INSERT INTO knowledge VALUES (2, 'restricted', 0)")
    conn.execute("INSERT INTO content_knowledge_links VALUES (1, 2, 0.9)")
    conn.execute("INSERT INTO content_knowledge_links VALUES (999, 888, -0.1)")

    report = build_content_knowledge_link_orphans_report_from_db(conn, now=NOW)

    assert report["summary"]["link_count"] == 2
    assert report["summary"]["finding_count"] == 2
    assert report["summary"]["by_issue_type"]["missing_content"] == 1
    assert report["summary"]["by_issue_type"]["missing_knowledge"] == 1
    assert report["summary"]["by_issue_type"]["restricted_knowledge"] == 1

    empty = build_content_knowledge_link_orphans_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert empty["summary"]["link_count"] == 0
    assert empty["findings"] == []


def test_json_and_text_formatters_are_stable():
    report = build_content_knowledge_link_orphans_report(
        [
            {
                "link_id": 1,
                "content_id": 1,
                "resolved_content_id": None,
                "knowledge_id": 2,
                "resolved_knowledge_id": None,
                "relevance_score": "bad",
            }
        ],
        now=NOW,
    )

    payload = json.loads(format_content_knowledge_link_orphans_json(report))
    assert payload["artifact_type"] == "content_knowledge_link_orphans"
    assert list(payload) == sorted(payload)
    text = format_content_knowledge_link_orphans_text(report)
    assert "Content Knowledge Link Orphans" in text
    assert "link_id | content_id | knowledge_id" in text


def test_cli_supports_db_json_text_and_invalid_limit(tmp_path, monkeypatch, capsys):
    db_path = tmp_path / "links.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE generated_content (id INTEGER PRIMARY KEY);
        CREATE TABLE knowledge (id INTEGER PRIMARY KEY, license TEXT, approved INTEGER);
        CREATE TABLE content_knowledge_links (content_id INTEGER, knowledge_id INTEGER, relevance_score REAL);
        INSERT INTO content_knowledge_links VALUES (1, 2, NULL);
        """
    )
    conn.close()

    assert script.main(["--db", str(db_path), "--limit", "5", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "content_knowledge_link_orphans"
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "invalid_relevance_score" in capsys.readouterr().out

    monkeypatch.setattr(script, "script_context", lambda: _script_context(sqlite3.connect(":memory:")))
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["findings"] == []
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])
