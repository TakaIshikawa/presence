from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from knowledge.source_attribution_chain_breaks import build_source_attribution_chain_breaks_report


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "source_attribution_chain_breaks.py"
spec = importlib.util.spec_from_file_location("source_attribution_chain_breaks_script", SCRIPT)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE generated_content (id INTEGER PRIMARY KEY, content_type TEXT, created_at TEXT);
        CREATE TABLE content_knowledge_links (content_id INTEGER, knowledge_id INTEGER);
        CREATE TABLE knowledge (id INTEGER PRIMARY KEY, source_id INTEGER, source_url TEXT, metadata_checked_at TEXT);
        CREATE TABLE curated_sources (id INTEGER PRIMARY KEY, knowledge_id INTEGER, source_url TEXT, curated INTEGER);"""
    )
    return conn


def test_broken_foreign_key_like_links():
    conn = _conn()
    conn.execute("INSERT INTO generated_content VALUES (1, 'x_post', '2026-04-25T00:00:00+00:00')")
    conn.execute("INSERT INTO content_knowledge_links VALUES (1, 99)")

    report = build_source_attribution_chain_breaks_report(conn, now=NOW)

    assert report.issues[0].issue_type == "missing_knowledge_row"
    assert report.issues[0].knowledge_id == 99


def test_missing_urls_and_stale_metadata():
    conn = _conn()
    conn.execute("INSERT INTO generated_content VALUES (1, 'x_post', '2026-04-25T00:00:00+00:00')")
    conn.execute("INSERT INTO knowledge VALUES (2, 10, NULL, '2025-01-01T00:00:00+00:00')")
    conn.execute("INSERT INTO content_knowledge_links VALUES (1, 2)")

    report = build_source_attribution_chain_breaks_report(conn, now=NOW)
    types = [issue.issue_type for issue in report.issues]

    assert "missing_source_url" in types
    assert "stale_link_metadata" in types


def test_filters_and_uncited_curated_reference():
    conn = _conn()
    conn.execute("INSERT INTO generated_content VALUES (1, 'blog_post', '2026-04-25T00:00:00+00:00')")
    conn.execute("INSERT INTO knowledge VALUES (2, 10, 'https://example.com', '2026-04-01T00:00:00+00:00')")
    conn.execute("INSERT INTO curated_sources VALUES (10, 2, 'https://example.com', 1)")

    report = build_source_attribution_chain_breaks_report(conn, content_type="blog_post", issue_type="uncited_curated_reference", now=NOW)

    assert report.issues[0].issue_type == "uncited_curated_reference"
    assert report.issues[0].source_id == 10


def test_cli_json_output(capsys, tmp_path):
    db_path = tmp_path / "source.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """CREATE TABLE generated_content (id INTEGER PRIMARY KEY, content_type TEXT, created_at TEXT);
        CREATE TABLE content_knowledge_links (content_id INTEGER, knowledge_id INTEGER);
        CREATE TABLE knowledge (id INTEGER PRIMARY KEY, source_id INTEGER, source_url TEXT, metadata_checked_at TEXT);
        INSERT INTO generated_content VALUES (1, 'x_post', '2026-04-25T00:00:00+00:00');
        INSERT INTO content_knowledge_links VALUES (1, 99);"""
    )
    conn.close()

    assert script.main(["--db", str(db_path), "--days", "60", "--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["artifact_type"] == "source_attribution_chain_breaks"
    assert payload["issues"][0]["issue_type"] == "missing_knowledge_row"
