"""Tests for knowledge source citation decay reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from knowledge.source_citation_decay import (
    build_source_citation_decay_report_from_db,
    format_source_citation_decay_json,
    format_source_citation_decay_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "knowledge_source_citation_decay.py"
spec = importlib.util.spec_from_file_location("knowledge_source_citation_decay_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE knowledge (
             id TEXT PRIMARY KEY,
             source_type TEXT,
             author TEXT,
             title TEXT,
             url TEXT
           );
           CREATE TABLE content_knowledge_links (
             id INTEGER PRIMARY KEY,
             content_id INTEGER,
             knowledge_id TEXT,
             used_at TEXT
           );"""
    )
    return conn


def test_report_flags_decay_by_source_type_and_author():
    conn = _conn()
    conn.executemany("INSERT INTO knowledge (id, source_type, author, title, url) VALUES (?, ?, ?, ?, ?)", [
        ("k1", "blog", "Ada", "One", "https://one.example"),
        ("k2", "blog", "Ada", "Two", "https://two.example"),
        ("k3", "paper", "Bea", "Three", None),
    ])
    conn.executemany("INSERT INTO content_knowledge_links (content_id, knowledge_id, used_at) VALUES (?, ?, ?)", [
        (1, "k1", "2026-04-25T00:00:00+00:00"),
        (2, "k1", "2026-04-26T00:00:00+00:00"),
        (3, "k2", "2026-04-27T00:00:00+00:00"),
        (4, "k1", "2026-05-15T00:00:00+00:00"),
        (5, "k3", "2026-04-25T00:00:00+00:00"),
        (6, "k3", "2026-05-15T00:00:00+00:00"),
    ])

    report = build_source_citation_decay_report_from_db(conn, now=NOW, window_days=20, min_drop=2, min_drop_percent=0.5)

    assert report["artifact_type"] == "knowledge_source_citation_decay"
    assert len(report["decay_buckets"]) == 1
    bucket = report["decay_buckets"][0]
    assert bucket["source_type"] == "blog"
    assert bucket["author"] == "ada"
    assert bucket["baseline_count"] == 3
    assert bucket["recent_count"] == 1
    assert bucket["drop_count"] == 2
    assert bucket["source_examples"][0]["knowledge_id"] == "k1"
    assert bucket["source_examples"][0]["title"] == "One"


def test_source_type_filter_formatters_cli_and_missing_schema(tmp_path, capsys):
    conn = _conn()
    conn.execute("INSERT INTO knowledge (id, source_type, author, title) VALUES ('k1', 'blog', 'Ada', 'One')")
    conn.executemany("INSERT INTO content_knowledge_links (content_id, knowledge_id, used_at) VALUES (?, 'k1', ?)", [
        (1, "2026-04-25T00:00:00+00:00"),
        (2, "2026-04-26T00:00:00+00:00"),
    ])
    report = build_source_citation_decay_report_from_db(conn, now=NOW, window_days=10, source_type="paper")
    assert report["decay_buckets"] == []
    assert json.loads(format_source_citation_decay_json(report))["artifact_type"] == "knowledge_source_citation_decay"
    assert "No knowledge source citation decay" in format_source_citation_decay_text(report)

    db_path = tmp_path / "knowledge.sqlite"
    conn.commit()
    dest = sqlite3.connect(db_path)
    conn.backup(dest)
    dest.close()
    assert script.main(["--db", str(db_path), "--format", "json", "--now", NOW.isoformat(), "--window-days", "10"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["eligible_citation_count"] == 2
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Knowledge Source Citation Decay" in capsys.readouterr().out

    missing = sqlite3.connect(":memory:")
    assert build_source_citation_decay_report_from_db(missing)["missing_tables"] == ["knowledge", "content_knowledge_links"]
