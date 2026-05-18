"""Tests for blog post source gap reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.blog_post_source_gap import (
    build_blog_post_source_gap_report,
    build_blog_post_source_gap_report_from_db,
    format_blog_post_source_gap_json,
    format_blog_post_source_gap_text,
)


NOW = datetime(2026, 5, 18, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "blog_post_source_gap.py"
spec = importlib.util.spec_from_file_location("blog_post_source_gap_script", SCRIPT_PATH)
blog_post_source_gap_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(blog_post_source_gap_script)


@contextmanager
def _script_context(conn):
    yield SimpleNamespace(), conn


def test_flags_zero_sources_and_unsupported_claim_sections():
    report = build_blog_post_source_gap_report(
        [
            {
                "id": "draft-1",
                "title": "Metric post",
                "source_count": 0,
                "sections": [
                    {"heading": "Proof", "claim_heavy": True, "text": "Data shows 40% faster delivery."},
                    {"heading": "Intro", "text": "Opening note."},
                ],
            }
        ],
        min_sources=2,
        now=NOW,
    )

    finding = report["findings"][0]

    assert finding["severity"] == "critical"
    assert finding["gap_types"] == ["zero_sources", "unsupported_claim_sections"]
    assert finding["unsupported_sections"][0]["section_id"] == "Proof"
    assert report["totals"]["gap_counts"]["zero_sources"] == 1


def test_flags_below_threshold_source_counts():
    report = build_blog_post_source_gap_report(
        [{"id": "post-1", "sources": ["a"], "sections": [{"claim_heavy": True, "evidence": ["a"]}]}],
        min_sources=3,
        now=NOW,
    )

    assert report["findings"][0]["severity"] == "medium"
    assert report["findings"][0]["gap_types"] == ["below_threshold_sources"]


def test_supported_blog_item_is_not_reported():
    report = build_blog_post_source_gap_report(
        [{"id": "post-1", "sources": ["a", "b"], "sections": [{"claim_heavy": True, "evidence": ["a"]}]}],
        min_sources=2,
        now=NOW,
    )

    assert report["findings"] == []
    assert report["empty_state"]["is_empty"] is True


def test_db_loader_json_text_and_cli(monkeypatch, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE blog_posts (
            id TEXT,
            title TEXT,
            status TEXT,
            source_count INTEGER,
            sections TEXT
        )"""
    )
    conn.execute(
        "INSERT INTO blog_posts VALUES (?, ?, ?, ?, ?)",
        ("blog-1", "Unsupported", "draft", 0, json.dumps([{"id": "claims", "claim_count": 2}])),
    )
    conn.commit()

    report = build_blog_post_source_gap_report_from_db(conn, now=NOW)

    assert json.loads(format_blog_post_source_gap_json(report))["artifact_type"] == "blog_post_source_gap"
    assert "blog-1 severity=critical" in format_blog_post_source_gap_text(report)
    monkeypatch.setattr(blog_post_source_gap_script, "script_context", lambda: _script_context(conn))
    monkeypatch.setattr(
        blog_post_source_gap_script,
        "build_blog_post_source_gap_report_from_db",
        lambda db, **kwargs: build_blog_post_source_gap_report_from_db(db, now=NOW, **kwargs),
    )
    assert blog_post_source_gap_script.main(["--table", "--min-sources", "2"]) == 0
    assert "Blog Post Source Gap" in capsys.readouterr().out
