"""Tests for generated-content knowledge excerpt reuse detection."""

from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from knowledge.excerpt_reuse import (
    build_knowledge_excerpt_reuse_report,
    format_knowledge_excerpt_reuse_json,
    format_knowledge_excerpt_reuse_text,
    longest_shared_token_span,
    normalize_text,
)


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "detect_knowledge_excerpt_reuse.py"
)
spec = importlib.util.spec_from_file_location("detect_knowledge_excerpt_reuse_script", SCRIPT_PATH)
detect_knowledge_excerpt_reuse_script = importlib.util.module_from_spec(spec)
sys.modules["detect_knowledge_excerpt_reuse_script"] = detect_knowledge_excerpt_reuse_script
assert spec and spec.loader
spec.loader.exec_module(detect_knowledge_excerpt_reuse_script)

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


def _content(db, text: str, *, content_type: str = "blog_post") -> int:
    return db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=8.0,
        eval_feedback="ok",
    )


def _knowledge(
    conn: sqlite3.Connection,
    text: str,
    *,
    insight: str | None = None,
    source_id: str = "item-1",
) -> int:
    cursor = conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, insight, approved)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            "curated_article",
            source_id,
            f"https://example.test/{source_id}",
            "Analyst",
            text,
            insight,
            1,
        ),
    )
    conn.commit()
    return int(cursor.lastrowid)


def test_normalization_and_longest_shared_span_are_token_based():
    assert normalize_text("Ship, SHIP! v2.0") == "ship ship v2 0"

    span = longest_shared_token_span(
        "Intro Alpha beta gamma delta. Outro",
        "Before alpha beta gamma delta after",
    )

    assert span.token_count == 4
    assert span.generated_start == 1
    assert span.knowledge_start == 1
    assert span.similarity == 0.667
    assert "alpha beta gamma delta" in span.generated_excerpt


def test_flags_long_copied_excerpt_and_reports_previews(db):
    copied = (
        "teams keep their release notes useful when they describe the user visible "
        "change first and leave implementation details for the changelog"
    )
    content_id = _content(db, f"My note: {copied}. This is why launch notes land.")
    knowledge_id = _knowledge(
        db.conn,
        f"Context before. {copied}. Context after.",
        source_id="release-notes",
    )

    report = build_knowledge_excerpt_reuse_report(
        db,
        min_tokens=12,
        similarity_threshold=0.3,
        now=NOW,
    )

    assert len(report.findings) == 1
    finding = report.findings[0]
    assert finding.content_id == content_id
    assert finding.knowledge_id == knowledge_id
    assert finding.knowledge_identifier == "curated_article:release-notes"
    assert finding.knowledge_field == "content"
    assert finding.overlap_token_count >= 18
    assert "release notes useful" in finding.generated_excerpt
    assert finding.generated_excerpt == finding.knowledge_excerpt


def test_ignores_short_common_phrases_below_threshold(db):
    _content(db, "This week we learned that reliable systems need clear ownership.")
    _knowledge(
        db.conn,
        "A short memo says reliable systems need clear ownership before scaling.",
    )

    report = build_knowledge_excerpt_reuse_report(
        db,
        min_tokens=8,
        similarity_threshold=0.1,
        now=NOW,
    )

    assert report.findings == ()


def test_uses_optional_title_summary_text_fields_when_present():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE generated_content (
               id INTEGER PRIMARY KEY,
               content_type TEXT,
               content TEXT
           )"""
    )
    conn.execute(
        """CREATE TABLE knowledge (
               id INTEGER PRIMARY KEY,
               source_id TEXT,
               title TEXT,
               summary TEXT
           )"""
    )
    copied = "clear incident reviews separate timeline facts from interpretation and next actions"
    conn.execute(
        "INSERT INTO generated_content (id, content_type, content) VALUES (?, ?, ?)",
        (1, "newsletter", copied),
    )
    conn.execute(
        "INSERT INTO knowledge (id, source_id, title, summary) VALUES (?, ?, ?, ?)",
        (2, "incident-review", "Incident reviews", f"Intro {copied} outro"),
    )

    report = build_knowledge_excerpt_reuse_report(
        conn,
        min_tokens=10,
        similarity_threshold=0.8,
        now=NOW,
    )

    assert [finding.knowledge_field for finding in report.findings] == ["summary"]
    assert report.missing_columns == {}


def test_handles_missing_optional_schema_without_crashing():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE generated_content (id INTEGER PRIMARY KEY, content TEXT)")
    conn.execute(
        "INSERT INTO generated_content (id, content) VALUES (?, ?)",
        (1, "Generated copy"),
    )

    report = build_knowledge_excerpt_reuse_report(conn, now=NOW)

    assert report.findings == ()
    assert report.missing_tables == ("knowledge",)
    assert report.generated_content_count == 1
    assert report.knowledge_item_count == 0


def test_json_and_text_output_are_stable(db):
    copied = "product teams should cite sources and synthesize claims instead of copying long passages"
    _content(db, copied)
    _knowledge(db.conn, copied, source_id="citation")

    report = build_knowledge_excerpt_reuse_report(
        db,
        min_tokens=10,
        similarity_threshold=0.8,
        now=NOW,
    )
    payload = json.loads(format_knowledge_excerpt_reuse_json(report))
    text = format_knowledge_excerpt_reuse_text(report)

    assert list(payload) == sorted(payload)
    assert payload["summary"]["total_findings"] == 1
    assert "KNOWLEDGE EXCERPT REUSE" in text
    assert "content_id=" in text
    assert "knowledge=curated_article:citation" in text
    assert "overlap_tokens=13" in text


def test_limit_and_validation(db):
    copied = "one two three four five six seven eight nine ten eleven twelve"
    _content(db, copied)
    _knowledge(db.conn, copied, source_id="a")
    _knowledge(db.conn, copied, source_id="b")

    report = build_knowledge_excerpt_reuse_report(
        db,
        min_tokens=12,
        similarity_threshold=1,
        limit=1,
        now=NOW,
    )

    assert len(report.findings) == 1
    try:
        build_knowledge_excerpt_reuse_report(db, min_tokens=0)
    except ValueError as exc:
        assert "min_tokens must be at least 1" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_cli_outputs_json_for_patched_script_context(db, capsys):
    copied = "alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu"
    _content(db, copied)
    _knowledge(db.conn, copied, source_id="cli")

    @contextmanager
    def fake_script_context():
        yield SimpleNamespace(), db

    with patch.object(detect_knowledge_excerpt_reuse_script, "script_context", fake_script_context):
        result = detect_knowledge_excerpt_reuse_script.main(
            ["--format", "json", "--min-tokens", "12", "--similarity-threshold", "1"]
        )

    payload = json.loads(capsys.readouterr().out)
    assert result == 1
    assert payload["summary"]["total_findings"] == 1

    result = detect_knowledge_excerpt_reuse_script.main(["--min-tokens", "0"])
    captured = capsys.readouterr()
    assert result == 1
    assert "min_tokens must be at least 1" in captured.err
