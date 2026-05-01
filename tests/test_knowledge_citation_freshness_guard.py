"""Tests for knowledge citation freshness guard."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

from output.knowledge_citation_freshness_guard import (
    build_knowledge_citation_freshness_report,
    export_to_json,
    format_text_report,
)


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "check_citation_freshness.py"
)
spec = importlib.util.spec_from_file_location("check_citation_freshness_script", SCRIPT_PATH)
check_citation_freshness_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(check_citation_freshness_script)

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, text: str = "Generated post with knowledge") -> int:
    return db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=8.0,
        eval_feedback="ok",
    )


def _knowledge(
    db,
    source_id: str,
    *,
    source_type: str = "curated_article",
    source_url: str | None = "https://example.com/article",
    canonical_url: str | None = "https://example.com/article",
    approved: int = 1,
    published_days_ago: int = 10,
    author: str = "Example",
) -> int:
    metadata = (
        {"link_metadata": {"canonical_url": canonical_url}}
        if canonical_url is not None
        else {}
    )
    return db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, insight,
            license, attribution_required, approved, published_at, metadata)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            source_type,
            source_id,
            source_url,
            author,
            f"Content for {source_id}",
            f"Insight for {source_id}",
            "open",
            0,
            approved,
            (NOW - timedelta(days=published_days_ago)).isoformat(),
            json.dumps(metadata),
        ),
    ).lastrowid


def _codes(report) -> list[str]:
    return sorted(
        finding.code
        for item in report.items
        for finding in item.findings
    )


def test_fresh_citation_with_canonical_url_passes_read_only(db):
    content_id = _content(db)
    knowledge_id = _knowledge(db, "fresh")
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.9)])
    before = db.conn.total_changes

    report = build_knowledge_citation_freshness_report(
        db,
        content_id=content_id,
        days=90,
        require_canonical=True,
        now=NOW,
    )
    payload = json.loads(export_to_json(report))
    text = format_text_report(report)

    assert report.blocked_count == 0
    assert report.warning_count == 0
    assert report.passed_count == 1
    assert report.items[0].status == "passed"
    assert payload["artifact_type"] == "knowledge_citation_freshness"
    assert list(payload.keys()) == sorted(payload.keys())
    assert "Knowledge Citation Freshness" in text
    assert "knowledge #1" in text
    assert db.conn.total_changes == before


def test_flags_stale_retired_untraceable_and_unhealthy_sources(db):
    content_id = _content(db)
    stale_id = _knowledge(db, "stale", published_days_ago=120)
    retired_id = _knowledge(db, "retired", approved=0)
    missing_url_id = _knowledge(
        db,
        "missing-url",
        source_url=None,
        canonical_url=None,
    )
    unhealthy_id = _knowledge(
        db,
        "https://paused.example/post",
        source_url="https://paused.example/post",
        canonical_url="https://paused.example/post",
        author="paused.example",
    )
    db.conn.execute(
        """INSERT INTO curated_sources
           (source_type, identifier, name, active, status, last_fetch_status)
           VALUES (?, ?, ?, ?, ?, ?)""",
        ("blog", "paused.example", "Paused", 0, "paused", "failure"),
    )
    db.insert_content_knowledge_links(
        content_id,
        [(stale_id, 0.9), (retired_id, 0.8), (missing_url_id, 0.7), (unhealthy_id, 0.6)],
    )

    report = build_knowledge_citation_freshness_report(db, days=90, now=NOW)

    assert report.blocked_count == 4
    assert _codes(report) == [
        "missing_canonical_url",
        "retired_knowledge",
        "stale_knowledge",
        "unhealthy_source",
        "untraceable_knowledge",
    ]
    by_knowledge = {item.knowledge_id: item for item in report.items}
    assert by_knowledge[missing_url_id].reason_codes == [
        "missing_canonical_url",
        "untraceable_knowledge",
    ]


def test_canonical_requirement_is_configurable(db):
    content_id = _content(db)
    knowledge_id = _knowledge(
        db,
        "source-url-only",
        source_url="https://example.com/source-url-only",
        canonical_url=None,
    )
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.9)])

    strict_report = build_knowledge_citation_freshness_report(
        db,
        content_id=content_id,
        require_canonical=True,
        now=NOW,
    )
    relaxed_report = build_knowledge_citation_freshness_report(
        db,
        content_id=content_id,
        require_canonical=False,
        now=NOW,
    )

    assert strict_report.warning_count == 1
    assert _codes(strict_report) == ["missing_canonical_url"]
    assert relaxed_report.passed_count == 1
    assert relaxed_report.items[0].reason_codes == []


def test_tolerates_missing_optional_metadata_column():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            content_type TEXT,
            content TEXT,
            created_at TEXT
        );
        CREATE TABLE knowledge (
            id INTEGER PRIMARY KEY,
            source_type TEXT NOT NULL,
            source_id TEXT,
            source_url TEXT,
            author TEXT,
            content TEXT NOT NULL,
            approved INTEGER,
            published_at TEXT
        );
        CREATE TABLE content_knowledge_links (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            knowledge_id INTEGER,
            relevance_score REAL,
            created_at TEXT
        );
        """
    )
    conn.execute(
        "INSERT INTO generated_content (id, content_type, content, created_at) VALUES (1, 'x_post', 'text', ?)",
        (NOW.isoformat(),),
    )
    conn.execute(
        """INSERT INTO knowledge
           (id, source_type, source_id, source_url, author, content, approved, published_at)
           VALUES (1, 'curated_article', 'minimal', 'https://example.com/minimal',
                   'Example', 'knowledge', 1, ?)""",
        ((NOW - timedelta(days=1)).isoformat(),),
    )
    conn.execute(
        "INSERT INTO content_knowledge_links (content_id, knowledge_id, relevance_score) VALUES (1, 1, 0.9)"
    )

    report = build_knowledge_citation_freshness_report(
        conn,
        content_id=1,
        require_canonical=False,
        now=NOW,
    )

    assert report.passed_count == 1
    assert report.missing_required_tables == []


def test_missing_required_tables_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_knowledge_citation_freshness_report(conn, now=NOW)

    assert report.linked_knowledge_count == 0
    assert report.missing_required_tables == [
        "generated_content",
        "content_knowledge_links",
        "knowledge",
    ]


def test_cli_outputs_json_and_can_fail_on_blocked(db, capsys):
    content_id = _content(db)
    knowledge_id = _knowledge(db, "stale-cli", published_days_ago=120)
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.9)])

    with patch.object(
        check_citation_freshness_script,
        "script_context",
        return_value=_script_context(db),
    ):
        exit_code = check_citation_freshness_script.main(
            [
                "--content-id",
                str(content_id),
                "--days",
                "90",
                "--format",
                "json",
                "--fail-on-blocked",
            ]
        )

    assert exit_code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["blocked_count"] == 1
    assert payload["items"][0]["reason_codes"] == ["stale_knowledge"]

    with patch.object(
        check_citation_freshness_script,
        "script_context",
        return_value=_script_context(db),
    ):
        exit_code = check_citation_freshness_script.main(["--allow-missing-canonical"])

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "Knowledge Citation Freshness" in output
