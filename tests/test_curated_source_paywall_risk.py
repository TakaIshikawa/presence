"""Tests for curated source paywall risk reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from knowledge.curated_source_paywall_risk import (
    build_curated_source_paywall_risk_report,
    format_curated_source_paywall_risk_json,
    format_curated_source_paywall_risk_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "curated_source_paywall_risk.py"
spec = importlib.util.spec_from_file_location("curated_source_paywall_risk_script", SCRIPT_PATH)
curated_source_paywall_risk_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(curated_source_paywall_risk_script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE curated_sources (
            id INTEGER PRIMARY KEY,
            source_type TEXT,
            identifier TEXT,
            name TEXT,
            feed_url TEXT,
            canonical_url TEXT,
            homepage_url TEXT,
            link_title TEXT,
            site_name TEXT,
            status TEXT,
            last_fetch_status TEXT,
            last_error TEXT,
            metadata TEXT,
            created_at TEXT
        );
        CREATE TABLE knowledge (
            id INTEGER PRIMARY KEY,
            source_type TEXT,
            source_id TEXT,
            source_url TEXT,
            author TEXT,
            content TEXT,
            insight TEXT,
            published_at TEXT,
            ingested_at TEXT,
            created_at TEXT,
            metadata TEXT
        );
        """
    )
    return conn


def test_metadata_driven_blocked_status_is_reported():
    conn = _conn()
    conn.execute(
        """INSERT INTO curated_sources
           (id, source_type, identifier, name, canonical_url, metadata)
           VALUES (1, 'blog', 'blocked.example.com', 'Blocked',
                   'https://blocked.example.com/post',
                   '{"link_metadata": {"status_code": 403}}')"""
    )

    report = build_curated_source_paywall_risk_report(conn, now=NOW)
    by_type = {risk.risk_type: risk for risk in report.risks}

    assert by_type["blocked_status"].source_id == 1
    assert by_type["blocked_status"].confidence == 0.95
    assert by_type["missing_excerpt"].risk_type == "missing_excerpt"
    assert "blocked_status" in format_curated_source_paywall_risk_text(report)


def test_heuristic_paywall_detection_uses_url_and_metadata_terms():
    conn = _conn()
    conn.execute(
        """INSERT INTO curated_sources
           (id, source_type, identifier, name, canonical_url, link_title, metadata)
           VALUES (2, 'newsletter', 'premium.example.com', 'Premium Letter',
                   'https://example.substack.com/p/story',
                   'Subscriber-only analysis',
                   '{"access": "subscription required"}')"""
    )

    report = build_curated_source_paywall_risk_report(conn, now=NOW)

    risk_types = {risk.risk_type for risk in report.risks}
    assert "paywall_keyword" in risk_types
    assert report.totals["by_risk_type"]["paywall_keyword"] == 1


def test_source_type_filtering_and_recent_excerpt_suppress_missing_excerpt():
    conn = _conn()
    conn.execute(
        """INSERT INTO curated_sources
           (id, source_type, identifier, name, canonical_url)
           VALUES (1, 'blog', 'blog.example.com', 'Blog', 'https://blog.example.com')"""
    )
    conn.execute(
        """INSERT INTO curated_sources
           (id, source_type, identifier, name, canonical_url, last_error)
           VALUES (2, 'newsletter', 'letter.example.com', 'Letter',
                   'https://letter.example.com', 'login required')"""
    )
    conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, content, insight, ingested_at)
           VALUES ('curated_article', 'blog.example.com', 'https://blog.example.com/post',
                   'accessible excerpt', '', '2026-05-01T10:00:00+00:00')"""
    )

    blog_report = build_curated_source_paywall_risk_report(
        conn,
        source_type="blog",
        now=NOW,
    )
    newsletter_report = build_curated_source_paywall_risk_report(
        conn,
        source_type="newsletter",
        now=NOW,
    )

    assert blog_report.risks == ()
    assert blog_report.totals["sources_scanned"] == 1
    assert [risk.risk_type for risk in newsletter_report.risks] == [
        "login_required",
        "missing_excerpt",
    ]


def test_json_cli_output_and_min_confidence(capsys, tmp_path):
    db_path = tmp_path / "sources.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE curated_sources (
            id INTEGER PRIMARY KEY,
            source_type TEXT,
            identifier TEXT,
            name TEXT,
            feed_url TEXT,
            canonical_url TEXT,
            last_fetch_status TEXT,
            last_error TEXT,
            metadata TEXT
        );
        CREATE TABLE knowledge (
            id INTEGER PRIMARY KEY,
            source_type TEXT,
            source_id TEXT,
            source_url TEXT,
            content TEXT,
            insight TEXT,
            ingested_at TEXT
        );
        INSERT INTO curated_sources
          (id, source_type, identifier, name, canonical_url, metadata)
        VALUES
          (1, 'blog', 'cli.example.com', 'CLI',
           'https://cli.example.com/post', '{"http_status": 451}');
        """
    )
    conn.close()

    assert curated_source_paywall_risk_script.main(
        [
            "--db",
            str(db_path),
            "--days",
            "30",
            "--min-confidence",
            "0.9",
            "--format",
            "json",
        ]
    ) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["artifact_type"] == "curated_source_paywall_risk"
    assert [risk["risk_type"] for risk in payload["risks"]] == ["blocked_status"]
    assert payload["filters"]["min_confidence"] == 0.9

    assert curated_source_paywall_risk_script.main(["--min-confidence", "1.5"]) == 2
    assert "between 0 and 1" in capsys.readouterr().err


def test_missing_knowledge_schema_empty_state():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE curated_sources (
            id INTEGER PRIMARY KEY,
            source_type TEXT,
            identifier TEXT
        )"""
    )

    report = build_curated_source_paywall_risk_report(conn, now=NOW)
    payload = json.loads(format_curated_source_paywall_risk_json(report))

    assert payload["empty_state"]["is_empty"] is True
    assert payload["missing_tables"] == ["knowledge"]
