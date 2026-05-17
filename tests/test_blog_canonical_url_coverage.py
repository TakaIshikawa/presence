"""Tests for blog canonical URL coverage reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.blog_canonical_url_coverage import (
    build_blog_canonical_url_coverage_report,
    format_blog_canonical_url_coverage_json,
    format_blog_canonical_url_coverage_text,
)


NOW = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "blog_canonical_url_coverage.py"
spec = importlib.util.spec_from_file_location("blog_canonical_url_coverage_script", SCRIPT_PATH)
blog_canonical_url_coverage_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(blog_canonical_url_coverage_script)


@contextmanager
def _script_context(conn: sqlite3.Connection):
    yield SimpleNamespace(), SimpleNamespace(conn=conn)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            content_type TEXT,
            title TEXT,
            slug TEXT,
            canonical_url TEXT,
            content TEXT,
            published INTEGER
        )"""
    )
    conn.execute(
        """CREATE TABLE content_publications (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            platform TEXT,
            status TEXT,
            published_url TEXT
        )"""
    )
    return conn


def _post(conn: sqlite3.Connection, title: str, slug: str, canonical_url: str | None, published: int = 1) -> int:
    cur = conn.execute(
        """INSERT INTO generated_content
           (content_type, title, slug, canonical_url, content, published)
           VALUES ('blog_post', ?, ?, ?, ?, ?)""",
        (title, slug, canonical_url, f"# {title}\nslug: {slug}", published),
    )
    conn.commit()
    return int(cur.lastrowid)


def test_complete_canonical_coverage_has_no_issues():
    conn = _conn()
    content_id = _post(conn, "Complete", "complete", "https://example.com/blog/complete")
    conn.execute(
        "INSERT INTO content_publications (content_id, platform, status, published_url) VALUES (?, 'blog', 'published', ?)",
        (content_id, "https://example.com/blog/complete"),
    )
    conn.commit()

    report = build_blog_canonical_url_coverage_report(
        conn,
        expected_base_url="https://example.com/blog",
        now=NOW,
    )

    assert report.summary["total_posts"] == 1
    assert report.summary["covered_posts"] == 1
    assert report.summary["missing_count"] == 0
    assert report.summary["mismatch_count"] == 0
    assert report.issues == ()


def test_missing_canonical_url_is_reported():
    conn = _conn()
    _post(conn, "Missing", "missing", None)

    report = build_blog_canonical_url_coverage_report(conn, now=NOW)

    assert report.summary["total_posts"] == 1
    assert report.summary["covered_posts"] == 0
    assert report.summary["missing_count"] == 1
    assert report.summary["coverage_rate"] == 0.0
    assert report.issues[0].issue_type == "missing_canonical_url"


def test_slug_and_publication_url_mismatches_are_reported():
    conn = _conn()
    content_id = _post(conn, "Mismatch", "expected-slug", "https://example.com/blog/other-slug")
    conn.execute(
        "INSERT INTO content_publications (content_id, platform, status, published_url) VALUES (?, 'blog', 'published', ?)",
        (content_id, "https://example.com/blog/expected-slug"),
    )
    conn.commit()

    report = build_blog_canonical_url_coverage_report(
        conn,
        expected_base_url="https://example.com/blog",
        now=NOW,
    )

    assert report.summary["covered_posts"] == 1
    assert report.summary["mismatch_count"] == 2
    assert [issue.issue_type for issue in report.issues] == [
        "canonical_publication_mismatch",
        "canonical_slug_mismatch",
    ]


def test_json_text_and_cli_output(monkeypatch, capsys):
    conn = _conn()
    _post(conn, "CLI", "cli", "https://example.com/blog/cli")
    monkeypatch.setattr(blog_canonical_url_coverage_script, "script_context", lambda: _script_context(conn))
    monkeypatch.setattr(
        blog_canonical_url_coverage_script,
        "build_blog_canonical_url_coverage_report",
        lambda db, **kwargs: build_blog_canonical_url_coverage_report(db, now=NOW, **kwargs),
    )

    report = build_blog_canonical_url_coverage_report(conn, now=NOW)
    payload = json.loads(format_blog_canonical_url_coverage_json(report))
    text = format_blog_canonical_url_coverage_text(report)
    exit_code = blog_canonical_url_coverage_script.main(["--format", "json"])
    cli_payload = json.loads(capsys.readouterr().out)

    assert payload["artifact_type"] == "blog_canonical_url_coverage"
    assert "Blog Canonical URL Coverage" in text
    assert cli_payload["summary"]["covered_posts"] == 1
    assert exit_code == 0
