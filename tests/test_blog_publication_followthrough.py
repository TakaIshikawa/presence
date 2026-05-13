from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.blog_publication_followthrough import build_blog_publication_followthrough_report


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "blog_publication_followthrough.py"
spec = importlib.util.spec_from_file_location("blog_publication_followthrough_script", SCRIPT)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY, content_type TEXT, title TEXT, content TEXT, created_at TEXT
        );
        CREATE TABLE content_publications (
            id INTEGER PRIMARY KEY, content_id INTEGER, platform TEXT, url TEXT, published_at TEXT
        );"""
    )
    return conn


def test_stale_unpublished_drafts():
    conn = _conn()
    conn.execute("INSERT INTO generated_content VALUES (1, 'blog_post', 'Draft', 'Long blog body', '2026-04-20T00:00:00+00:00')")

    report = build_blog_publication_followthrough_report(conn, min_age_days=3, now=NOW)

    assert report.issues[0].issue_type == "unpublished_blog_draft"
    assert report.issues[0].content_preview == "Long blog body"


def test_published_long_form_missing_url():
    conn = _conn()
    conn.execute("INSERT INTO generated_content VALUES (2, 'long_form', 'Essay', 'Essay body', '2026-04-20T00:00:00+00:00')")
    conn.execute("INSERT INTO content_publications VALUES (10, 2, 'blog', NULL, '2026-04-22T00:00:00+00:00')")

    report = build_blog_publication_followthrough_report(conn, now=NOW)

    assert report.issues[0].issue_type == "missing_blog_url"
    assert report.issues[0].publication_id == 10


def test_orphan_publication_rows():
    conn = _conn()
    conn.execute("INSERT INTO content_publications VALUES (11, 99, 'blog', 'https://example.com/post', '2026-04-22T00:00:00+00:00')")

    report = build_blog_publication_followthrough_report(conn, now=NOW)

    assert report.issues[0].issue_type == "orphan_blog_publication"
    assert report.issues[0].content_id == 99


def test_filters_and_empty_state():
    conn = _conn()
    conn.execute("INSERT INTO generated_content VALUES (1, 'blog_post', 'Draft', 'Long blog body', '2026-04-20T00:00:00+00:00')")

    report = build_blog_publication_followthrough_report(conn, issue_type="missing_blog_url", now=NOW)

    assert report.issues == ()
    assert report.empty_state["is_empty"] is True


def test_cli_json_output(capsys, tmp_path):
    db_path = tmp_path / "blog.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY, content_type TEXT, title TEXT, content TEXT, created_at TEXT
        );
        CREATE TABLE content_publications (
            id INTEGER PRIMARY KEY, content_id INTEGER, platform TEXT, url TEXT, published_at TEXT
        );
        INSERT INTO generated_content VALUES (1, 'blog_post', 'Draft', 'Long blog body', '2026-04-20T00:00:00+00:00');"""
    )
    conn.close()

    assert script.main(["--db", str(db_path), "--days", "90", "--min-age-days", "1", "--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["artifact_type"] == "blog_publication_followthrough"
    assert payload["issues"][0]["issue_type"] == "unpublished_blog_draft"
