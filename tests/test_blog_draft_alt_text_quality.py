from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path

from evaluation.blog_draft_alt_text_quality import (
    build_blog_draft_alt_text_quality_report,
    build_blog_draft_alt_text_quality_report_from_db,
    format_blog_draft_alt_text_quality_json,
    format_blog_draft_alt_text_quality_text,
)


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "blog_draft_alt_text_quality.py"
spec = importlib.util.spec_from_file_location("blog_draft_alt_text_quality_script", SCRIPT)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_markdown_html_and_issue_classification():
    report = build_blog_draft_alt_text_quality_report(
        [
            {
                "draft_id": "d1",
                "body": "![](missing.png) ![ok](short.png) ![hero image](hero-image.jpg) "
                "<img src='dup1.png' alt='Same useful alt text'> <img src='dup2.png' alt='Same useful alt text'>",
            }
        ],
        now=NOW,
        min_chars=6,
    )
    issues = {item["issue_type"] for item in report["findings"]}
    assert {"missing", "short", "filename_like", "duplicate"} <= issues
    finding = report["findings"][0]
    assert {"draft_id", "image_src", "alt_text", "issue_type", "recommendation"} <= set(finding)
    assert json.loads(format_blog_draft_alt_text_quality_json(report))["artifact_type"] == "blog_draft_alt_text_quality"
    assert "Blog Draft Alt Text Quality" in format_blog_draft_alt_text_quality_text(report)


def test_db_builder_filters_drafts_and_cli(tmp_path, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE blog_drafts (id TEXT, body TEXT, status TEXT)")
    conn.execute("INSERT INTO blog_drafts VALUES ('d1', '![](missing.png)', 'draft')")
    conn.execute("INSERT INTO blog_drafts VALUES ('p1', '![](published.png)', 'published')")
    report = build_blog_draft_alt_text_quality_report_from_db(conn, now=NOW)
    assert report["summary"]["drafts"] == 1
    assert report["findings"][0]["draft_id"] == "d1"
    db = tmp_path / "db.sqlite"
    out = sqlite3.connect(db)
    conn.commit()
    conn.backup(out)
    out.close()
    assert script.main(["--db", str(db), "--format", "text"]) == 0
    assert "d1 | missing.png | missing" in capsys.readouterr().out
    assert script.main(["--db", str(db), "--limit", "0"]) == 2


def test_generated_content_and_missing_schema():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE generated_content (id TEXT, content_type TEXT, content TEXT, status TEXT)")
    conn.execute("INSERT INTO generated_content VALUES ('g1', 'blog_post', '<img src=\"x.png\">', 'draft')")
    report = build_blog_draft_alt_text_quality_report_from_db(conn, now=NOW)
    assert report["findings"][0]["draft_id"] == "g1"
    missing = build_blog_draft_alt_text_quality_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["blog_drafts|generated_content"]
    assert missing["empty_state"]["reason"] == "missing_schema"
