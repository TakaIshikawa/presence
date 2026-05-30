from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path

from evaluation.blog_draft_image_dimension_gaps import (
    build_blog_draft_image_dimension_gaps_report,
    build_blog_draft_image_dimension_gaps_report_from_db,
    format_blog_draft_image_dimension_gaps_json,
    format_blog_draft_image_dimension_gaps_text,
)


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "blog_draft_image_dimension_gaps.py"
spec = importlib.util.spec_from_file_location("blog_draft_image_dimension_gaps_script", SCRIPT)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_markdown_attributes_html_and_minimum_warnings():
    report = build_blog_draft_image_dimension_gaps_report(
        [
            {
                "draft_id": "d1",
                "path": "drafts/post.md",
                "body": "![hero](hero.jpg){width=120 height=80} ![partial](partial.jpg){width=500} <img src='missing.png'> <img src=\"ok.png\" width=\"640\" height=\"360\">",
            }
        ],
        now=NOW,
        min_width=300,
        min_height=200,
    )
    reasons = {item["reason"] for item in report["issues"]}
    assert {"width_below_minimum", "height_below_minimum", "partial_dimensions", "missing_dimensions"} <= reasons
    issue = report["issues"][0]
    assert {"draft_id", "draft_path", "image_src", "reason", "recommendation"} <= set(issue)
    assert json.loads(format_blog_draft_image_dimension_gaps_json(report))["artifact_type"] == "blog_draft_image_dimension_gaps"
    assert "Blog Draft Image Dimension Gaps" in format_blog_draft_image_dimension_gaps_text(report)


def test_db_builder_filters_drafts_and_cli(tmp_path, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE blog_drafts (id TEXT, path TEXT, body TEXT, status TEXT)")
    conn.execute("INSERT INTO blog_drafts VALUES ('d1', 'a.md', '<img src=\"x.png\">', 'draft')")
    conn.execute("INSERT INTO blog_drafts VALUES ('p1', 'b.md', '<img src=\"y.png\">', 'published')")
    report = build_blog_draft_image_dimension_gaps_report_from_db(conn, now=NOW)
    assert report["summary"]["drafts"] == 1
    assert report["issues"][0]["draft_id"] == "d1"
    db = tmp_path / "db.sqlite"
    out = sqlite3.connect(db)
    conn.commit()
    conn.backup(out)
    out.close()
    assert script.main(["--db", str(db), "--format", "text"]) == 0
    assert "d1 | a.md | x.png | missing_dimensions" in capsys.readouterr().out
    assert script.main(["--db", str(db), "--min-width", "0"]) == 2


def test_generated_content_metadata_and_missing_schema():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE generated_content (id TEXT, content_type TEXT, content TEXT, status TEXT, metadata TEXT)")
    conn.execute(
        "INSERT INTO generated_content VALUES ('g1', 'blog_post', '', 'draft', ?)",
        (json.dumps({"images": [{"src": "meta.jpg", "width": 100, "height": 100}]}),),
    )
    report = build_blog_draft_image_dimension_gaps_report_from_db(conn, now=NOW, min_width=300, min_height=200)
    assert {item["reason"] for item in report["issues"]} == {"width_below_minimum", "height_below_minimum"}
    missing = build_blog_draft_image_dimension_gaps_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["blog_drafts|generated_content"]
