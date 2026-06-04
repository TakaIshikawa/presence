from __future__ import annotations
import importlib.util, json, sqlite3
from datetime import datetime, timezone
from pathlib import Path

from evaluation.blog_draft_internal_link_gaps import build_blog_draft_internal_link_gaps_report, build_blog_draft_internal_link_gaps_report_from_db, extract_links, format_blog_draft_internal_link_gaps_json, format_blog_draft_internal_link_gaps_text

NOW = datetime(2026, 6, 1, tzinfo=timezone.utc)
SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "blog_draft_internal_link_gaps.py"
spec = importlib.util.spec_from_file_location("script_blog_draft_internal_link_gaps", SCRIPT)
script = importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)


def test_extracts_markdown_and_html_links():
    assert extract_links("[a](/about) <a href='https://example.com/blog'>b</a>") == ["/about", "https://example.com/blog"]


def test_flags_external_only_publishable_draft():
    report = build_blog_draft_internal_link_gaps_report(
        [{"id": "d1", "title": "Draft", "slug": "draft", "status": "ready", "body": "[x](https://elsewhere.test/a)"}],
        allowed_domains=["example.com"],
        min_internal_links=1,
        now=NOW,
    )
    assert report["gaps"][0]["draft_id"] == "d1"
    assert report["gaps"][0]["internal_link_count"] == 0
    assert report["gaps"][0]["external_link_count"] == 1


def test_treats_relative_and_configured_domain_as_internal():
    report = build_blog_draft_internal_link_gaps_report(
        [{"id": "d1", "status": "ready", "body": "[a](/about) <a href=\"https://www.example.com/blog/post\">post</a>"}],
        allowed_domains=["example.com"],
        min_internal_links=2,
        now=NOW,
    )
    assert report["gaps"] == []


def test_threshold_and_eligibility_filters():
    rows = [
        {"id": "draft", "status": "draft", "body": "[a](/one)"},
        {"id": "published", "status": "published", "body": ""},
        {"id": "blocked", "status": "ready", "publishable": "false", "body": ""},
    ]
    report = build_blog_draft_internal_link_gaps_report(rows, min_internal_links=2, allowed_domains=["example.com"], now=NOW)
    assert [g["draft_id"] for g in report["gaps"]] == ["draft"]


def test_db_formatters_and_cli(tmp_path, capsys):
    db = tmp_path / "db.sqlite"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE blog_drafts (id TEXT, title TEXT, slug TEXT, status TEXT, body TEXT, updated_at TEXT)")
    conn.execute("INSERT INTO blog_drafts VALUES (?,?,?,?,?,?)", ("d1", "Draft", "draft", "ready", "[x](https://external.test)", "2026-05-31T00:00:00+00:00"))
    conn.commit(); conn.close()
    with sqlite3.connect(db) as read:
        read.row_factory = sqlite3.Row
        report = build_blog_draft_internal_link_gaps_report_from_db(read, allowed_domains=["example.com"], min_internal_links=1, now=NOW)
    assert json.loads(format_blog_draft_internal_link_gaps_json(report))["artifact_type"] == "blog_draft_internal_link_gaps"
    assert "Blog Draft" in format_blog_draft_internal_link_gaps_text(report)
    assert script.main(["--db", str(db), "--format", "text", "--allowed-domain", "example.com", "--min-internal-links", "1"]) == 0
    assert capsys.readouterr().out
    assert script.main(["--db", str(db), "--limit", "0"]) == 2


def test_empty_and_missing_schema():
    report = build_blog_draft_internal_link_gaps_report([{"id": "d1", "status": "ready", "body": "[a](/one)"}], min_internal_links=1, now=NOW)
    assert report["empty_state"]["is_empty"]
    assert build_blog_draft_internal_link_gaps_report_from_db(sqlite3.connect(":memory:"))["missing_tables"]
