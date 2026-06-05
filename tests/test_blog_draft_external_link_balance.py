from __future__ import annotations
from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util, json, sqlite3
from pathlib import Path
from types import SimpleNamespace
import pytest

from evaluation.blog_draft_external_link_balance import build_blog_draft_external_link_balance_report, build_blog_draft_external_link_balance_report_from_db, extract_external_links, format_blog_draft_external_link_balance_json, format_blog_draft_external_link_balance_text

SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "blog_draft_external_link_balance.py"
spec = importlib.util.spec_from_file_location("blog_draft_external_link_balance_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db

def test_extracts_markdown_and_bare_urls_but_ignores_non_external_links():
    text = "[a](https://a.com/x) https://b.com/y [mail](mailto:x@y.com) [rel](/post) [anchor](#top) https://own.test/page"
    assert [u for u in extract_external_links(text, own_domains={"own.test"})] == ["https://a.com/x", "https://b.com/y"]

def test_flags_too_few_too_many_and_domain_concentration():
    rows = [
        {"id": 1, "content_id": 11, "title": "Few", "status": "draft", "body": "https://a.com/1", "updated_at": "2026-06-01T00:00:00+00:00"},
        {"id": 2, "content_id": 12, "title": "Many", "status": "ready", "body": " ".join(f"https://d{i}.com/x" for i in range(5)), "updated_at": "2026-06-01T00:00:00+00:00"},
        {"id": 3, "content_id": 13, "title": "Concentrated", "status": "review", "body": "https://a.com/1 https://a.com/2 https://b.com/1", "updated_at": "2026-06-01T00:00:00+00:00"},
        {"id": 4, "title": "Own", "status": "draft", "body": "https://own.test/a https://b.com/1 https://c.com/1", "updated_at": "2026-06-01T00:00:00+00:00"},
    ]
    report = build_blog_draft_external_link_balance_report(rows, min_external_links=2, max_external_links=4, max_domain_share=0.6, own_domains=("own.test",), now=datetime(2026, 6, 5, tzinfo=timezone.utc))
    by_title = {f["title"]: f for f in report["findings"]}
    assert by_title["Few"]["gap_reasons"] == ["too_few_external_links"]
    assert by_title["Many"]["gap_reasons"] == ["too_many_external_links"]
    assert by_title["Concentrated"]["top_domain"] == "a.com"
    assert by_title["Concentrated"]["top_domain_share"] == 0.6667
    assert "Own" not in by_title

def test_filters_status_and_lookback():
    report = build_blog_draft_external_link_balance_report(
        [
            {"id": 1, "status": "archived", "body": "none", "updated_at": "2026-06-04T00:00:00+00:00"},
            {"id": 2, "status": "draft", "body": "none", "updated_at": "2025-01-01T00:00:00+00:00"},
        ],
        now=datetime(2026, 6, 5, tzinfo=timezone.utc),
    )
    assert report["findings"] == []

def test_db_builder_and_formatters(tmp_path, capsys, monkeypatch):
    db_path = tmp_path / "drafts.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript("CREATE TABLE blog_drafts(id INTEGER, content_id INTEGER, title TEXT, body TEXT, status TEXT, updated_at TEXT); INSERT INTO blog_drafts VALUES(1,10,'Few','https://a.com','draft','2026-06-01T00:00:00+00:00');")
    conn.close()
    with sqlite3.connect(db_path) as db:
        report = build_blog_draft_external_link_balance_report_from_db(db, now=datetime(2026, 6, 5, tzinfo=timezone.utc))
    assert report["findings"][0]["draft_id"] == 1
    assert json.loads(format_blog_draft_external_link_balance_json(report))["artifact_type"] == "blog_draft_external_link_balance"
    assert "Few" in format_blog_draft_external_link_balance_text(report)
    assert script.main(["--db", str(db_path), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["findings"][0]["content_id"] == 10
    monkeypatch.setattr(script, "script_context", lambda: _script_context(sqlite3.connect(":memory:")))
    assert script.main(["--format", "json"]) == 0

def test_invalid_args():
    with pytest.raises(ValueError):
        build_blog_draft_external_link_balance_report([], min_external_links=3, max_external_links=2)
    with pytest.raises(SystemExit):
        script.parse_args(["--max-domain-share", "2"])
