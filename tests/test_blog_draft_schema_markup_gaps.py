from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.blog_draft_schema_markup_gaps import build_blog_draft_schema_markup_gaps_report_from_db, format_blog_draft_schema_markup_gaps_json, format_blog_draft_schema_markup_gaps_text

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "blog_draft_schema_markup_gaps.py"
spec = importlib.util.spec_from_file_location("script_blog_draft_schema_markup_gaps", SCRIPT); script = importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)


def _db():
    c = sqlite3.connect(":memory:"); c.row_factory = sqlite3.Row
    c.executescript("CREATE TABLE blog_drafts (id TEXT, title TEXT, status TEXT, schema_type TEXT, canonical_url TEXT, published_at TEXT, author TEXT, image TEXT, metadata TEXT);")
    c.execute("INSERT INTO blog_drafts VALUES (?,?,?,?,?,?,?,?,?)", ("d1", "Ready Missing", "ready", "", "notaurl", "bad-date", "", "", None))
    c.execute("INSERT INTO blog_drafts VALUES (?,?,?,?,?,?,?,?,?)", ("d2", "Good", "ready", "BlogPosting", "https://example.com/good", "2026-05-01T00:00:00Z", "Ada", "https://example.com/i.jpg", None))
    c.execute("INSERT INTO blog_drafts VALUES (?,?,?,?,?,?,?,?,?)", ("d3", "Draft", "draft", "", "", "", "", "", None))
    c.commit(); return c


def test_schema_markup_report_formatters_and_cli(tmp_path, capsys):
    r = build_blog_draft_schema_markup_gaps_report_from_db(_db())
    assert r["artifact_type"] == "blog_draft_schema_markup_gaps"
    assert r["summary"]["publishable_count"] == 2
    assert r["schema_gaps"][0]["draft_id"] == "d1"
    assert json.loads(format_blog_draft_schema_markup_gaps_json(r))["schema_gaps"][0]["reason"]
    text = format_blog_draft_schema_markup_gaps_text(r)
    assert "Blog Draft Schema Markup Gaps" in text and "d1" in text and "Ready Missing" in text
    db = tmp_path / "db.sqlite"; out = sqlite3.connect(db); _db().backup(out); out.close()
    assert script.main(["--db", str(db), "--format", "text", "--limit", "10"]) == 0
    assert "Totals:" in capsys.readouterr().out
    assert script.main(["--db", str(db), "--limit", "0"]) == 2


def test_missing_schema_records_metadata():
    r = build_blog_draft_schema_markup_gaps_report_from_db(sqlite3.connect(":memory:"))
    assert r["missing_tables"]
