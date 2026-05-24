from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.blog_draft_fact_density import build_blog_draft_fact_density_report_from_db, format_blog_draft_fact_density_json, format_blog_draft_fact_density_text

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "blog_draft_fact_density.py"
spec = importlib.util.spec_from_file_location("blog_draft_fact_density_script", SCRIPT)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _db() -> sqlite3.Connection:
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.executescript("""CREATE TABLE generated_content (id INTEGER PRIMARY KEY, title TEXT, content_type TEXT, status TEXT, body TEXT); CREATE TABLE content_claims (content_id INTEGER); CREATE TABLE content_knowledge_links (content_id INTEGER);""")
    return c


def test_fact_density_flags_risks_and_formats(tmp_path, capsys):
    c = _db()
    c.execute("INSERT INTO generated_content VALUES (1,'Thin','blog','draft','soft intro words only')")
    c.execute("INSERT INTO generated_content VALUES (2,'Quoted','blog','draft','\"quote\" \"quote\" \"quote\"')")
    c.execute("INSERT INTO generated_content VALUES (3,'Ok','blog','draft','According to data 42 percent because evidence')")
    c.executemany("INSERT INTO content_knowledge_links VALUES (3)", [(), (), ()])
    report = build_blog_draft_fact_density_report_from_db(c, min_facts_per_100_words=10)
    assert [r["content_id"] for r in report["findings"]] == ["1", "2"]
    assert "quote_heavy" in report["findings"][1]["risk_reason"]
    assert json.loads(format_blog_draft_fact_density_json(report))["artifact_type"] == "blog_draft_fact_density"
    assert "Blog Draft Fact Density" in format_blog_draft_fact_density_text(report)
    c.commit()
    path = tmp_path / "db.sqlite"
    with sqlite3.connect(path) as out:
        c.backup(out)
    assert script.main(["--db", str(path), "--format", "text", "--limit", "2"]) == 0
    assert "Blog Draft Fact Density" in capsys.readouterr().out
    assert script.main(["--db", str(path), "--limit", "0"]) == 2


def test_missing_generated_content_table():
    report = build_blog_draft_fact_density_report_from_db(sqlite3.connect(":memory:"))
    assert report["missing_tables"] == ["generated_content"]

