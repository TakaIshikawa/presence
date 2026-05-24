from __future__ import annotations
import importlib.util, json, sqlite3
from datetime import datetime, timezone
from pathlib import Path
from evaluation.knowledge_source_citation_recency import build_knowledge_source_citation_recency_report_from_db, format_knowledge_source_citation_recency_json, format_knowledge_source_citation_recency_text
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"knowledge_source_citation_recency.py"; spec=importlib.util.spec_from_file_location("script_knowledge_source_citation_recency",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.executescript("CREATE TABLE content_knowledge_links (content_id TEXT, source_id TEXT, cited_at TEXT); CREATE TABLE knowledge_sources (id TEXT, url TEXT, title TEXT, last_refreshed_at TEXT, topic TEXT);")
    c.execute("INSERT INTO content_knowledge_links VALUES (?,?,?)",("c1","s1","2026-05-24T00:00:00+00:00")); c.execute("INSERT INTO knowledge_sources VALUES (?,?,?,?,?)",("s1","https://docs.example.com/a","Old source","2025-01-01T00:00:00+00:00","ai")); c.commit(); return c
def test_report_db_formatters_and_cli(tmp_path,capsys):
    r=build_knowledge_source_citation_recency_report_from_db(_db(),now=datetime(2026,5,25,tzinfo=timezone.utc),max_age_days=30); assert r["artifact_type"]=="knowledge_source_citation_recency"; assert r["findings"]; assert r["summary"]["by_topic"]["ai"]==1
    assert json.loads(format_knowledge_source_citation_recency_json(r))["artifact_type"]=="knowledge_source_citation_recency"; assert "Knowledge" in format_knowledge_source_citation_recency_text(r)
    db=tmp_path/"db.sqlite"; out=sqlite3.connect(db); _db().backup(out); out.close(); assert script.main(["--db",str(db),"--format","text","--max-age-days","30"])==0; assert capsys.readouterr().out; assert script.main(["--db",str(db),"--limit","0"])==2
def test_missing_link_table_returns_empty_with_schema_gap():
    r=build_knowledge_source_citation_recency_report_from_db(sqlite3.connect(":memory:")); assert r["findings"]==[] and r["missing_tables"]
