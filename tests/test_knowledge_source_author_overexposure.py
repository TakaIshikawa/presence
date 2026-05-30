from __future__ import annotations
from datetime import datetime, timezone
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.knowledge_source_author_overexposure import build_knowledge_source_author_overexposure_report, build_knowledge_source_author_overexposure_report_from_db, format_knowledge_source_author_overexposure_json, format_knowledge_source_author_overexposure_text
NOW=datetime(2026,5,24,12,tzinfo=timezone.utc)
SCRIPT_PATH=Path(__file__).resolve().parent.parent/"scripts"/"knowledge_source_author_overexposure.py"; spec=importlib.util.spec_from_file_location("knowledge_source_author_overexposure_script",SCRIPT_PATH); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_computes_author_share_recent_window_and_unknown_bucket():
    rows=[{"content_id":"c1","source_id":"s1","author":"Ada","linked_at":"2026-05-20T00:00:00+00:00"},{"content_id":"c2","source_id":"s2","author":"Ada","linked_at":"2026-05-21T00:00:00+00:00"},{"content_id":"c3","source_id":"s3","author":"","linked_at":"2026-05-22T00:00:00+00:00"},{"content_id":"old","source_id":"s4","author":"Ada","linked_at":"2026-03-01T00:00:00+00:00"}]
    r=build_knowledge_source_author_overexposure_report(rows,window_days=10,max_author_share=0.5,now=NOW)
    assert r["findings"][0]["author"]=="Ada"
    assert r["findings"][0]["author_share"]==0.6667
    assert r["findings"][0]["content_count"]==2
def test_db_join_metadata_formatters_cli_and_missing_schema(tmp_path,capsys):
    conn=sqlite3.connect(":memory:"); conn.execute("CREATE TABLE knowledge_sources (id TEXT, metadata TEXT)"); conn.execute("CREATE TABLE content_knowledge_links (content_id TEXT, source_id TEXT, linked_at TEXT)")
    conn.executemany("INSERT INTO knowledge_sources VALUES (?,?)",[("s1",json.dumps({"author":"Ada"})),("s2",json.dumps({"byline":"Ada"})),("s3","{}")])
    conn.executemany("INSERT INTO content_knowledge_links VALUES (?,?,?)",[("c1","s1","2026-05-20T00:00:00+00:00"),("c2","s2","2026-05-21T00:00:00+00:00"),("c3","s3","2026-05-22T00:00:00+00:00")])
    r=build_knowledge_source_author_overexposure_report_from_db(conn,window_days=10,max_author_share=0.5,now=NOW)
    assert r["findings"][0]["author"]=="Ada"
    assert json.loads(format_knowledge_source_author_overexposure_json(r))["artifact_type"]=="knowledge_source_author_overexposure"
    assert "Knowledge Source Author Overexposure" in format_knowledge_source_author_overexposure_text(r)
    db=tmp_path/"ks.sqlite"; conn.commit(); out=sqlite3.connect(db); conn.backup(out); out.close()
    assert script.main(["--db",str(db),"--window-days","30","--max-author-share","0.5"])==0
    assert json.loads(capsys.readouterr().out)["summary"]["finding_count"]==1
    assert build_knowledge_source_author_overexposure_report_from_db(sqlite3.connect(":memory:"),now=NOW)["missing_tables"]==["content_knowledge_links","knowledge_sources"]
