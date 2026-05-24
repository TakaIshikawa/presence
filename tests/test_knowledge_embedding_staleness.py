from __future__ import annotations
from datetime import datetime,timezone
import importlib.util,json,sqlite3
from pathlib import Path
from evaluation.knowledge_embedding_staleness import build_knowledge_embedding_staleness_report_from_db,format_knowledge_embedding_staleness_json,format_knowledge_embedding_staleness_text
NOW=datetime(2026,5,24,tzinfo=timezone.utc); SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"knowledge_embedding_staleness.py"; spec=importlib.util.spec_from_file_location("knowledge_embedding_staleness_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_embedding_staleness_and_cli(tmp_path,capsys):
 c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.executescript("CREATE TABLE knowledge (id INTEGER, source_type TEXT, source_url TEXT, updated_at TEXT); CREATE TABLE knowledge_embeddings (knowledge_id INTEGER, embedded_at TEXT, embedding_model TEXT);")
 c.execute("INSERT INTO knowledge VALUES (1,'url','https://e','2026-05-20T00:00:00+00:00')"); c.execute("INSERT INTO knowledge VALUES (2,'url','https://m','2026-05-01T00:00:00+00:00')"); c.execute("INSERT INTO knowledge_embeddings VALUES (1,'2026-05-01T00:00:00+00:00','old')")
 r=build_knowledge_embedding_staleness_report_from_db(c,current_model="new",now=NOW)
 assert {f["knowledge_id"] for f in r["findings"]}=={"1","2"}; assert "model_outdated" in r["findings"][1]["reason"] or "model_outdated" in r["findings"][0]["reason"]; assert json.loads(format_knowledge_embedding_staleness_json(r))["artifact_type"]=="knowledge_embedding_staleness"; assert "Knowledge Embedding" in format_knowledge_embedding_staleness_text(r)
 c.commit(); path=tmp_path/"db.sqlite"
 with sqlite3.connect(path) as out: c.backup(out)
 assert script.main(["--db",str(path),"--format","text","--current-model","new"])==0; assert "Knowledge Embedding" in capsys.readouterr().out; assert script.main(["--db",str(path),"--max-age-days","0"])==2
def test_missing_table():
 assert build_knowledge_embedding_staleness_report_from_db(sqlite3.connect(":memory:"))["missing_tables"]==["knowledge|knowledge_sources"]
