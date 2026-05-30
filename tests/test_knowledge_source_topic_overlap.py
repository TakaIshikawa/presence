from __future__ import annotations
import importlib.util,json,sqlite3
from pathlib import Path
from evaluation.knowledge_source_topic_overlap import build_knowledge_source_topic_overlap_report_from_db
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"knowledge_source_topic_overlap.py"; spec=importlib.util.spec_from_file_location("knowledge_source_topic_overlap_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
 c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.execute("CREATE TABLE knowledge_sources (id TEXT, url TEXT, tags TEXT, metadata TEXT, updated_at TEXT)"); c.executemany("INSERT INTO knowledge_sources VALUES (?,?,?,?,?)",[("1","https://a","ai,ops",None,"2026-05-26T00:00:00+00:00"),("2","https://b","ai",None,"2026-05-25T00:00:00+00:00"),("3","https://c","ops",None,"2026-05-24T00:00:00+00:00")]); c.commit(); return c
def test_topic_overlap_and_cli(tmp_path,capsys):
 r=build_knowledge_source_topic_overlap_report_from_db(_db(),min_sources=2)
 assert {f["topic"] for f in r["findings"]}=={"ai","ops"}; assert r["findings"][0]["source_count"]==2
 assert build_knowledge_source_topic_overlap_report_from_db(_db(),topic="ai")["summary"]["topic_count"]==1
 db=tmp_path/"db.sqlite"; out=sqlite3.connect(db); _db().backup(out); out.close(); assert script.main(["--db",str(db),"--min-sources","2","--format","json"])==0; assert json.loads(capsys.readouterr().out)["artifact_type"]=="knowledge_source_topic_overlap"
