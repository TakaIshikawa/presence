from __future__ import annotations
import importlib.util,json,sqlite3
from pathlib import Path
from evaluation.reply_draft_source_license_exposure import build_reply_draft_source_license_exposure_report_from_db,format_reply_draft_source_license_exposure_json
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"reply_draft_source_license_exposure.py"; spec=importlib.util.spec_from_file_location("reply_draft_source_license_exposure_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_license_exposure_and_cli(tmp_path):
 c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.executescript("CREATE TABLE reply_queue (id TEXT,status TEXT,author TEXT); CREATE TABLE reply_knowledge_links (reply_queue_id TEXT,knowledge_id TEXT); CREATE TABLE knowledge (id TEXT,url TEXT,license TEXT);")
 c.executemany("INSERT INTO reply_queue VALUES (?,?,?)",[("r1","draft","ann"),("r2","posted","bob")]); c.executemany("INSERT INTO reply_knowledge_links VALUES (?,?)",[("r1","k1"),("r2","k2")]); c.executemany("INSERT INTO knowledge VALUES (?,?,?)",[("k1","https://x","NonCommercial"),("k2","https://y",None)])
 r=build_reply_draft_source_license_exposure_report_from_db(c)
 assert r["findings"][0]["reason"]=="noncommercial"; assert json.loads(format_reply_draft_source_license_exposure_json(r))["artifact_type"]=="reply_draft_source_license_exposure"
 assert build_reply_draft_source_license_exposure_report_from_db(c,include_posted=True)["totals"]["finding_count"]==2
 p=tmp_path/"db.sqlite"; c.commit()
 with sqlite3.connect(p) as out: c.backup(out)
 assert script.main(["--db",str(p),"--include-posted"])==0; assert script.main(["--db",str(p),"--limit","0"])==2
def test_missing_tables():
 assert "reply_queue" in build_reply_draft_source_license_exposure_report_from_db(sqlite3.connect(":memory:"))["missing_tables"]
