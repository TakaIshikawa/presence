from __future__ import annotations
import importlib.util,json,sqlite3
from pathlib import Path
from evaluation.publication_attempt_redirect_chain_risk import build_publication_attempt_redirect_chain_risk_report_from_db,format_publication_attempt_redirect_chain_risk_json
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"publication_attempt_redirect_chain_risk.py"; spec=importlib.util.spec_from_file_location("publication_attempt_redirect_chain_risk_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_redirect_risk_and_cli(tmp_path):
 c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.executescript("CREATE TABLE publication_attempts (id INTEGER,platform TEXT,content_id TEXT,url TEXT,status TEXT); CREATE TABLE url_check_results (attempt_id INTEGER,final_url TEXT,redirect_hops INTEGER,checked_at TEXT);")
 c.execute("INSERT INTO publication_attempts VALUES (1,'x','c1','https://bit.ly/a','success')"); c.execute("INSERT INTO url_check_results VALUES (1,'http://other.com/a',5,'2026-01-01')")
 r=build_publication_attempt_redirect_chain_risk_report_from_db(c,max_hops=3)
 assert {"too_many_hops","final_domain_mismatch","http_downgrade","shortened_link_chain"} <= set(r["findings"][0]["reasons"]); assert json.loads(format_publication_attempt_redirect_chain_risk_json(r))["artifact_type"]=="publication_attempt_redirect_chain_risk"
 p=tmp_path/"db.sqlite"; c.commit()
 with sqlite3.connect(p) as out: c.backup(out)
 assert script.main(["--db",str(p),"--format","text"])==0; assert script.main(["--db",str(p),"--max-hops","0"])==2
def test_missing_table():
 assert build_publication_attempt_redirect_chain_risk_report_from_db(sqlite3.connect(":memory:"))["missing_tables"]==["publication_attempts"]
