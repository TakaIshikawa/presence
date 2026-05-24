from __future__ import annotations
import importlib.util,json,sqlite3
from pathlib import Path
from evaluation.content_claim_evidence_domain_concentration import build_content_claim_evidence_domain_concentration_report_from_db,format_content_claim_evidence_domain_concentration_json
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"content_claim_evidence_domain_concentration.py"; spec=importlib.util.spec_from_file_location("content_claim_evidence_domain_concentration_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_domain_concentration_and_cli(tmp_path):
 c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.executescript("CREATE TABLE content_claim_checks (id INTEGER,content_id TEXT,evidence_url TEXT,evidence TEXT);")
 c.executemany("INSERT INTO content_claim_checks VALUES (?,?,?,?)",[(1,"c1","https://www.news.example/a",None),(2,"c1","https://news.example/b",None),(3,"c1",None,'{"url":"https://other.test/x"}'),(4,"c2",None,None)])
 r=build_content_claim_evidence_domain_concentration_report_from_db(c,min_claims=2,max_domain_share=.5)
 assert r["totals"]["missing_evidence_url_count"]==1; assert r["findings"][0]["top_domain"]=="news.example"; assert json.loads(format_content_claim_evidence_domain_concentration_json(r))["artifact_type"]=="content_claim_evidence_domain_concentration"
 p=tmp_path/"db.sqlite"; c.commit()
 with sqlite3.connect(p) as out: c.backup(out)
 assert script.main(["--db",str(p),"--min-claims","2","--max-domain-share",".5"])==0; assert script.main(["--db",str(p),"--max-domain-share","2"])==2
def test_schema_gap():
 c=sqlite3.connect(":memory:"); c.execute("CREATE TABLE content_claim_checks (id INTEGER, content_id TEXT)")
 assert build_content_claim_evidence_domain_concentration_report_from_db(c)["missing_columns"]=={"content_claim_checks":["evidence_url"]}
