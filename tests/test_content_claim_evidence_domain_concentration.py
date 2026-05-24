from __future__ import annotations
import importlib.util,json,sqlite3
from pathlib import Path
from evaluation import content_claim_evidence_domain_concentration as mod
SCRIPT=Path(__file__).resolve().parent.parent/'scripts'/'content_claim_evidence_domain_concentration.py'
spec=importlib.util.spec_from_file_location('content_claim_evidence_domain_concentration_script',SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def test_report_and_cli(tmp_path,capsys):
 rows=[{'content_id':1,'evidence_url':'https://a.com/x'},{'content_id':1,'evidence_url':'https://a.com/y'},{'content_id':1,'evidence_url':'https://b.com/z'}]
 r=mod.build_content_claim_evidence_domain_concentration_report(rows,min_claims=2,max_domain_share=.5); assert r['findings'][0]['top_domain']=='a.com'
 db=tmp_path/'x.sqlite'; c=sqlite3.connect(db); c.executescript('CREATE TABLE content_claim_checks (content_id INTEGER,evidence_url TEXT);'); c.executemany('INSERT INTO content_claim_checks VALUES (:content_id,:evidence_url)',rows); c.commit(); c.close()
 assert script.main(['--db',str(db),'--format','json','--max-domain-share','.5'])==0; assert json.loads(capsys.readouterr().out)['artifact_type']==mod.ARTIFACT_TYPE
 assert script.main(['--db',str(db),'--limit','0'])==2
def test_missing(): assert mod.build_content_claim_evidence_domain_concentration_report_from_db(sqlite3.connect(':memory:'))['missing_tables']==['content_claim_checks']

