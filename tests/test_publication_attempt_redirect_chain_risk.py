from __future__ import annotations
import importlib.util,json,sqlite3
from pathlib import Path
from evaluation import publication_attempt_redirect_chain_risk as mod
SCRIPT=Path(__file__).resolve().parent.parent/'scripts'/'publication_attempt_redirect_chain_risk.py'
spec=importlib.util.spec_from_file_location('publication_attempt_redirect_chain_risk_script',SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def test_report_and_cli(tmp_path,capsys):
 rows=[{'id':1,'platform':'x','content_id':2,'url':'https://example.com/a','final_url':'http://other.com/b','redirect_hops':5,'status':'success'}]
 r=mod.build_publication_attempt_redirect_chain_risk_report(rows,max_hops=3)
 assert {'too_many_hops','final_domain_mismatch','http_downgrade'} <= set(r['findings'][0]['risk_reasons'])
 db=tmp_path/'x.sqlite'; c=sqlite3.connect(db); c.executescript('CREATE TABLE publication_attempts (id INTEGER,platform TEXT,content_id INTEGER,url TEXT,final_url TEXT,redirect_hops INTEGER,status TEXT); INSERT INTO publication_attempts VALUES (1,"x",2,"https://example.com/a","http://other.com/b",5,"success");'); c.close()
 assert script.main(['--db',str(db),'--format','json'])==0; assert json.loads(capsys.readouterr().out)['artifact_type']==mod.ARTIFACT_TYPE
 assert script.main(['--db',str(db),'--max-hops','-1'])==2
def test_missing(): assert mod.build_publication_attempt_redirect_chain_risk_report_from_db(sqlite3.connect(':memory:'))['missing_tables']==['publication_attempts']

