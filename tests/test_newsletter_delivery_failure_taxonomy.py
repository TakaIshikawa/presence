from __future__ import annotations
import importlib.util,json,sqlite3
from pathlib import Path
from evaluation import newsletter_delivery_failure_taxonomy as mod
SCRIPT=Path(__file__).resolve().parent.parent/'scripts'/'newsletter_delivery_failure_taxonomy.py'
spec=importlib.util.spec_from_file_location('newsletter_delivery_failure_taxonomy_script',SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def test_report_and_cli(tmp_path,capsys):
 rows=[{'status':'bounced','error_message':'hard bounce','provider':'ses','issue_id':'i1','subscriber_id':'s1','occurred_at':'2026-05-01T00:00:00Z'},{'status':'failed','error_message':'timeout','provider':'mailgun','issue_id':'i1'}]
 r=mod.build_newsletter_delivery_failure_taxonomy_report(rows)
 assert r['totals']['by_category']['bounce']==1 and r['totals']['by_category']['timeout']==1
 assert 'provider_breakdown' in r and 'issue_breakdown' in r
 db=tmp_path/'x.sqlite'; c=sqlite3.connect(db); c.executescript('CREATE TABLE newsletter_delivery_events (status TEXT,error_message TEXT,provider TEXT,issue_id TEXT,subscriber_id TEXT,occurred_at TEXT);'); c.executemany('INSERT INTO newsletter_delivery_events VALUES (:status,:error_message,:provider,:issue_id,:subscriber_id,:occurred_at)',[{**x,'subscriber_id':x.get('subscriber_id'),'occurred_at':x.get('occurred_at')} for x in rows]); c.commit(); c.close()
 assert script.main(['--db',str(db),'--format','json'])==0; assert json.loads(capsys.readouterr().out)['artifact_type']==mod.ARTIFACT_TYPE
 assert script.main(['--db',str(db),'--limit','0'])==2
def test_missing_and_text():
 r=mod.build_newsletter_delivery_failure_taxonomy_report_from_db(sqlite3.connect(':memory:'))
 assert r['missing_tables']==['newsletter_delivery_events']; assert 'Newsletter Delivery Failure' in mod.format_newsletter_delivery_failure_taxonomy_text(r)
