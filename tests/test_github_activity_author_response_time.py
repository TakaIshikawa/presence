from __future__ import annotations
import importlib.util,json,sqlite3
from pathlib import Path
from evaluation import github_activity_author_response_time as mod
SCRIPT=Path(__file__).resolve().parent.parent/'scripts'/'github_activity_author_response_time.py'
spec=importlib.util.spec_from_file_location('github_activity_author_response_time_script',SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def test_report_and_cli(tmp_path,capsys):
 acts=[{'id':1,'author':'octo','repository':'r','occurred_at':'2026-05-01T00:00:00Z'}]; resp=[{'activity_id':1,'responded_at':'2026-05-03T00:00:00Z'}]
 r=mod.build_github_activity_author_response_time_report(acts,resp,sla_hours=24,now='2026-05-04T00:00:00Z'); assert r['findings'][0]['reason']=='sla_breach'
 db=tmp_path/'x.sqlite'; c=sqlite3.connect(db); c.executescript('CREATE TABLE github_activity (id INTEGER,author TEXT,repository TEXT,occurred_at TEXT); CREATE TABLE reply_queue (activity_id INTEGER,created_at TEXT); INSERT INTO github_activity VALUES (1,"octo","r","2026-05-01T00:00:00Z"); INSERT INTO reply_queue VALUES (1,"2026-05-03T00:00:00Z");'); c.close()
 assert script.main(['--db',str(db),'--format','json'])==0; assert json.loads(capsys.readouterr().out)['artifact_type']==mod.ARTIFACT_TYPE
 assert script.main(['--db',str(db),'--sla-hours','0'])==2
def test_missing(): assert mod.build_github_activity_author_response_time_report_from_db(sqlite3.connect(':memory:'))['missing_tables']==['github_activity']

