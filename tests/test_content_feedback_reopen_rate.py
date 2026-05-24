from __future__ import annotations
import importlib.util,json,sqlite3
from pathlib import Path
from evaluation import content_feedback_reopen_rate as mod
SCRIPT=Path(__file__).resolve().parent.parent/'scripts'/'content_feedback_reopen_rate.py'
spec=importlib.util.spec_from_file_location('content_feedback_reopen_rate_script',SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def test_report_and_cli(tmp_path,capsys):
 rows=[{'feedback_id':1,'status':'resolved','reviewer':'ann','content_type':'blog','resolution_reason':'fixed','occurred_at':'2026-05-01T00:00:00Z'},{'feedback_id':1,'status':'reopened','reviewer':'ann','content_type':'blog','occurred_at':'2026-05-02T00:00:00Z'}]
 r=mod.build_content_feedback_reopen_rate_report(rows,now='2026-05-03T00:00:00Z'); assert r['totals']['reopen_rate']==1
 db=tmp_path/'x.sqlite'; c=sqlite3.connect(db); c.executescript('CREATE TABLE content_feedback_events (feedback_id INTEGER,status TEXT,reviewer TEXT,content_type TEXT,resolution_reason TEXT,occurred_at TEXT);'); c.executemany('INSERT INTO content_feedback_events VALUES (:feedback_id,:status,:reviewer,:content_type,:resolution_reason,:occurred_at)',[{**x,'resolution_reason':x.get('resolution_reason')} for x in rows]); c.commit(); c.close()
 assert script.main(['--db',str(db),'--format','text'])==0; assert 'Reopen Rate' in capsys.readouterr().out
 assert script.main(['--db',str(db),'--window-days','0'])==2
def test_missing(): assert mod.build_content_feedback_reopen_rate_report_from_db(sqlite3.connect(':memory:'))['missing_tables']==['content_feedback']

