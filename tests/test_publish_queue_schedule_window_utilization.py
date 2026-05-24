from __future__ import annotations
import importlib.util,json,sqlite3
from pathlib import Path
from evaluation import publish_queue_schedule_window_utilization as mod
SCRIPT=Path(__file__).resolve().parent.parent/'scripts'/'publish_queue_schedule_window_utilization.py'
spec=importlib.util.spec_from_file_location('publish_queue_schedule_window_utilization_script',SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def test_timezone_missing_empty_text_json_and_cli(tmp_path,capsys):
 q=[{'id':1,'platform':'x','scheduled_at':'2026-05-25T00:30:00Z'},{'id':2,'platform':'x','scheduled_at':'2026-05-25T01:00:00Z'},{'id':3,'platform':'x','scheduled_at':'2026-05-25T08:00:00Z'}]
 w=[{'id':'am','platform':'x','day_of_week':'mon','start_time':'09:00','end_time':'11:00','capacity':1}]
 r=mod.build_publish_queue_schedule_window_utilization_report(q,w,timezone_name='Asia/Tokyo',underused_threshold=.1,overfilled_threshold=1)
 assert any(f.get('type')=='overfilled' for f in r['findings']) and any(f.get('reason')=='outside_window' for f in r['findings'])
 assert json.loads(mod.format_publish_queue_schedule_window_utilization_json(r))['artifact_type']==mod.ARTIFACT_TYPE
 assert 'Publish Queue Schedule Window' in mod.format_publish_queue_schedule_window_utilization_text(r)
 db=tmp_path/'x.sqlite'; c=sqlite3.connect(db); c.executescript('CREATE TABLE publish_queue (id INTEGER,platform TEXT,scheduled_at TEXT); CREATE TABLE posting_windows (id TEXT,platform TEXT,day_of_week TEXT,start_time TEXT,end_time TEXT,capacity INTEGER);'); c.executemany('INSERT INTO publish_queue VALUES (:id,:platform,:scheduled_at)',q); c.execute('INSERT INTO posting_windows VALUES ("am","x","mon","09:00","11:00",1)'); c.commit(); c.close()
 assert script.main(['--db',str(db),'--format','text','--timezone','Asia/Tokyo'])==0; assert 'Asia/Tokyo' in capsys.readouterr().out
 assert script.main(['--db',str(db),'--underused-threshold','-1'])==1
def test_missing_window_metadata_and_empty():
 miss=mod.build_publish_queue_schedule_window_utilization_report_from_db(sqlite3.connect(':memory:'))
 assert miss['missing_tables']==['publish_queue']
 r=mod.build_publish_queue_schedule_window_utilization_report([],[]); assert r['empty_state']['is_empty']

