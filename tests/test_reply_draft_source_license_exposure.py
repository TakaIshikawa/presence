from __future__ import annotations
import importlib.util,json,sqlite3
from pathlib import Path
from evaluation import reply_draft_source_license_exposure as mod
SCRIPT=Path(__file__).resolve().parent.parent/'scripts'/'reply_draft_source_license_exposure.py'
spec=importlib.util.spec_from_file_location('reply_draft_source_license_exposure_script',SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def test_report_and_cli(tmp_path,capsys):
 rows=[{'reply_queue_id':1,'status':'draft','author':'me','source_url':'https://x','license':'noncommercial'}]
 assert mod.build_reply_draft_source_license_exposure_report(rows)['findings'][0]['reason']=='restricted_license'
 db=tmp_path/'x.sqlite'; c=sqlite3.connect(db); c.executescript('CREATE TABLE reply_queue (id INTEGER,status TEXT,author TEXT); CREATE TABLE reply_knowledge_links (reply_queue_id INTEGER,knowledge_id INTEGER); CREATE TABLE knowledge (id INTEGER,source_url TEXT,license TEXT); INSERT INTO reply_queue VALUES (1,"draft","me"); INSERT INTO reply_knowledge_links VALUES (1,2); INSERT INTO knowledge VALUES (2,"https://x","private");'); c.close()
 assert script.main(['--db',str(db),'--format','text'])==0; assert 'License Exposure' in capsys.readouterr().out
 assert script.main(['--db',str(db),'--limit','0'])==2
def test_missing(): assert mod.build_reply_draft_source_license_exposure_report_from_db(sqlite3.connect(':memory:'))['missing_tables']==['knowledge','reply_knowledge_links','reply_queue']

