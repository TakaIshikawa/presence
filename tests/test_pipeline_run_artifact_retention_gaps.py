from __future__ import annotations
import importlib.util,json,sqlite3
from pathlib import Path
from evaluation import pipeline_run_artifact_retention_gaps as mod
SCRIPT=Path(__file__).resolve().parent.parent/'scripts'/'pipeline_run_artifact_retention_gaps.py'
spec=importlib.util.spec_from_file_location('pipeline_run_artifact_retention_gaps_script',SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def test_report_and_cli(tmp_path,capsys):
 r=mod.build_pipeline_run_artifact_retention_gaps_report([{'id':1,'stage':'build'}],[{'id':2,'run_id':1,'path':'','size_bytes':5}],max_size_bytes=10)
 assert r['findings'][0]['reason']=='unlinked'
 db=tmp_path/'x.sqlite'; c=sqlite3.connect(db); c.executescript('CREATE TABLE pipeline_runs (id INTEGER,stage TEXT); CREATE TABLE pipeline_artifacts (id INTEGER,run_id INTEGER,path TEXT,size_bytes INTEGER); INSERT INTO pipeline_runs VALUES (1,"build"); INSERT INTO pipeline_artifacts VALUES (2,1,"",5);'); c.close()
 assert script.main(['--db',str(db),'--format','json'])==0; assert json.loads(capsys.readouterr().out)['artifact_type']==mod.ARTIFACT_TYPE
 assert script.main(['--db',str(db),'--max-size-bytes','0'])==2
def test_missing(): assert mod.build_pipeline_run_artifact_retention_gaps_report_from_db(sqlite3.connect(':memory:'))['missing_tables']==['pipeline_runs']

