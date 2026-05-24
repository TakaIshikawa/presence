from __future__ import annotations
import importlib.util,json,sqlite3
from datetime import datetime,timezone
from pathlib import Path
from evaluation.pipeline_run_artifact_retention_gaps import build_pipeline_run_artifact_retention_gaps_report_from_db,format_pipeline_run_artifact_retention_gaps_json
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"pipeline_run_artifact_retention_gaps.py"; spec=importlib.util.spec_from_file_location("pipeline_run_artifact_retention_gaps_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_retention_gaps_and_cli(tmp_path):
 c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.executescript("CREATE TABLE pipeline_runs (id TEXT,stage TEXT,created_at TEXT); CREATE TABLE pipeline_artifacts (run_id TEXT,stage TEXT,path TEXT,expires_at TEXT,size_bytes INTEGER);")
 c.executemany("INSERT INTO pipeline_runs VALUES (?,?,?)",[("r1","build","2026-01-01T00:00:00+00:00"),("r2","publish","2026-01-01T00:00:00+00:00")])
 c.execute("INSERT INTO pipeline_artifacts VALUES ('r1','build','', '2026-01-02T00:00:00+00:00', 200)")
 r=build_pipeline_run_artifact_retention_gaps_report_from_db(c,max_size_bytes=100,now=datetime(2026,1,3,tzinfo=timezone.utc))
 assert {"missing_artifact","expired_artifact","oversized_artifact","unlinked_artifact"} <= {f["issue_type"] for f in r["findings"]}; assert json.loads(format_pipeline_run_artifact_retention_gaps_json(r))["artifact_type"]=="pipeline_run_artifact_retention_gaps"
 p=tmp_path/"db.sqlite"; c.commit()
 with sqlite3.connect(p) as out: c.backup(out)
 assert script.main(["--db",str(p),"--max-size-bytes","100"])==0; assert script.main(["--db",str(p),"--limit","0"])==2
def test_missing_table():
 assert build_pipeline_run_artifact_retention_gaps_report_from_db(sqlite3.connect(":memory:"))["missing_tables"]==["pipeline_runs"]
