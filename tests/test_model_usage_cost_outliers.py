from __future__ import annotations
import importlib.util,sqlite3
from datetime import datetime,timezone
from pathlib import Path
from evaluation.model_usage_cost_outliers import build_model_usage_cost_outliers_report
NOW=datetime(2026,6,1,tzinfo=timezone.utc)
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"model_usage_cost_outliers.py"; spec=importlib.util.spec_from_file_location("script_model_usage_cost_outliers",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_flags_usage_and_sorts_by_cost():
 rows=[{"id":"a","estimated_cost":2,"model":"m","operation":"op"},{"id":"b","estimated_cost":5,"model":"m","operation":"op"}]
 assert [i["usage_id"] for i in build_model_usage_cost_outliers_report(rows,min_cost_usd=1,now=NOW)["outliers"]]==["b","a"]
def test_groups_runs():
 rows=[{"id":"a","run_id":"r1","estimated_cost":.6,"total_tokens":10},{"id":"b","run_id":"r1","estimated_cost":.6,"total_tokens":20}]
 r=build_model_usage_cost_outliers_report(rows,min_cost_usd=1,group_by="run",now=NOW)
 assert r["outliers"][0]["run_id"]=="r1" and r["outliers"][0]["tokens"]==30
def test_cli(tmp_path,capsys):
 db=tmp_path/"db.sqlite"; c=sqlite3.connect(db); c.execute("CREATE TABLE model_usage (id TEXT, run_id TEXT, model_name TEXT, operation_name TEXT, total_tokens INTEGER, estimated_cost REAL)"); c.execute("INSERT INTO model_usage VALUES (?,?,?,?,?,?)",("u1","r1","gpt","op",100,2.5)); c.commit(); c.close()
 assert script.main(["--db",str(db),"--format","text","--min-cost-usd","1"])==0; assert capsys.readouterr().out
