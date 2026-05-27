from __future__ import annotations
import importlib.util,json,sqlite3
from pathlib import Path
from evaluation.model_usage_stage_budget_burn import build_model_usage_stage_budget_burn_report_from_db
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"model_usage_stage_budget_burn.py"; spec=importlib.util.spec_from_file_location("model_usage_stage_budget_burn_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
 c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.execute("CREATE TABLE model_usage (id TEXT, stage TEXT, model TEXT, cost_usd REAL, total_tokens INTEGER, created_at TEXT)")
 c.executemany("INSERT INTO model_usage VALUES (?,?,?,?,?,?)",[("r1","draft","m",7,800,"2026-05-26T00:00:00+00:00"),("r2","draft","m",6,500,"2026-05-26T00:00:00+00:00"),("r3","eval","m",1,2000,"2026-05-26T00:00:00+00:00")]); c.commit(); return c
def test_aggregates_flags_and_filters(tmp_path,capsys):
 r=build_model_usage_stage_budget_burn_report_from_db(_db(),budget_usd=10,token_budget=1000)
 assert r["stages"][0]["stage"]=="draft" and r["stages"][0]["over_budget"]; assert r["summary"]["over_budget_count"]==2
 assert build_model_usage_stage_budget_burn_report_from_db(_db(),stage="eval")["summary"]["stage_count"]==1
 db=tmp_path/"db.sqlite"; out=sqlite3.connect(db); _db().backup(out); out.close()
 assert script.main(["--db",str(db),"--budget-usd","10","--token-budget","1000","--format","json"])==0; assert json.loads(capsys.readouterr().out)["artifact_type"]=="model_usage_stage_budget_burn"
