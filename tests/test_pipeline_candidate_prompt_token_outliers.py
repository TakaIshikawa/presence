from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.pipeline_candidate_prompt_token_outliers import build_pipeline_candidate_prompt_token_outliers_report_from_db, format_pipeline_candidate_prompt_token_outliers_json, format_pipeline_candidate_prompt_token_outliers_text
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"pipeline_candidate_prompt_token_outliers.py"; spec=importlib.util.spec_from_file_location("script_pipeline_candidate_prompt_token_outliers",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.executescript("CREATE TABLE model_usage (run_id TEXT, candidate_id TEXT, model TEXT, input_tokens INTEGER, output_tokens INTEGER, stage TEXT);")
    for i,t in enumerate([100,110,120,800]): c.execute("INSERT INTO model_usage VALUES (?,?,?,?,?,?)",(f"r{i}",f"cand{i}","gpt",t,10,"synthesis"))
    c.commit(); return c
def test_report_db_formatters_and_cli(tmp_path,capsys):
    r=build_pipeline_candidate_prompt_token_outliers_report_from_db(_db(),multiplier=2); assert r["artifact_type"]=="pipeline_candidate_prompt_token_outliers"; assert r["findings"]; assert r["findings"][0]["candidate_id"]=="cand3"
    assert json.loads(format_pipeline_candidate_prompt_token_outliers_json(r))["artifact_type"]=="pipeline_candidate_prompt_token_outliers"; assert "Pipeline" in format_pipeline_candidate_prompt_token_outliers_text(r)
    db=tmp_path/"db.sqlite"; out=sqlite3.connect(db); _db().backup(out); out.close(); assert script.main(["--db",str(db),"--format","text","--multiplier","2"])==0; assert capsys.readouterr().out; assert script.main(["--db",str(db),"--limit","0"])==2
def test_missing_schema():
    assert build_pipeline_candidate_prompt_token_outliers_report_from_db(sqlite3.connect(":memory:"))["missing_tables"]
