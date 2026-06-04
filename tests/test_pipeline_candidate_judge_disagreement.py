from __future__ import annotations
import importlib.util,sqlite3
from datetime import datetime,timezone
from pathlib import Path
from evaluation.pipeline_candidate_judge_disagreement import build_pipeline_candidate_judge_disagreement_report
NOW=datetime(2026,6,1,tzinfo=timezone.utc)
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"pipeline_candidate_judge_disagreement.py"; spec=importlib.util.spec_from_file_location("script_pipeline_candidate_judge_disagreement",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_score_delta_and_label_conflict_grouped():
 rows=[{"run_id":"r","candidate_id":"c","evaluator":"a","score":.9,"label":"pass"},{"run_id":"r","candidate_id":"c","evaluator":"b","score":.4,"label":"fail"}]
 r=build_pipeline_candidate_judge_disagreement_report(rows,min_score_delta=.2,now=NOW)
 assert len(r["disagreements"])==1 and r["disagreements"][0]["score_delta"]==.5
def test_evaluator_filter():
 rows=[{"run_id":"r","candidate_id":"c","evaluator":"a","score":.9},{"run_id":"r","candidate_id":"c","evaluator":"b","score":.1}]
 assert build_pipeline_candidate_judge_disagreement_report(rows,evaluators=["a"],now=NOW)["disagreements"]==[]
def test_cli(tmp_path,capsys):
 db=tmp_path/"db.sqlite"; c=sqlite3.connect(db); c.execute("CREATE TABLE pipeline_candidate_evaluations (run_id TEXT,candidate_id TEXT,evaluator TEXT,score REAL,label TEXT)"); c.executemany("INSERT INTO pipeline_candidate_evaluations VALUES (?,?,?,?,?)",[("r","c","a",.9,"pass"),("r","c","b",.1,"fail")]); c.commit(); c.close()
 assert script.main(["--db",str(db),"--format","text"])==0; assert capsys.readouterr().out
