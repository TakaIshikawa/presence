from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.pipeline_candidate_evaluator_disagreement import build_pipeline_candidate_evaluator_disagreement_report_from_db
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"pipeline_candidate_evaluator_disagreement.py"; spec=importlib.util.spec_from_file_location("pipeline_candidate_evaluator_disagreement_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.execute("CREATE TABLE pipeline_candidate_evaluations (id TEXT, content_type TEXT, artifact_json TEXT, created_at TEXT)")
    c.execute("INSERT INTO pipeline_candidate_evaluations VALUES (?,?,?,?)",("r1","blog",'{"selected_candidate_id":"b","evaluations":[{"evaluator":"a","score":0.9,"gate":"pass"},{"evaluator":"b","score":0.2,"gate":"fail"}],"candidates":[{"id":"b","rank":2}]}',"2026-05-01")); c.commit(); return c
def test_disagreement_cli(tmp_path,capsys):
    assert build_pipeline_candidate_evaluator_disagreement_report_from_db(_db(),min_score_spread=.3)["findings"][0]["reasons"]
    db=tmp_path/"db.sqlite"; out=sqlite3.connect(db); _db().backup(out); out.close()
    assert script.main(["--db",str(db),"--format","json","--min-score-spread",".3"])==0
    assert json.loads(capsys.readouterr().out)["findings"][0]["run_id"]=="r1"
