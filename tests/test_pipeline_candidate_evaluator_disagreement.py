from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.pipeline_candidate_evaluator_disagreement import build_pipeline_candidate_evaluator_disagreement_report_from_db, format_pipeline_candidate_evaluator_disagreement_json, format_pipeline_candidate_evaluator_disagreement_text
SCRIPT=Path(__file__).resolve().parent.parent/'scripts'/'pipeline_candidate_evaluator_disagreement.py'; spec=importlib.util.spec_from_file_location('pipeline_candidate_evaluator_disagreement_script',SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c=sqlite3.connect(':memory:'); c.row_factory=sqlite3.Row; c.executescript('CREATE TABLE pipeline_candidate_evaluations (run_id TEXT, candidate_id TEXT, evaluator TEXT, score REAL, gate_decision TEXT, selected_candidate_id TEXT, rank INTEGER, content_type TEXT, created_at TEXT);')
    c.executemany('INSERT INTO pipeline_candidate_evaluations VALUES (?,?,?,?,?,?,?,?,?)',[('r1','c1','a',9,'pass','c2',2,'post','2026-05-01'),('r1','c1','b',4,'fail','c2',2,'post','2026-05-01'),('r2','c1','a',8,'pass','c1',1,'post','2026-05-01')]); c.commit(); return c
def test_score_and_gate_disagreement_cli(tmp_path,capsys):
    r=build_pipeline_candidate_evaluator_disagreement_report_from_db(_db(),min_score_spread=2)
    assert r['findings'][0]['run_id']=='r1'
    assert r['findings'][0]['score_spread']==5
    assert json.loads(format_pipeline_candidate_evaluator_disagreement_json(r))['artifact_type']=='pipeline_candidate_evaluator_disagreement'
    assert 'Pipeline Candidate' in format_pipeline_candidate_evaluator_disagreement_text(r)
    db=tmp_path/'db.sqlite'; out=sqlite3.connect(db); _db().backup(out); out.close()
    assert script.main(['--db',str(db),'--format','text','--min-score-spread','2'])==0
    assert 'r1' in capsys.readouterr().out
