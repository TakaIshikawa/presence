from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.pipeline_candidate_refinement_loop_risk import build_pipeline_candidate_refinement_loop_risk_report_from_db, format_pipeline_candidate_refinement_loop_risk_json, format_pipeline_candidate_refinement_loop_risk_text
SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "pipeline_candidate_refinement_loop_risk.py"; spec = importlib.util.spec_from_file_location("script_pipeline_candidate_refinement_loop_risk", SCRIPT); script = importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c = sqlite3.connect(":memory:"); c.row_factory = sqlite3.Row
    c.executescript("CREATE TABLE candidate_evaluations (id TEXT, candidate_id TEXT, run_id TEXT, score REAL, status TEXT, created_at TEXT); CREATE TABLE pipeline_gate_results (id TEXT, candidate_id TEXT, run_id TEXT, gate_result TEXT, created_at TEXT);")
    for i, score in enumerate([0.6, 0.5, 0.5], 1): c.execute("INSERT INTO candidate_evaluations VALUES (?,?,?,?,?,?)", (f"e{i}", "c1", "r1", score, "evaluated", f"2026-05-0{i}"))
    c.execute("INSERT INTO pipeline_gate_results VALUES (?,?,?,?,?)", ("g1", "c1", "r1", "failed", "2026-05-04")); c.execute("INSERT INTO pipeline_gate_results VALUES (?,?,?,?,?)", ("g2", "c1", "r1", "failed", "2026-05-05"))
    c.commit(); return c
def test_pipeline_loop_report_and_cli(tmp_path, capsys):
    r = build_pipeline_candidate_refinement_loop_risk_report_from_db(_db())
    reasons = {f["loop_reason"] for f in r["refinement_loop_risks"]}
    assert r["artifact_type"] == "pipeline_candidate_refinement_loop_risk"
    assert {"repeated_refinement_attempts", "non_improving_scores", "repeated_final_gate_failures"} <= reasons
    assert r["refinement_loop_risks"][0]["attempt_count"] >= 3
    assert json.loads(format_pipeline_candidate_refinement_loop_risk_json(r))["artifact_type"] == r["artifact_type"]
    assert "Pipeline Candidate" in format_pipeline_candidate_refinement_loop_risk_text(r)
    db = tmp_path / "db.sqlite"; out = sqlite3.connect(db); _db().backup(out); out.close()
    assert script.main(["--db", str(db), "--format", "text"]) == 0 and capsys.readouterr().out
    assert script.main(["--db", str(db), "--limit", "0"]) == 2
def test_missing_schema():
    assert build_pipeline_candidate_refinement_loop_risk_report_from_db(sqlite3.connect(":memory:"))["missing_tables"]
