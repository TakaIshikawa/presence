from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.pipeline_run_manual_rerun_effectiveness import build_pipeline_run_manual_rerun_effectiveness_report_from_db, format_pipeline_run_manual_rerun_effectiveness_json, format_pipeline_run_manual_rerun_effectiveness_text
SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "pipeline_run_manual_rerun_effectiveness.py"
spec = importlib.util.spec_from_file_location("pipeline_run_manual_rerun_effectiveness_script", SCRIPT); script = importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def _db():
    c = sqlite3.connect(":memory:"); c.row_factory = sqlite3.Row
    c.executescript("CREATE TABLE pipeline_runs (id TEXT, parent_run_id TEXT, stage TEXT, status TEXT, initiated_by TEXT, created_at TEXT, finished_at TEXT);")
    return c

def test_rerun_outcomes_and_cli(tmp_path, capsys):
    c = _db()
    rows = [("1",None,"build","failed","ci","2026-05-01T00:00:00+00:00","2026-05-01T00:10:00+00:00"),("2","1","build","success","alice","2026-05-01T00:20:00+00:00","2026-05-01T00:30:00+00:00"),("3","1","build","failed","bob","2026-05-01T00:40:00+00:00","2026-05-01T00:45:00+00:00"),("4","1","build","running","alice","2026-05-01T00:50:00+00:00",None)]
    c.executemany("INSERT INTO pipeline_runs VALUES (?,?,?,?,?,?,?)", rows)
    report = build_pipeline_run_manual_rerun_effectiveness_report_from_db(c)
    assert report["summary"]["by_outcome"] == {"repeated_failure": 1, "resolved": 1, "still_running": 1}
    assert json.loads(format_pipeline_run_manual_rerun_effectiveness_json(report))["artifact_type"] == "pipeline_run_manual_rerun_effectiveness"
    assert "Pipeline Run Manual" in format_pipeline_run_manual_rerun_effectiveness_text(report)
    c.commit(); path = tmp_path / "db.sqlite"
    with sqlite3.connect(path) as out: c.backup(out)
    assert script.main(["--db", str(path), "--format", "text"]) == 0
    assert "Pipeline Run Manual" in capsys.readouterr().out
    assert script.main(["--db", str(path), "--limit", "0"]) == 2

def test_missing_table():
    assert build_pipeline_run_manual_rerun_effectiveness_report_from_db(sqlite3.connect(":memory:"))["missing_tables"] == ["pipeline_runs|workflow_runs"]

