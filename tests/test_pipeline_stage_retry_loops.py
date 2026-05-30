from __future__ import annotations
import json, sqlite3, importlib.util
from pathlib import Path
from evaluation.pipeline_stage_retry_loops import build_pipeline_stage_retry_loops_report, build_pipeline_stage_retry_loops_report_from_db, format_pipeline_stage_retry_loops_json
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "pipeline_stage_retry_loops.py"
spec = importlib.util.spec_from_file_location("pipeline_stage_retry_loops_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def test_builder_findings_and_empty_state():
    report=build_pipeline_stage_retry_loops_report([{'id':1,'content_id':1,'stage':'draft','status':'retry','started_at':'2026-05-20T00:00:00+00:00'},{'id':2,'content_id':1,'stage':'draft','status':'retry','started_at':'2026-05-20T01:00:00+00:00'},{'id':3,'content_id':1,'stage':'draft','status':'retry','started_at':'2026-05-20T02:00:00+00:00'}], now='2026-05-24T00:00:00+00:00')
    payload=json.loads(format_pipeline_stage_retry_loops_json(report))
    assert payload["artifact_type"] == "pipeline_stage_retry_loops"
    assert payload["totals"]["finding_count"] >= 1
    empty=build_pipeline_stage_retry_loops_report([], now='2026-05-24T00:00:00+00:00')
    assert empty["empty_state"]["is_empty"] is True

def test_from_db_missing_table_and_cli_validation():
    conn=sqlite3.connect(":memory:")
    report=build_pipeline_stage_retry_loops_report_from_db(conn)
    assert report["missing_tables"] or report["empty_state"]["is_empty"]
    assert script.main(["--limit","0"]) == 2
