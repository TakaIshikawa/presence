from __future__ import annotations
import json, sqlite3, importlib.util
from pathlib import Path
from evaluation.prompt_version_deployment_lag import build_prompt_version_deployment_lag_report, build_prompt_version_deployment_lag_report_from_db, format_prompt_version_deployment_lag_json
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "prompt_version_deployment_lag.py"
spec = importlib.util.spec_from_file_location("prompt_version_deployment_lag_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def test_builder_findings_and_empty_state():
    report=build_prompt_version_deployment_lag_report([{'id':1,'name':'p','version':'1','approved_at':'2026-05-20T00:00:00+00:00'}], usage_rows=[], now='2026-05-24T00:00:00+00:00')
    payload=json.loads(format_prompt_version_deployment_lag_json(report))
    assert payload["artifact_type"] == "prompt_version_deployment_lag"
    assert payload["totals"]["finding_count"] >= 1
    empty=build_prompt_version_deployment_lag_report([], usage_rows=[], now='2026-05-24T00:00:00+00:00')
    assert empty["empty_state"]["is_empty"] is True

def test_from_db_missing_table_and_cli_validation():
    conn=sqlite3.connect(":memory:")
    report=build_prompt_version_deployment_lag_report_from_db(conn)
    assert report["missing_tables"] or report["empty_state"]["is_empty"]
    assert script.main(["--limit","0"]) == 2
