from __future__ import annotations
import json, sqlite3, importlib.util
from pathlib import Path
from evaluation.model_usage_cache_savings import build_model_usage_cache_savings_report, build_model_usage_cache_savings_report_from_db, format_model_usage_cache_savings_json
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "model_usage_cache_savings.py"
spec = importlib.util.spec_from_file_location("model_usage_cache_savings_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def test_builder_findings_and_empty_state():
    report=build_model_usage_cache_savings_report([{'provider':'o','model':'m','prompt_name':'p','input_tokens':2000,'cached_input_tokens':10,'output_tokens':1,'cost_usd':1,'created_at':'2026-05-20T00:00:00+00:00'}], now='2026-05-24T00:00:00+00:00')
    payload=json.loads(format_model_usage_cache_savings_json(report))
    assert payload["artifact_type"] == "model_usage_cache_savings"
    assert payload["totals"]["finding_count"] >= 1
    empty=build_model_usage_cache_savings_report([], now='2026-05-24T00:00:00+00:00')
    assert empty["empty_state"]["is_empty"] is True

def test_from_db_missing_table_and_cli_validation():
    conn=sqlite3.connect(":memory:")
    report=build_model_usage_cache_savings_report_from_db(conn)
    assert report["missing_tables"] or report["empty_state"]["is_empty"]
    assert script.main(["--limit","0"]) == 2
