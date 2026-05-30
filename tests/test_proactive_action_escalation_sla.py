from __future__ import annotations
import json, sqlite3, importlib.util
from pathlib import Path
from evaluation.proactive_action_escalation_sla import build_proactive_action_escalation_sla_report, build_proactive_action_escalation_sla_report_from_db, format_proactive_action_escalation_sla_json
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "proactive_action_escalation_sla.py"
spec = importlib.util.spec_from_file_location("proactive_action_escalation_sla_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def test_builder_findings_and_empty_state():
    report=build_proactive_action_escalation_sla_report([{'id':1,'action_type':'reply','source':'mention','status':'open','owner':'','created_at':'2026-05-20T00:00:00+00:00'}], now='2026-05-24T00:00:00+00:00')
    payload=json.loads(format_proactive_action_escalation_sla_json(report))
    assert payload["artifact_type"] == "proactive_action_escalation_sla"
    assert payload["totals"]["finding_count"] >= 1
    empty=build_proactive_action_escalation_sla_report([], now='2026-05-24T00:00:00+00:00')
    assert empty["empty_state"]["is_empty"] is True

def test_from_db_missing_table_and_cli_validation():
    conn=sqlite3.connect(":memory:")
    report=build_proactive_action_escalation_sla_report_from_db(conn)
    assert report["missing_tables"] or report["empty_state"]["is_empty"]
    assert script.main(["--limit","0"]) == 2
