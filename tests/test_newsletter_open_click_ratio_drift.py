from __future__ import annotations
import json, sqlite3, importlib.util
from pathlib import Path
from evaluation.newsletter_open_click_ratio_drift import build_newsletter_open_click_ratio_drift_report, build_newsletter_open_click_ratio_drift_report_from_db, format_newsletter_open_click_ratio_drift_json
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_open_click_ratio_drift.py"
spec = importlib.util.spec_from_file_location("newsletter_open_click_ratio_drift_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def test_builder_findings_and_empty_state():
    report=build_newsletter_open_click_ratio_drift_report([{'id':1,'send_id':'s1','issue_id':'i1','open_rate':0.5,'click_rate':0.2,'fetched_at':'2026-05-01T00:00:00+00:00'},{'id':2,'send_id':'s1','issue_id':'i1','open_rate':0.5,'click_rate':0.01,'fetched_at':'2026-05-02T00:00:00+00:00'}], min_ratio=0.1, drop_threshold=0.2)
    payload=json.loads(format_newsletter_open_click_ratio_drift_json(report))
    assert payload["artifact_type"] == "newsletter_open_click_ratio_drift"
    assert payload["totals"]["finding_count"] >= 1
    empty=build_newsletter_open_click_ratio_drift_report([], )
    assert empty["empty_state"]["is_empty"] is True

def test_from_db_missing_table_and_cli_validation():
    conn=sqlite3.connect(":memory:")
    report=build_newsletter_open_click_ratio_drift_report_from_db(conn)
    assert report["missing_tables"] or report["empty_state"]["is_empty"]
    assert script.main(["--limit","0"]) == 2
