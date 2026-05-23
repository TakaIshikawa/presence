from __future__ import annotations
import json, sqlite3, importlib.util
from pathlib import Path
from evaluation.publish_queue_priority_inversion import build_publish_queue_priority_inversion_report, build_publish_queue_priority_inversion_report_from_db, format_publish_queue_priority_inversion_json
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publish_queue_priority_inversion.py"
spec = importlib.util.spec_from_file_location("publish_queue_priority_inversion_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def test_builder_findings_and_empty_state():
    report=build_publish_queue_priority_inversion_report([{'id':1,'platform':'x','campaign_id':'c','priority':10,'scheduled_at':'2026-05-20T02:00:00+00:00'},{'id':2,'platform':'x','campaign_id':'c','priority':1,'scheduled_at':'2026-05-20T01:00:00+00:00'}], now='2026-05-24T00:00:00+00:00')
    payload=json.loads(format_publish_queue_priority_inversion_json(report))
    assert payload["artifact_type"] == "publish_queue_priority_inversion"
    assert payload["totals"]["finding_count"] >= 1
    empty=build_publish_queue_priority_inversion_report([], now='2026-05-24T00:00:00+00:00')
    assert empty["empty_state"]["is_empty"] is True

def test_from_db_missing_table_and_cli_validation():
    conn=sqlite3.connect(":memory:")
    report=build_publish_queue_priority_inversion_report_from_db(conn)
    assert report["missing_tables"] or report["empty_state"]["is_empty"]
    assert script.main(["--limit","0"]) == 2
