from __future__ import annotations
import json, sqlite3, importlib.util
from pathlib import Path
from evaluation.publication_attempt_success_url_gaps import build_publication_attempt_success_url_gaps_report, build_publication_attempt_success_url_gaps_report_from_db, format_publication_attempt_success_url_gaps_json
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_attempt_success_url_gaps.py"
spec = importlib.util.spec_from_file_location("publication_attempt_success_url_gaps_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def test_builder_findings_and_empty_state():
    report=build_publication_attempt_success_url_gaps_report([{'id':1,'content_id':2,'platform':'x','status':'success','published_url':'','raw_response':'ok','attempted_at':'2026-05-20T00:00:00+00:00'}], now='2026-05-24T00:00:00+00:00')
    payload=json.loads(format_publication_attempt_success_url_gaps_json(report))
    assert payload["artifact_type"] == "publication_attempt_success_url_gaps"
    assert payload["totals"]["finding_count"] >= 1
    empty=build_publication_attempt_success_url_gaps_report([], now='2026-05-24T00:00:00+00:00')
    assert empty["empty_state"]["is_empty"] is True

def test_from_db_missing_table_and_cli_validation():
    conn=sqlite3.connect(":memory:")
    report=build_publication_attempt_success_url_gaps_report_from_db(conn)
    assert report["missing_tables"] or report["empty_state"]["is_empty"]
    assert script.main(["--limit","0"]) == 2
