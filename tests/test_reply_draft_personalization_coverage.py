from __future__ import annotations
import json, sqlite3, importlib.util
from pathlib import Path
from evaluation.reply_draft_personalization_coverage import build_reply_draft_personalization_coverage_report, build_reply_draft_personalization_coverage_report_from_db, format_reply_draft_personalization_coverage_json
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_draft_personalization_coverage.py"
spec = importlib.util.spec_from_file_location("reply_draft_personalization_coverage_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def test_builder_findings_and_empty_state():
    report=build_reply_draft_personalization_coverage_report([{'id':1,'draft_text':'Thanks','target_handle':'alice','target_name':'Alice','conversation_context':'topic','relationship_context':'','knowledge_source_ids':'','status':'draft'}], )
    payload=json.loads(format_reply_draft_personalization_coverage_json(report))
    assert payload["artifact_type"] == "reply_draft_personalization_coverage"
    assert payload["totals"]["finding_count"] >= 1
    empty=build_reply_draft_personalization_coverage_report([], )
    assert empty["empty_state"]["is_empty"] is True

def test_from_db_missing_table_and_cli_validation():
    conn=sqlite3.connect(":memory:")
    report=build_reply_draft_personalization_coverage_report_from_db(conn)
    assert report["missing_tables"] or report["empty_state"]["is_empty"]
    assert script.main(["--limit","0"]) == 2
