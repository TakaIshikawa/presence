from __future__ import annotations
import json, sqlite3, importlib.util
from pathlib import Path
from evaluation.knowledge_citation_canonicalization import build_knowledge_citation_canonicalization_report, build_knowledge_citation_canonicalization_report_from_db, format_knowledge_citation_canonicalization_json
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "knowledge_citation_canonicalization.py"
spec = importlib.util.spec_from_file_location("knowledge_citation_canonicalization_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def test_builder_findings_and_empty_state():
    report=build_knowledge_citation_canonicalization_report([{'id':1,'url':'http://example.com/a?utm_source=x'},{'id':2,'url':'https://example.com/a/'}], )
    payload=json.loads(format_knowledge_citation_canonicalization_json(report))
    assert payload["artifact_type"] == "knowledge_citation_canonicalization"
    assert payload["totals"]["finding_count"] >= 1
    empty=build_knowledge_citation_canonicalization_report([], )
    assert empty["empty_state"]["is_empty"] is True

def test_from_db_missing_table_and_cli_validation():
    conn=sqlite3.connect(":memory:")
    report=build_knowledge_citation_canonicalization_report_from_db(conn)
    assert report["missing_tables"] or report["empty_state"]["is_empty"]
    assert script.main(["--limit","0"]) == 2
