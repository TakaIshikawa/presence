from __future__ import annotations
import json, sqlite3, importlib.util
from pathlib import Path
from evaluation.newsletter_segment_delivery_skew import build_newsletter_segment_delivery_skew_report, build_newsletter_segment_delivery_skew_report_from_db, format_newsletter_segment_delivery_skew_json
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_segment_delivery_skew.py"
spec = importlib.util.spec_from_file_location("newsletter_segment_delivery_skew_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def test_builder_findings_and_empty_state():
    report=build_newsletter_segment_delivery_skew_report([{'segment':'a','subscriber_count':90,'open_rate':0.5,'click_rate':0.1,'sent_at':'2026-05-20T00:00:00+00:00'},{'segment':'b','subscriber_count':10,'open_rate':0.0,'click_rate':0.0,'sent_at':'2026-05-20T00:00:00+00:00'}], now='2026-05-24T00:00:00+00:00', min_share=0.6, rate_delta_threshold=0.2)
    payload=json.loads(format_newsletter_segment_delivery_skew_json(report))
    assert payload["artifact_type"] == "newsletter_segment_delivery_skew"
    assert payload["totals"]["finding_count"] >= 1
    empty=build_newsletter_segment_delivery_skew_report([], now='2026-05-24T00:00:00+00:00')
    assert empty["empty_state"]["is_empty"] is True

def test_from_db_missing_table_and_cli_validation():
    conn=sqlite3.connect(":memory:")
    report=build_newsletter_segment_delivery_skew_report_from_db(conn)
    assert report["missing_tables"] or report["empty_state"]["is_empty"]
    assert script.main(["--limit","0"]) == 2
