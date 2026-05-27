from __future__ import annotations
from datetime import datetime, timezone
import json, sqlite3
from evaluation.publish_queue_manual_override_audit import build_publish_queue_manual_override_audit_report, build_publish_queue_manual_override_audit_report_from_db
NOW=datetime(2026,5,1,12,tzinfo=timezone.utc)
def test_flags_override_gaps_independently():
    r=build_publish_queue_manual_override_audit_report([{"queue_id":1,"status":"queued","manual_override":True,"quality_status":"failed","created_at":"2026-04-20T00:00:00+00:00"}],max_age_hours=24,now=NOW)
    assert r["artifact_type"]=="publish_queue_manual_override_audit"
    assert {"missing_actor","missing_reason","stale_override","quality_gate_conflict"} <= {f["gap_reason"] for f in r["findings"]}
def test_db_metadata_fallbacks():
    c=sqlite3.connect(":memory:"); c.execute("CREATE TABLE publish_queue (id INTEGER, status TEXT, metadata TEXT, created_at TEXT)")
    c.execute("INSERT INTO publish_queue VALUES (1,'queued',?,?)",(json.dumps({"manual_override":True,"actor":"u"}),NOW.isoformat()))
    assert build_publish_queue_manual_override_audit_report_from_db(c,now=NOW)["findings"][0]["gap_reason"]=="missing_reason"
