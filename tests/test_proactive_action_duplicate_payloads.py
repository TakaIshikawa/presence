from __future__ import annotations
import sqlite3, json
from engagement.proactive_action_duplicate_payloads import build_proactive_action_duplicate_payloads_report, build_proactive_action_duplicate_payloads_report_from_db

def test_exact_normalized_duplicates_across_targets_ignore_short():
    rows = [
        {"id": 1, "status": "queued", "target_url": "https://a.test", "payload": "Hello there, this is a detailed draft payload with enough text."},
        {"id": 2, "status": "queued", "target_url": "https://b.test", "payload": " hello THERE,   this is a detailed draft payload with enough text. "},
        {"id": 3, "status": "queued", "target_url": "https://c.test", "payload": "same"},
    ]
    r = build_proactive_action_duplicate_payloads_report(rows, min_length=20)
    assert r["artifact_type"] == "proactive_action_duplicate_payloads"
    assert r["findings"][0]["action_ids"] == [1, 2]
    assert r["findings"][0]["duplicate_reason"] == "exact_normalized_payload"

def test_db_reads_metadata_and_malformed_json():
    c = sqlite3.connect(":memory:")
    c.execute("CREATE TABLE proactive_actions (id INTEGER, status TEXT, target_url TEXT, metadata TEXT)")
    p = "Opening clause with enough duplicated payload text for a real check"
    c.execute("INSERT INTO proactive_actions VALUES (1,'queued','https://a.test',?)", (json.dumps({"draft": p}),))
    c.execute("INSERT INTO proactive_actions VALUES (2,'queued','https://b.test',?)", (p,))
    r = build_proactive_action_duplicate_payloads_report_from_db(c, min_length=20)
    assert r["summary"]["finding_count"] >= 1
