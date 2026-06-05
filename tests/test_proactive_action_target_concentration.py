from __future__ import annotations
import json, sqlite3
from evaluation.proactive_action_target_concentration import build_proactive_action_target_concentration_report, build_proactive_action_target_concentration_report_from_db, normalize_handle, extract_domain, format_proactive_action_target_concentration_json, format_proactive_action_target_concentration_text
def test_handle_domain_and_row_builder():
    assert normalize_handle("@User-Name!")=="username"
    assert extract_domain("https://www.Example.com/a")=="example.com"
    rows=[{"id":i,"target_handle":"@A","status":"open","created_at":"2026-06-01T00:00:00+00:00"} for i in range(3)]+[{"id":9,"target_handle":"@B","status":"open","created_at":"2026-06-01T00:00:00+00:00"}]
    r=build_proactive_action_target_concentration_report(rows,min_count=3,min_share=.5,status=("open",))
    assert r["findings"][0]["target_key"]=="a"
def test_db_and_formatters():
    c=sqlite3.connect(":memory:"); c.executescript("CREATE TABLE proactive_actions(id INTEGER,target_url TEXT,status TEXT,created_at TEXT); INSERT INTO proactive_actions VALUES(1,'https://x.com/a','open','2026-06-01T00:00:00+00:00'),(2,'https://x.com/b','open','2026-06-01T00:00:00+00:00'),(3,'https://x.com/c','open','2026-06-01T00:00:00+00:00');")
    r=build_proactive_action_target_concentration_report_from_db(c,target_type="domain",min_count=3,min_share=.5)
    assert r["findings"][0]["target_type"]=="domain"
    assert json.loads(format_proactive_action_target_concentration_json(r))["artifact_type"]=="proactive_action_target_concentration"
    assert "x.com" in format_proactive_action_target_concentration_text(r)
