from __future__ import annotations
import json, sqlite3
from evaluation.generated_content_source_attribution_gaps import build_generated_content_source_attribution_gaps_report, build_generated_content_source_attribution_gaps_report_from_db, format_generated_content_source_attribution_gaps_json, format_generated_content_source_attribution_gaps_text
def test_flags_source_metadata_without_visible_attribution():
    rows=[{"id":1,"content_type":"post","body":"A claim with no cite","metadata":"source_id:abc https://a.com/x","created_at":"2026-06-01T00:00:00+00:00"},{"id":2,"content_type":"post","body":"According to Source: https://a.com/x","metadata":"https://a.com/x","created_at":"2026-06-01T00:00:00+00:00"},{"id":3,"content_type":"post","body":"No sources","metadata":"","created_at":"2026-06-01T00:00:00+00:00"}]
    r=build_generated_content_source_attribution_gaps_report(rows)
    assert [f["content_id"] for f in r["findings"]]==[1]
def test_db_filter_and_formatters():
    c=sqlite3.connect(":memory:"); c.executescript("CREATE TABLE generated_content(id INTEGER, content_type TEXT, body TEXT, metadata TEXT, created_at TEXT); INSERT INTO generated_content VALUES(1,'post','Claim','source_id:abc','2026-06-01T00:00:00+00:00');")
    r=build_generated_content_source_attribution_gaps_report_from_db(c,content_type="post")
    assert r["findings"][0]["source_count"]==1
    assert json.loads(format_generated_content_source_attribution_gaps_json(r))["artifact_type"]=="generated_content_source_attribution_gaps"
    assert "sources=1" in format_generated_content_source_attribution_gaps_text(r)
