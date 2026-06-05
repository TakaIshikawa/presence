from __future__ import annotations
import json, sqlite3
from evaluation.published_post_quote_reuse_risk import build_published_post_quote_reuse_risk_report, build_published_post_quote_reuse_risk_report_from_db, extract_quoted_phrases, normalize_phrase, format_published_post_quote_reuse_risk_json, format_published_post_quote_reuse_risk_text
def test_quote_extraction_reuse_and_platform_filter():
    assert extract_quoted_phrases('A "repeat this exact phrase" here')==["repeat this exact phrase"]
    assert normalize_phrase("Repeat, this!")=="repeat this"
    rows=[{"id":1,"platform":"x","body":'"repeat this exact phrase"',"published_at":"2026-06-01T00:00:00+00:00"},{"id":2,"platform":"x","body":'"Repeat this exact phrase"',"published_at":"2026-06-02T00:00:00+00:00"},{"id":3,"platform":"y","body":'"repeat this exact phrase"',"published_at":"2026-06-02T00:00:00+00:00"}]
    r=build_published_post_quote_reuse_risk_report(rows,platform="x")
    assert r["findings"][0]["reuse_count"]==2
def test_db_and_formatters():
    c=sqlite3.connect(":memory:"); c.executescript("CREATE TABLE published_posts(id INTEGER,platform TEXT,body TEXT,published_at TEXT); INSERT INTO published_posts VALUES(1,'x','\"a reusable quoted phrase\"','2026-06-01T00:00:00+00:00'),(2,'x','\"a reusable quoted phrase\"','2026-06-02T00:00:00+00:00');")
    r=build_published_post_quote_reuse_risk_report_from_db(c)
    assert r["findings"][0]["post_ids"]==[1,2]
    assert json.loads(format_published_post_quote_reuse_risk_json(r))["artifact_type"]=="published_post_quote_reuse_risk"
    assert "reuse=2" in format_published_post_quote_reuse_risk_text(r)
