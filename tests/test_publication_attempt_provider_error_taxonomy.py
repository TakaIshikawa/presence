from __future__ import annotations
import json, sqlite3
from evaluation.publication_attempt_provider_error_taxonomy import build_publication_attempt_provider_error_taxonomy_report, build_publication_attempt_provider_error_taxonomy_report_from_db, classify_error_family, format_publication_attempt_provider_error_taxonomy_json, format_publication_attempt_provider_error_taxonomy_text
def test_keyword_classification_and_filters():
    assert classify_error_family("429 rate limit")=="rate_limit"
    rows=[{"id":1,"content_id":1,"provider":"p","platform":"x","status":"failed","error":"auth token bad","created_at":"2026-06-01T00:00:00+00:00"},{"id":2,"provider":"p","platform":"y","status":"success","error":"auth","created_at":"2026-06-01T00:00:00+00:00"}]
    r=build_publication_attempt_provider_error_taxonomy_report(rows,provider="p",platform="x")
    assert r["findings"][0]["error_family"]=="auth"
def test_db_and_formatters():
    c=sqlite3.connect(":memory:"); c.executescript("CREATE TABLE publication_attempts(id INTEGER,content_id INTEGER,provider TEXT,platform TEXT,status TEXT,error TEXT,created_at TEXT); INSERT INTO publication_attempts VALUES(1,1,'p','x','error','timeout','2026-06-01T00:00:00+00:00');")
    r=build_publication_attempt_provider_error_taxonomy_report_from_db(c)
    assert r["findings"][0]["affected_content_count"]==1
    assert json.loads(format_publication_attempt_provider_error_taxonomy_json(r))["artifact_type"]=="publication_attempt_provider_error_taxonomy"
    assert "timeout" in format_publication_attempt_provider_error_taxonomy_text(r)
