from __future__ import annotations
import json, sqlite3
from evaluation.newsletter_bounce_domain_clusters import build_newsletter_bounce_domain_clusters_report, build_newsletter_bounce_domain_clusters_report_from_db, extract_email_domain, format_newsletter_bounce_domain_clusters_json, format_newsletter_bounce_domain_clusters_text
def test_domain_extraction_and_row_builder():
    assert extract_email_domain("A@Example.COM")=="example.com"
    assert extract_email_domain("bad-address")==""
    rows=[{"id":1,"email":"a@x.com","event_type":"bounce","created_at":"2026-06-01T00:00:00+00:00"},{"id":2,"email":"b@X.com","event_type":"failed","created_at":"2026-06-02T00:00:00+00:00"},{"id":3,"email":"c@y.com","event_type":"open","created_at":"2026-06-02T00:00:00+00:00"}]
    r=build_newsletter_bounce_domain_clusters_report(rows,min_count=2,min_bounce_rate=.5)
    assert r["findings"][0]["domain"]=="x.com"
def test_db_json_text():
    c=sqlite3.connect(":memory:"); c.executescript("CREATE TABLE newsletter_events(id INTEGER,email TEXT,event_type TEXT,created_at TEXT); INSERT INTO newsletter_events VALUES(1,'a@x.com','bounce','2026-06-01T00:00:00+00:00'),(2,'b@x.com','bounce','2026-06-02T00:00:00+00:00');")
    r=build_newsletter_bounce_domain_clusters_report_from_db(c,min_count=2)
    assert r["findings"][0]["bounce_count"]==2
    assert json.loads(format_newsletter_bounce_domain_clusters_json(r))["artifact_type"]=="newsletter_bounce_domain_clusters"
    assert "x.com" in format_newsletter_bounce_domain_clusters_text(r)
