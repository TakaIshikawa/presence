from __future__ import annotations
import json, sqlite3
from evaluation.generated_content_first_person_density import build_generated_content_first_person_density_report, build_generated_content_first_person_density_report_from_db, strip_quotes_and_code, format_generated_content_first_person_density_json, format_generated_content_first_person_density_text
def test_density_scoring_and_quote_code_exclusion():
    text="> I me my quoted\n```we us our```\nI think my work shows our path forward with clear details for readers today."
    assert "quoted" not in strip_quotes_and_code(text)
    r=build_generated_content_first_person_density_report([{"id":1,"content_type":"post","status":"draft","body":text,"created_at":"2026-06-01T00:00:00+00:00"}],min_words=5,min_density=.2,now=__import__("datetime").datetime(2026,6,5,tzinfo=__import__("datetime").timezone.utc))
    assert r["findings"][0]["sample_terms"]==["i","my","our"]
def test_filters_and_db_and_formatters():
    c=sqlite3.connect(":memory:"); c.executescript("CREATE TABLE generated_content(id INTEGER, content_type TEXT, status TEXT, body TEXT, created_at TEXT); INSERT INTO generated_content VALUES(1,'post','draft','I my our words words words words words words words','2026-06-01T00:00:00+00:00');")
    r=build_generated_content_first_person_density_report_from_db(c,content_type="post",min_words=5,min_density=.1)
    assert r["findings"][0]["content_id"]==1
    assert build_generated_content_first_person_density_report([{"id":2,"content_type":"thread","body":"I my words words words words words","created_at":"2026-06-01T00:00:00+00:00"}],content_type="post",min_words=5)["findings"]==[]
    assert json.loads(format_generated_content_first_person_density_json(r))["artifact_type"]=="generated_content_first_person_density"
    assert "0." in format_generated_content_first_person_density_text(r)
