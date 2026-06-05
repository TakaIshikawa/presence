from __future__ import annotations
import json, sqlite3
from evaluation.reply_draft_excessive_apology_report import build_reply_draft_excessive_apology_report, build_reply_draft_excessive_apology_report_from_db, count_apologies, strip_quoted_or_source_text, format_reply_draft_excessive_apology_report_json, format_reply_draft_excessive_apology_report_text
def test_threshold_quote_exclusion_and_ordering():
    assert count_apologies("Sorry, my bad. I apologize.")==["sorry","my bad","i apologize"]
    assert "Sorry" not in strip_quoted_or_source_text("> Sorry quoted\nActual")
    rows=[{"id":2,"body":"sorry apologies my bad forgive me"},{"id":1,"body":"> sorry sorry\nsorry my bad"}]
    r=build_reply_draft_excessive_apology_report(rows,min_apology_count=2)
    assert [f["reply_id"] for f in r["findings"]]==[2,1]
def test_db_and_formatters():
    c=sqlite3.connect(":memory:"); c.executescript("CREATE TABLE reply_drafts(id INTEGER,mention_id INTEGER,body TEXT,created_at TEXT); INSERT INTO reply_drafts VALUES(1,2,'sorry my bad','2026-06-01T00:00:00+00:00');")
    r=build_reply_draft_excessive_apology_report_from_db(c,min_apology_count=2)
    assert r["findings"][0]["mention_id"]==2
    assert json.loads(format_reply_draft_excessive_apology_report_json(r))["artifact_type"]=="reply_draft_excessive_apology_report"
    assert "apologies=2" in format_reply_draft_excessive_apology_report_text(r)
