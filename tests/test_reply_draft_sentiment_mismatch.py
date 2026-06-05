from __future__ import annotations
import json, sqlite3
from evaluation.reply_draft_sentiment_mismatch import build_reply_draft_sentiment_mismatch_report, build_reply_draft_sentiment_mismatch_report_from_db, infer_sentiment, format_reply_draft_sentiment_mismatch_json, format_reply_draft_sentiment_mismatch_text
def test_metadata_keyword_fallback_and_filters():
    assert infer_sentiment("this is awful and broken")=="negative"
    rows=[{"id":1,"mention_id":2,"platform":"x","status":"draft","inbound_sentiment":"negative","draft_sentiment":"positive","draft_text":"Great!","created_at":"2026-06-01T00:00:00+00:00"},{"id":2,"platform":"y","status":"draft","inbound_text":"awful","draft_text":"thanks","created_at":"2026-06-01T00:00:00+00:00"}]
    r=build_reply_draft_sentiment_mismatch_report(rows,platform="x",status=("draft",))
    assert r["findings"][0]["mismatch_type"]=="inappropriately_upbeat"
def test_db_and_formatters():
    c=sqlite3.connect(":memory:"); c.executescript("CREATE TABLE reply_drafts(id INTEGER,mention_id INTEGER,status TEXT,inbound_sentiment TEXT,draft_sentiment TEXT,draft_text TEXT,created_at TEXT); INSERT INTO reply_drafts VALUES(1,2,'draft','negative','positive','Great','2026-06-01T00:00:00+00:00');")
    r=build_reply_draft_sentiment_mismatch_report_from_db(c)
    assert r["findings"][0]["reply_id"]==1
    assert json.loads(format_reply_draft_sentiment_mismatch_json(r))["artifact_type"]=="reply_draft_sentiment_mismatch"
    assert "severity=3" in format_reply_draft_sentiment_mismatch_text(r)
