from __future__ import annotations
import sqlite3
from api.reply_draft_quality_summary import get_reply_draft_quality_summary
def test_quality_bands_flags_and_oldest():
    c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row
    c.executescript("CREATE TABLE reply_queue (id TEXT,status TEXT,created_at TEXT,quality_score INTEGER,sycophancy_flag INTEGER,generic_flag INTEGER,unanswered_question_flag INTEGER); INSERT INTO reply_queue VALUES ('1','pending','2026-01-01',9,1,0,0),('2','draft','2026-01-02',4,0,1,1);")
    r=get_reply_draft_quality_summary(c)
    assert r["artifact_type"]=="reply_draft_quality_summary" and r["quality_bands"]["high"]==1 and r["quality_bands"]["low"]==1
    assert r["flag_counts"]=={"sycophancy":1,"generic_reply":1,"unanswered_question":1} and r["oldest_pending_draft"]["id"]=="1"
    assert get_reply_draft_quality_summary(sqlite3.connect(":memory:"))["status"]=="missing_schema"
