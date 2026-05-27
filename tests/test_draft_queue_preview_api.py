from __future__ import annotations
import sqlite3
from api.draft_queue_preview import get_draft_queue_preview
def _c(): c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; return c
def test_populated_empty_and_missing_schema():
    c=_c(); c.executescript("CREATE TABLE generated_content (id TEXT,status TEXT,channel TEXT,content_type TEXT,created_at TEXT,review_metadata TEXT); INSERT INTO generated_content VALUES ('2','pending','x','post','2026-01-02',NULL),('1','queued','x','post','2026-01-01','{}'),('3','published','x','post','2026-01-03',NULL);")
    r=get_draft_queue_preview(c)
    assert r["artifact_type"]=="draft_queue_preview" and r["summary"]["pending_count"]==2 and r["groups"][0]["count"]==2 and r["oldest_draft"]["id"]=="1"
    e=_c(); e.execute("CREATE TABLE generated_content (id TEXT,status TEXT,channel TEXT,content_type TEXT,created_at TEXT)")
    assert get_draft_queue_preview(e)["oldest_draft"] is None
    assert get_draft_queue_preview(_c())["status"]=="missing_schema"
    p=_c(); p.execute("CREATE TABLE generated_content (id TEXT)")
    assert get_draft_queue_preview(p)["status"]=="missing_schema"
