from __future__ import annotations
import sqlite3
from evaluation.publication_channel_cooldown_violations import build_publication_channel_cooldown_violations_report, build_publication_channel_cooldown_violations_report_from_db

def test_adjacent_same_channel_only():
    rows=[{"id":1,"channel":"x","status":"queued","scheduled_at":"2026-05-01T10:00:00+00:00"},{"id":2,"channel":"x","status":"queued","scheduled_at":"2026-05-01T10:30:00+00:00"},{"id":3,"channel":"bsky","status":"queued","scheduled_at":"2026-05-01T10:40:00+00:00"}]
    r=build_publication_channel_cooldown_violations_report(rows,cooldown_minutes=60,now="2026-05-02T00:00:00+00:00" if False else None)
    assert r["artifact_type"]=="publication_channel_cooldown_violations"
    assert r["findings"][0]["earlier_item_id"]==1 and r["findings"][0]["later_item_id"]==2
def test_db_supports_publish_queue():
    c=sqlite3.connect(":memory:"); c.execute("CREATE TABLE publish_queue (id INTEGER, channel TEXT, status TEXT, scheduled_at TEXT)")
    c.executemany("INSERT INTO publish_queue VALUES (?,?,?,?)",[(1,"x","queued","2026-05-01T10:00:00+00:00"),(2,"x","queued","2026-05-01T10:10:00+00:00")])
    assert build_publication_channel_cooldown_violations_report_from_db(c,cooldown_minutes=60,window_days=9999)["summary"]["finding_count"]==1
