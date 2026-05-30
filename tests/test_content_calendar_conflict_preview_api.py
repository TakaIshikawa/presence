from __future__ import annotations
import sqlite3
from api.content_calendar_conflict_preview import get_content_calendar_conflict_preview
def test_calendar_conflicts_and_optional_blackouts():
    c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row
    c.executescript("CREATE TABLE content_calendar (id TEXT,channel TEXT,scheduled_at TEXT,topic TEXT); CREATE TABLE calendar_blackouts (channel TEXT,start_at TEXT,end_at TEXT,reason TEXT); INSERT INTO content_calendar VALUES ('1','x','2026-01-01T10:00:00','ai'),('2','x','2026-01-01T10:00:00','ai'),('3','y','2026-01-02T10:00:00','ai'); INSERT INTO calendar_blackouts VALUES ('x','2026-01-01T09:00:00','2026-01-01T11:00:00','holiday');")
    r=get_content_calendar_conflict_preview(c)
    types={x["type"] for x in r["conflicts"]}
    assert r["artifact_type"]=="content_calendar_conflict_preview" and {"same_channel_collision","topic_repetition","blackout_overlap"}<=types
    c2=sqlite3.connect(":memory:"); c2.row_factory=sqlite3.Row; c2.execute("CREATE TABLE content_calendar (id TEXT,channel TEXT,scheduled_at TEXT,topic TEXT)")
    assert get_content_calendar_conflict_preview(c2)["conflicts"]==[]
    assert get_content_calendar_conflict_preview(sqlite3.connect(":memory:"))["status"]=="missing_schema"
