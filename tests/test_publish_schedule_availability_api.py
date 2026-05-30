from __future__ import annotations
import sqlite3
from api.publish_schedule_availability import get_publish_schedule_availability
def test_availability_accounts_for_schedule_quiet_hours_and_blackouts():
    c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row
    c.executescript("CREATE TABLE publish_queue (channel TEXT,scheduled_at TEXT,status TEXT); CREATE TABLE publish_quiet_hours (channel TEXT,start_hour INTEGER,end_hour INTEGER); CREATE TABLE publish_blackouts (channel TEXT,start_at TEXT,end_at TEXT); INSERT INTO publish_queue VALUES ('x','2026-01-01T10:00:00','scheduled'); INSERT INTO publish_quiet_hours VALUES ('x',9,10); INSERT INTO publish_blackouts VALUES ('x','2026-01-01T10:00:00','2026-01-01T11:00:00');")
    r=get_publish_schedule_availability(c,now="2026-01-01T09:00:00",daily_capacity=2)
    assert r["artifact_type"]=="publish_schedule_availability" and r["channels"][0]["next_available_at"]=="2026-01-01T11:00:00" and r["channels"][0]["capacity_remaining"]==1
    assert get_publish_schedule_availability(sqlite3.connect(":memory:"))["status"]=="missing_schema"
