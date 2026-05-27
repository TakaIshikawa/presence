from __future__ import annotations
import sqlite3
from api.newsletter_campaign_readiness import get_newsletter_campaign_readiness
def test_campaigns_classified_and_ordered():
    c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row
    c.executescript("CREATE TABLE newsletter_campaigns (campaign_id TEXT,subject TEXT,body TEXT,segment TEXT,scheduled_at TEXT); CREATE TABLE newsletter_link_inventory (issue_id TEXT,url TEXT); INSERT INTO newsletter_campaigns VALUES ('b','','Body','all','2026-01-02'),('a','Sub','Body','all','2026-01-01'); INSERT INTO newsletter_link_inventory VALUES ('a','https://x.test');")
    r=get_newsletter_campaign_readiness(c)
    assert r["artifact_type"]=="newsletter_campaign_readiness" and [x["campaign_id"] for x in r["campaigns"]]==["a","b"]
    assert r["summary"]=={"ready_count":1,"blocked_count":1} and "subject" in r["campaigns"][1]["missing_fields"]
    assert get_newsletter_campaign_readiness(sqlite3.connect(":memory:"))["status"]=="missing_schema"
