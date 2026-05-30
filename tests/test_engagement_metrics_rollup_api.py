from __future__ import annotations
import sqlite3
from api.engagement_metrics_rollup import get_engagement_metrics_rollup
def test_rollups_include_metrics_and_missing_counts():
    c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row
    c.executescript("CREATE TABLE published_posts (id TEXT,channel TEXT,content_type TEXT,published_at TEXT,impressions INTEGER,likes INTEGER,replies INTEGER,reposts INTEGER,clicks INTEGER); INSERT INTO published_posts VALUES ('1','x','post','2099-01-01',100,10,1,2,3),('2','x','post','2099-01-02',NULL,5,0,0,1);")
    r=get_engagement_metrics_rollup(c,lookback_days=99999)
    g=r["rollups"][0]
    assert r["artifact_type"]=="engagement_metrics_rollup" and g["totals"]["likes"]==15 and g["averages"]["likes"]==7.5 and g["missing_metrics_count"]==1 and g["top_post"]["id"]=="1"
    assert get_engagement_metrics_rollup(sqlite3.connect(":memory:"))["status"]=="missing_schema"
