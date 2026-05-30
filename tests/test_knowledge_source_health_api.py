from __future__ import annotations
import sqlite3
from api.knowledge_source_health import get_knowledge_source_health
def test_source_health_counts_stale_failures_and_coverage():
    c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row
    c.executescript("CREATE TABLE knowledge_sources (id TEXT,source_type TEXT,status TEXT,last_success_at TEXT,canonical_url TEXT,failure_reason TEXT,last_failure_at TEXT); INSERT INTO knowledge_sources VALUES ('1','doc','ok','2026-01-01T00:00:00Z','https://x',NULL,NULL),('2','feed','error',NULL,'','timeout','2026-01-02T00:00:00Z');")
    r=get_knowledge_source_health(c,stale_days=1)
    assert r["artifact_type"]=="knowledge_source_health" and r["summary"]["total_sources"]==2 and r["summary"]["stale_count"]>=1
    assert r["recent_failures"][0]["failure_reason"]=="timeout" and r["counts_by_type_status"]
    assert get_knowledge_source_health(sqlite3.connect(":memory:"))["status"]=="missing_schema"
