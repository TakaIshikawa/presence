from __future__ import annotations
import sqlite3
from ._utils import columns, has_table, iso_days_ago, missing_schema
ARTIFACT_TYPE="knowledge_source_health"
def get_knowledge_source_health(conn:sqlite3.Connection,*,stale_days:int=30,limit:int=5)->dict:
    if not has_table(conn,"knowledge_sources"): return missing_schema(ARTIFACT_TYPE,["knowledge_sources"])
    cols=columns(conn,"knowledge_sources"); needed={"id","source_type","status","last_success_at","canonical_url"}
    if not needed<=cols: return missing_schema(ARTIFACT_TYPE,sorted(needed-cols))
    counts=[dict(r) for r in conn.execute("SELECT source_type,status,COUNT(*) AS count FROM knowledge_sources GROUP BY source_type,status ORDER BY source_type,status")]
    cutoff=iso_days_ago(stale_days)
    stale=[dict(r) for r in conn.execute("SELECT id,source_type,status,last_success_at FROM knowledge_sources WHERE last_success_at IS NULL OR last_success_at < ? ORDER BY last_success_at,id LIMIT ?",(cutoff,limit))]
    failure_cols={"failure_reason","last_failure_at"}&cols
    failures=[]
    if failure_cols=={"failure_reason","last_failure_at"}:
        failures=[dict(r) for r in conn.execute("SELECT id,source_type,failure_reason,last_failure_at AS observed_at FROM knowledge_sources WHERE failure_reason IS NOT NULL ORDER BY last_failure_at DESC,id LIMIT ?",(limit,))]
    total=conn.execute("SELECT COUNT(*) FROM knowledge_sources").fetchone()[0]
    canonical=conn.execute("SELECT COUNT(*) FROM knowledge_sources WHERE canonical_url IS NOT NULL AND canonical_url!=''").fetchone()[0]
    return {"artifact_type":ARTIFACT_TYPE,"summary":{"total_sources":total,"canonical_url_coverage":canonical/total if total else 0,"stale_count":len(stale)},"counts_by_type_status":counts,"stale_sources":stale,"recent_failures":failures,"stale_threshold_days":stale_days}
