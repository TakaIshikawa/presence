from __future__ import annotations
import sqlite3
from ._utils import columns, has_table, missing_schema
ARTIFACT_TYPE="draft_queue_preview"
def get_draft_queue_preview(conn:sqlite3.Connection)->dict:
    if not has_table(conn,"generated_content"): return missing_schema(ARTIFACT_TYPE,["generated_content"])
    cols=columns(conn,"generated_content"); needed={"id","status","channel","content_type","created_at"}
    if not needed<=cols: return missing_schema(ARTIFACT_TYPE,sorted(needed-cols))
    review_cols={"review_status","reviewed_at","reviewer","review_metadata"}&cols
    rows=[dict(r) for r in conn.execute("SELECT id,channel,content_type,created_at FROM generated_content WHERE status IN ('pending','queued','draft') ORDER BY created_at,id")]
    groups={}
    for r in rows:
        k=(r["channel"] or "unknown",r["content_type"] or "unknown"); g=groups.setdefault(k,{"channel":k[0],"content_type":k[1],"count":0,"oldest_created_at":r["created_at"]})
        g["count"]+=1; g["oldest_created_at"]=min(g["oldest_created_at"],r["created_at"])
    missing_review=0
    if review_cols:
        expr=" AND ".join([f"({c} IS NULL OR {c}='')" for c in review_cols])
        missing_review=conn.execute(f"SELECT COUNT(*) FROM generated_content WHERE status IN ('pending','queued','draft') AND ({expr})").fetchone()[0]
    else: missing_review=len(rows)
    return {"artifact_type":ARTIFACT_TYPE,"status":"ok","summary":{"pending_count":len(rows),"missing_review_metadata_count":missing_review},"groups":sorted(groups.values(),key=lambda g:(g["channel"],g["content_type"])),"oldest_draft":rows[0] if rows else None}
