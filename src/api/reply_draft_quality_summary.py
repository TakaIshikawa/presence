from __future__ import annotations
import sqlite3
from ._utils import columns, has_table, missing_schema
ARTIFACT_TYPE="reply_draft_quality_summary"
THRESHOLDS={"high_min":8,"medium_min":5}
def get_reply_draft_quality_summary(conn:sqlite3.Connection)->dict:
    if not has_table(conn,"reply_queue"): return missing_schema(ARTIFACT_TYPE,["reply_queue"])
    cols=columns(conn,"reply_queue"); needed={"id","status","created_at"}
    if not needed<=cols: return missing_schema(ARTIFACT_TYPE,sorted(needed-cols))
    score="quality_score" if "quality_score" in cols else None
    fields=[c for c in ["sycophancy_flag","generic_flag","unanswered_question_flag"] if c in cols]
    sel="id,created_at"+(f",{score}" if score else "")+(" ,"+",".join(fields) if fields else "")
    rows=[dict(r) for r in conn.execute(f"SELECT {sel} FROM reply_queue WHERE status IN ('pending','queued','draft') ORDER BY created_at,id")]
    bands={"high":0,"medium":0,"low":0,"unknown":0}; flags={"sycophancy":0,"generic_reply":0,"unanswered_question":0}
    for r in rows:
        s=r.get(score) if score else None
        bands["unknown" if s is None else "high" if s>=8 else "medium" if s>=5 else "low"]+=1
        flags["sycophancy"]+=1 if r.get("sycophancy_flag") else 0; flags["generic_reply"]+=1 if r.get("generic_flag") else 0; flags["unanswered_question"]+=1 if r.get("unanswered_question_flag") else 0
    return {"artifact_type":ARTIFACT_TYPE,"thresholds":THRESHOLDS,"summary":{"pending_count":len(rows)},"quality_bands":bands,"flag_counts":flags,"oldest_pending_draft":rows[0] if rows else None}
