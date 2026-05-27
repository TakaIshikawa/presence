from __future__ import annotations
import sqlite3
from ._utils import columns, has_table, missing_schema
ARTIFACT_TYPE="newsletter_campaign_readiness"
REQUIRED=["subject","body","segment","scheduled_at"]
def get_newsletter_campaign_readiness(conn:sqlite3.Connection)->dict:
    if not has_table(conn,"newsletter_campaigns"): return missing_schema(ARTIFACT_TYPE,["newsletter_campaigns"])
    cols=columns(conn,"newsletter_campaigns"); id_col="campaign_id" if "campaign_id" in cols else "id"
    needed={id_col,*REQUIRED}
    if not needed<=cols: return missing_schema(ARTIFACT_TYPE,sorted(needed-cols))
    link_table=has_table(conn,"newsletter_link_inventory")
    rows=[]
    for r in conn.execute(f"SELECT {id_col} AS campaign_id, subject, body, segment, scheduled_at FROM newsletter_campaigns ORDER BY scheduled_at, {id_col}"):
        d=dict(r); missing=[f for f in REQUIRED if not d.get(f)]
        if link_table and not conn.execute("SELECT 1 FROM newsletter_link_inventory WHERE issue_id=? LIMIT 1",(str(d["campaign_id"]),)).fetchone(): missing.append("link_inventory")
        rows.append({**d,"status":"blocked" if missing else "ready","missing_fields":missing})
    return {"artifact_type":ARTIFACT_TYPE,"summary":{"ready_count":sum(1 for r in rows if r["status"]=="ready"),"blocked_count":sum(1 for r in rows if r["status"]=="blocked")},"campaigns":rows}
