from __future__ import annotations
import sqlite3
from ._utils import columns, has_table, iso_days_ago, missing_schema
ARTIFACT_TYPE="engagement_metrics_rollup"; METRICS=["impressions","likes","replies","reposts","clicks"]
def get_engagement_metrics_rollup(conn:sqlite3.Connection,*,lookback_days:int=30)->dict:
    if not has_table(conn,"published_posts"): return missing_schema(ARTIFACT_TYPE,["published_posts"])
    cols=columns(conn,"published_posts"); needed={"id","channel","content_type"}
    if not needed<=cols: return missing_schema(ARTIFACT_TYPE,sorted(needed-cols))
    date_col="published_at" if "published_at" in cols else "fetched_at" if "fetched_at" in cols else None
    present=[m for m in METRICS if m in cols]; cutoff=iso_days_ago(lookback_days)
    where=f"WHERE {date_col}>=?" if date_col else ""; params=(cutoff,) if date_col else ()
    rows=[dict(r) for r in conn.execute(f"SELECT * FROM published_posts {where}",params)]
    groups={}
    for r in rows:
        k=(r.get("channel") or "unknown",r.get("content_type") or "unknown"); g=groups.setdefault(k,{"channel":k[0],"content_type":k[1],"post_count":0,"totals":{m:0 for m in present},"averages":{},"top_post":None,"missing_metrics_count":0})
        g["post_count"]+=1; total_eng=0
        for m in present:
            if r.get(m) is None: g["missing_metrics_count"]+=1
            else: g["totals"][m]+=r[m]; total_eng+=r[m]
        if not g["top_post"] or total_eng>g["top_post"]["engagement_total"]: g["top_post"]={"id":r["id"],"engagement_total":total_eng}
    for g in groups.values(): g["averages"]={m:(g["totals"][m]/g["post_count"] if g["post_count"] else 0) for m in present}
    return {"artifact_type":ARTIFACT_TYPE,"lookback_days":lookback_days,"metrics":present,"rollups":sorted(groups.values(),key=lambda g:(g["channel"],g["content_type"]))}
