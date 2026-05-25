"""Flag unusual newsletter segment subscriber growth."""
from __future__ import annotations
from collections import defaultdict
from datetime import timedelta
from typing import Any
from ._batch_report_common import *

ARTIFACT_TYPE="newsletter_segment_growth_anomalies"; DEFAULT_WINDOW_DAYS=7; DEFAULT_BASELINE_DAYS=28; DEFAULT_LIMIT=50; DEFAULT_RATIO=1.5

def build_newsletter_segment_growth_anomalies_report(rows:list[dict[str,Any]],*,window_days:int=DEFAULT_WINDOW_DAYS,baseline_days:int=DEFAULT_BASELINE_DAYS,limit:int=DEFAULT_LIMIT,missing_tables=None,missing_columns=None,now=None):
    positive("window_days",window_days); positive("baseline_days",baseline_days); positive("limit",limit)
    gen=now_value(now); recent_cut=gen-timedelta(days=window_days); base_cut=recent_cut-timedelta(days=baseline_days)
    buckets:dict[tuple[str,str],dict[str,int]]=defaultdict(lambda:{"recent":0,"baseline":0})
    for r in rows:
        ts=dt(r.get("subscribed_at") or r.get("created_at") or r.get("joined_at"))
        if not ts or ts<base_cut or ts>gen: continue
        sid=clean(r.get("segment_id") or r.get("segment") or r.get("status"),"unknown")
        name=clean(r.get("segment_name") or r.get("segment") or r.get("status"),sid)
        bucket="recent" if ts>=recent_cut else "baseline"
        buckets[(sid,name)][bucket]+=1
    anomalies=[]
    for (sid,name),v in buckets.items():
        expected=v["baseline"]*(window_days/baseline_days) if baseline_days else 0
        delta=round(v["recent"]-expected,2); ratio=round(v["recent"]/expected,4) if expected else (None if not v["recent"] else float("inf"))
        if abs(delta)>=2 or ratio in (float("inf"),) or (ratio is not None and (ratio>=DEFAULT_RATIO or ratio<=1/DEFAULT_RATIO)):
            action="investigate acquisition/import spike" if delta>0 else "check segment suppression or source drop"
            anomalies.append({"segment_id":sid,"segment_name":name,"recent_count":v["recent"],"baseline_count":v["baseline"],"growth_delta":delta,"growth_ratio":ratio,"recommended_action":action})
    anomalies.sort(key=lambda f:(-abs(f["growth_delta"]),str(f["segment_id"])))
    shown=anomalies[:limit]
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":gen.isoformat(),"filters":{"window_days":window_days,"baseline_days":baseline_days,"limit":limit},"summary":{"segments":len(buckets),"anomaly_count":len(anomalies),"shown":len(shown),"recent_total":sum(v["recent"] for v in buckets.values()),"baseline_total":sum(v["baseline"] for v in buckets.values())},"anomalies":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":empty_state(anomalies,"No newsletter segment growth anomalies found.",schema_gap=bool(missing_tables or missing_columns))}

def build_newsletter_segment_growth_anomalies_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); mt=[]; mc={}; rows=[]
    if "newsletter_subscribers" not in s:
        mt.append("newsletter_subscribers")
        return build_newsletter_segment_growth_anomalies_report([],missing_tables=mt,missing_columns=mc,**kw)
    cols=s["newsletter_subscribers"]
    if not ({"id","subscriber_id","email"} & cols): mc["newsletter_subscribers"]=["id|subscriber_id|email"]
    if not ({"subscribed_at","created_at","joined_at"} & cols): mc.setdefault("newsletter_subscribers",[]).append("subscribed_at|created_at|joined_at")
    if "newsletter_subscriber_segments" in s:
        j=s["newsletter_subscriber_segments"]; seg=s.get("newsletter_segments",set())
        if not ({"subscriber_id","email"} & j): mc["newsletter_subscriber_segments"]=["subscriber_id|email"]
        if not ({"segment_id","segment"} & j): mc.setdefault("newsletter_subscriber_segments",[]).append("segment_id|segment")
        if not mc:
            sub_id_col=next(c for c in ("id","subscriber_id","email") if c in cols); ts_col=next(c for c in ("subscribed_at","created_at","joined_at") if c in cols)
            join_key="subscriber_id" if "subscriber_id" in j else "email"; sub_key="id" if join_key=="subscriber_id" and "id" in cols else join_key
            seg_expr="ss.segment_id" if "segment_id" in j else "ss.segment"; name_col=next((c for c in ("name","segment_name") if c in seg),None)
            name_expr=f"COALESCE(ns.{name_col}, {seg_expr})" if name_col and "newsletter_segments" in s and {"id","segment_id"} & seg else seg_expr
            join_seg=""; 
            if "newsletter_segments" in s and ({"id","segment_id"} & seg):
                key="id" if "id" in seg else "segment_id"; join_seg=f" LEFT JOIN newsletter_segments ns ON ns.{key}=ss.segment_id"
            rows=[dict(r) for r in conn.execute(f"SELECT n.{sub_id_col} AS subscriber_key, n.{ts_col} AS subscribed_at, {seg_expr} AS segment_id, {name_expr} AS segment_name FROM newsletter_subscribers n JOIN newsletter_subscriber_segments ss ON ss.{join_key}=n.{sub_key}{join_seg}")]
    else:
        mt.append("newsletter_subscriber_segments|newsletter_segments")
        if not mc:
            if not ({"segment_id","segment","status"} & cols): mc["newsletter_subscribers"]=["segment_id|segment|status"]
            else: rows=load_table(conn,"newsletter_subscribers",cols,{"subscribed_at":("subscribed_at","created_at","joined_at"),"segment_id":("segment_id","segment","status"),"segment_name":("segment_name","segment","status")})
    return build_newsletter_segment_growth_anomalies_report(rows,missing_tables=mt,missing_columns=mc,**kw)

def format_newsletter_segment_growth_anomalies_json(r): return json_dumps(r)
def format_newsletter_segment_growth_anomalies_text(r):
    s=r["summary"]; lines=["Newsletter Segment Growth Anomalies",f"Generated: {r['generated_at']}",f"Totals: segments={s['segments']} anomalies={s['anomaly_count']} recent={s['recent_total']} baseline={s['baseline_total']}"]
    if r["missing_tables"]: lines.append("Missing tables: "+", ".join(r["missing_tables"]))
    if r["missing_columns"]: lines.append("Missing columns: "+flatten_missing(r["missing_columns"]))
    if not r["anomalies"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    lines+=["","segment_id | segment_name | recent | baseline | delta | ratio"]
    for f in r["anomalies"]: lines.append(f"{f['segment_id']} | {f['segment_name']} | {f['recent_count']} | {f['baseline_count']} | {f['growth_delta']} | {f['growth_ratio']}")
    return "\n".join(lines)
