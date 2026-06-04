"""Find newsletter audience segments with open rates far from baseline."""
from __future__ import annotations
from collections import defaultdict
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE="newsletter_open_rate_segment_outliers"; DEFAULT_LOOKBACK_DAYS=30; DEFAULT_MIN_SEGMENT_SIZE=20; DEFAULT_MIN_DELTA_RATE=0.1; DEFAULT_DIRECTION="both"; DEFAULT_LIMIT=50
def build_newsletter_open_rate_segment_outliers_report(rows:list[dict[str,Any]],*,lookback_days:int=DEFAULT_LOOKBACK_DAYS,min_segment_size:int=DEFAULT_MIN_SEGMENT_SIZE,min_delta_rate:float=DEFAULT_MIN_DELTA_RATE,direction:str=DEFAULT_DIRECTION,limit:int=DEFAULT_LIMIT,missing_tables:list[str]|None=None,missing_columns:dict[str,list[str]]|None=None,now=None)->dict[str,Any]:
 positive("lookback_days",lookback_days); positive("min_segment_size",min_segment_size); non_negative("min_delta_rate",min_delta_rate); positive("limit",limit)
 if direction not in {"low","high","both"}: raise ValueError("direction must be low, high, or both")
 gen=now_value(now); cutoff=gen-timedelta(days=lookback_days); campaigns=defaultdict(lambda:{"sent":0,"opens":0,"segments":defaultdict(lambda:{"sent":0,"opens":0})})
 for row in rows:
  sent_at=dt(row.get("sent_at") or row.get("created_at"))
  if sent_at and sent_at<cutoff: continue
  cid=clean(row.get("campaign_id") or row.get("newsletter_id")); sent=to_int(row.get("sent_count") or row.get("sent"),1); opens=to_int(row.get("open_count") or row.get("opens") or row.get("opened_count"),0)
  key=clean(row.get("segment_key") or row.get("segment") or row.get("tag"),"segment"); val=clean(row.get("segment_value") or row.get("segment_name") or row.get("tag_value") or row.get("segment"),"unknown")
  c=campaigns[cid]; c["sent"]+=sent; c["opens"]+=opens; s=c["segments"][(key,val)]; s["sent"]+=sent; s["opens"]+=opens
 findings=[]
 for cid,c in campaigns.items():
  baseline=c["opens"]/c["sent"] if c["sent"] else 0.0
  for (key,val),s in c["segments"].items():
   if s["sent"]<min_segment_size: continue
   rate=s["opens"]/s["sent"] if s["sent"] else 0.0; delta=rate-baseline; dirn="high" if delta>0 else "low"
   if abs(delta)>=min_delta_rate and (direction=="both" or direction==dirn):
    findings.append({"campaign_id":cid,"segment_key":key,"segment_value":val,"sent_count":s["sent"],"open_rate":round(rate,4),"baseline_open_rate":round(baseline,4),"delta_rate":round(delta,4),"direction":dirn})
 findings.sort(key=lambda i:(-abs(i["delta_rate"]),i["campaign_id"],i["segment_key"],i["segment_value"])); findings=findings[:limit]
 return {"artifact_type":ARTIFACT_TYPE,"generated_at":gen.isoformat(),"filters":{"lookback_days":lookback_days,"min_segment_size":min_segment_size,"min_delta_rate":min_delta_rate,"direction":direction,"limit":limit},"summary":{"campaigns_scanned":len(campaigns),"outlier_count":len(findings)},"outliers":findings,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":empty_state(findings,"No newsletter open-rate segment outliers found.",schema_gap=bool(missing_tables or missing_columns))}
def build_newsletter_open_rate_segment_outliers_report_from_db(db_or_conn:Any,**kwargs:Any)->dict[str,Any]:
 conn=connection(db_or_conn); sch=schema(conn); table=next((t for t in ("newsletter_segment_metrics","newsletter_audience_segments") if t in sch),None)
 if not table: return build_newsletter_open_rate_segment_outliers_report([],missing_tables=["newsletter_segment_metrics"],**kwargs)
 rows=load_table(conn,table,sch[table],{"campaign_id":("campaign_id","newsletter_id"),"segment_key":("segment_key","segment","tag"),"segment_value":("segment_value","segment_name","tag_value","segment"),"sent_count":("sent_count","sent"),"open_count":("open_count","opens","opened_count"),"sent_at":("sent_at","created_at")})
 return build_newsletter_open_rate_segment_outliers_report(rows,**kwargs)
def format_newsletter_open_rate_segment_outliers_json(report:dict[str,Any])->str: return json_dumps(report)
def format_newsletter_open_rate_segment_outliers_text(report:dict[str,Any])->str:
 lines=["Newsletter Open Rate Segment Outliers",f"Generated: {report['generated_at']}",f"Totals: campaigns={report['summary']['campaigns_scanned']} outliers={report['summary']['outlier_count']}"]
 if not report["outliers"]: lines.append(report["empty_state"]["message"]); return "\n".join(lines)
 lines+=["","campaign_id | segment | sent | open_rate | baseline | delta | direction"]+[f"{i['campaign_id']} | {i['segment_key']}={i['segment_value']} | {i['sent_count']} | {i['open_rate']} | {i['baseline_open_rate']} | {i['delta_rate']} | {i['direction']}" for i in report["outliers"]]
 return "\n".join(lines)
