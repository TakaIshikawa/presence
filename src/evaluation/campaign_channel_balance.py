"""Find campaigns overconcentrated in one generated content channel."""
from __future__ import annotations
from collections import Counter,defaultdict
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE="campaign_channel_balance"; DEFAULT_DAYS=30; DEFAULT_LIMIT=50; DEFAULT_MAX_CHANNEL_SHARE=.7; CHANNELS=("x_post","x_thread","newsletter","blog_post")
def _campaign(r):
 text=clean(r.get("campaign") or r.get("campaign_id")); meta=clean(r.get("metadata"))
 if text: return text
 try: return clean(json.loads(meta).get("campaign") or json.loads(meta).get("campaign_id"),"uncategorized")
 except Exception: return "uncategorized"
def build_campaign_channel_balance_report(rows:list[dict[str,Any]],*,days:int=DEFAULT_DAYS,campaign:str|None=None,limit:int=DEFAULT_LIMIT,max_channel_share:float=DEFAULT_MAX_CHANNEL_SHARE,missing_tables=None,missing_columns=None,now=None):
 positive("days",days); positive("limit",limit); bounded_share("max_channel_share",max_channel_share); buckets=defaultdict(Counter)
 for r in rows:
  camp=_campaign(r); 
  if campaign and camp!=campaign: continue
  buckets[camp][clean(r.get("content_type"),"unknown")]+=1
 findings=[]
 for camp,counts in buckets.items():
  total=sum(counts.values()); ch,count=counts.most_common(1)[0]; share=round(count/total,4); missing=[c for c in CHANNELS if counts.get(c,0)==0]
  if share>max_channel_share: findings.append({"campaign":camp,"total_count":total,"channel_counts":dict(sorted(counts.items())),"dominant_channel":ch,"dominant_share":share,"recommended_missing_channels":missing})
 findings.sort(key=lambda f:(-f["dominant_share"],-f["total_count"],f["campaign"]))
 return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(now),"filters":{"days":days,"campaign":campaign,"limit":limit,"max_channel_share":max_channel_share},"summary":{"campaign_count":len(buckets),"finding_count":len(findings)},"findings":findings[:limit],"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":empty_state(findings,"No campaign channel balance issues found.",schema_gap=bool(missing_tables or missing_columns))}
def build_campaign_channel_balance_report_from_db(db_or_conn:Any,**kw):
 conn=connection(db_or_conn); s=schema(conn)
 if "generated_content" not in s: return build_campaign_channel_balance_report([],missing_tables=["generated_content"],**kw)
 c=s["generated_content"]; miss=[] if "content_type" in c else ["content_type"]
 if not ({"campaign","campaign_id","metadata"}&c): miss.append("campaign|campaign_id|metadata")
 if miss: return build_campaign_channel_balance_report([],missing_columns={"generated_content":miss},**kw)
 where=[]; params=[]; days=kw.get("days",DEFAULT_DAYS)
 if "created_at" in c: where.append("(created_at IS NULL OR created_at >= ?)"); params.append((now_value(kw.get("now"))-timedelta(days=days)).isoformat())
 q=f"SELECT {pick(c,'content_type',out='content_type')}, {pick(c,'campaign','campaign_id',out='campaign')}, {pick(c,'metadata','meta_json',out='metadata')} FROM generated_content"+((" WHERE "+" AND ".join(where)) if where else "")+" ORDER BY rowid"
 return build_campaign_channel_balance_report([dict(r) for r in conn.execute(q,params)],**kw)
def format_campaign_channel_balance_json(r): return json_dumps(r)
def format_campaign_channel_balance_text(r):
 lines=["Campaign Channel Balance",f"Artifact: {r['artifact_type']}",f"Generated: {r['generated_at']}",f"Totals: campaigns={r['summary']['campaign_count']} findings={r['summary']['finding_count']}"]
 for f in r["findings"]: lines.append(f"- {f['campaign']}: {f['dominant_channel']} {f['dominant_share']}")
 return "\n".join(lines)
