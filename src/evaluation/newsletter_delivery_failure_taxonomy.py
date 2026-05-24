"""Classify newsletter delivery failures into deterministic taxonomy buckets."""
from __future__ import annotations
from collections import Counter,defaultdict
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE="newsletter_delivery_failure_taxonomy"; DEFAULT_LIMIT=50
CATS={"bounce":["bounce","bounced","mailbox","recipient"],"suppression":["suppression","suppressed","blocked","unsubscribe"],"provider_reject":["reject","denied","invalid","policy","spam"],"timeout":["timeout","timed out","deadline"]}
def _cat(r):
    text=" ".join(lower(r.get(k)) for k in ("status","error_code","error_message","provider_error"))
    for c,terms in CATS.items():
        if any(t in text for t in terms): return c
    return "unknown"
def build_newsletter_delivery_failure_taxonomy_report(rows:list[dict[str,Any]],*,limit:int=DEFAULT_LIMIT,missing_tables=None,missing_columns=None,now=None)->dict[str,Any]:
    positive("limit",limit); cats=Counter(); providers=defaultdict(Counter); issues=defaultdict(Counter); findings=[]
    for i,r in enumerate(rows):
        status=lower(r.get("status")); msg=clean(r.get("error_message") or r.get("provider_error")); code=clean(r.get("error_code"))
        if status in {"sent","delivered","success","opened","clicked"} and not msg and not code: continue
        c=_cat(r); cats[c]+=1; provider=clean(r.get("provider"),"unknown"); issue=clean(r.get("issue_id"),"unknown")
        providers[provider][c]+=1; issues[issue][c]+=1
        findings.append({"event_id":r.get("event_id") or r.get("id") or i+1,"provider":provider,"issue_id":issue,"subscriber_id":r.get("subscriber_id"),"category":c,"status":status or None,"error_code":code or None,"snippet":msg[:120] or None,"occurred_at":(dt(r.get("occurred_at")) or dt(r.get("created_at")) or None).isoformat() if (dt(r.get("occurred_at")) or dt(r.get("created_at"))) else None})
    findings.sort(key=lambda f:(f["category"],f["provider"],f["occurred_at"] or "",str(f["event_id"])))
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(now),"filters":{"limit":limit},"totals":{"events":len(rows),"failures":len(findings),"by_category":dict(sorted(cats.items()))},"provider_breakdown":[{"provider":p,"total":sum(c.values()),"by_category":dict(sorted(c.items()))} for p,c in sorted(providers.items())],"issue_breakdown":[{"issue_id":i,"total":sum(c.values()),"by_category":dict(sorted(c.items()))} for i,c in sorted(issues.items())],"findings":findings[:limit],"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":empty_state(findings,"No newsletter delivery failures found.",schema_gap=bool(missing_tables or missing_columns))}
def build_newsletter_delivery_failure_taxonomy_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); mt=[] if "newsletter_delivery_events" in s else ["newsletter_delivery_events"]; mc={}; rows=[]
    if not mt:
        cols=s["newsletter_delivery_events"]; rows=load_table(conn,"newsletter_delivery_events",cols,{"event_id":("id","event_id"),"provider":("provider",),"issue_id":("issue_id","newsletter_issue_id"),"subscriber_id":("subscriber_id",),"status":("status",),"error_code":("error_code",),"error_message":("error_message","message"),"provider_error":("provider_error","payload","error_payload"),"occurred_at":("occurred_at","created_at")})
    return build_newsletter_delivery_failure_taxonomy_report(rows,missing_tables=mt,missing_columns=mc,**kw)
def format_newsletter_delivery_failure_taxonomy_json(r): return json_dumps(r)
def format_newsletter_delivery_failure_taxonomy_text(r):
    lines=["Newsletter Delivery Failure Taxonomy",f"Generated: {r['generated_at']}",f"Totals: events={r['totals']['events']} failures={r['totals']['failures']}"]
    if r['missing_tables']: lines.append('Missing tables: '+', '.join(r['missing_tables']))
    if not r['findings']: lines.append(r['empty_state']['message']); return '\n'.join(lines)
    lines.append('Category totals: '+', '.join(f"{k}={v}" for k,v in r['totals']['by_category'].items()))
    lines += ['', 'provider | issue_id | category | status | error_code']
    for f in r['findings']: lines.append(f"{f['provider']} | {f['issue_id']} | {f['category']} | {f['status'] or '-'} | {f['error_code'] or '-'}")
    return '\n'.join(lines)
