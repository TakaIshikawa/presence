"""Newsletter Delivery Failure Taxonomy."""
from __future__ import annotations

from collections import Counter,defaultdict
from typing import Any
from ._report_utils import clean,connection,expr,json_dumps,lower,now_iso,schema
ARTIFACT_TYPE='newsletter_delivery_failure_taxonomy'; DEFAULT_LIMIT=50
CATS=(('bounce',('bounce','bounced','hard_bounce','soft_bounce')),('suppression',('suppress','blocked','unsubscribe','complaint')),('provider_reject',('reject','denied','policy','invalid','spam')),('timeout',('timeout','timed out','deadline')),)
def _cat(r):
 text=' '.join(lower(r.get(k)) for k in ('status','error_code','error_message','provider_error','payload'))
 for c,needles in CATS:
  if any(n in text for n in needles): return c
 return 'unknown'
def build_newsletter_delivery_failure_taxonomy_report(rows:list[dict[str,Any]],*,limit:int=DEFAULT_LIMIT,missing_tables=None,missing_columns=None,now=None)->dict[str,Any]:
 if limit<=0: raise ValueError('limit must be positive')
 failures=[]; prov=defaultdict(Counter); issue=defaultdict(Counter); totals=Counter()
 for i,r in enumerate(rows):
  status=lower(r.get('status'))
  if status in {'sent','delivered','open','click','success'}: continue
  cat=_cat(r); provider=clean(r.get('provider'),'unknown').lower(); iid=clean(r.get('issue_id'),'unknown')
  totals[cat]+=1; prov[provider][cat]+=1; issue[iid][cat]+=1
  failures.append({'category':cat,'provider':provider,'issue_id':r.get('issue_id'),'subscriber_id':r.get('subscriber_id'),'occurred_at':clean(r.get('occurred_at')) or None,'status':clean(r.get('status')) or None,'error_code':clean(r.get('error_code')) or None,'severity':{'bounce':4,'suppression':3,'provider_reject':3,'timeout':2}.get(cat,1),'_i':i})
 failures.sort(key=lambda f:(-f['severity'],f['occurred_at'] or '',f['provider'],f['_i']))
 shown=[{k:v for k,v in f.items() if k!='_i'} for f in failures[:limit]]
 return {'artifact_type':ARTIFACT_TYPE,'generated_at':now_iso(now),'filters':{'limit':limit},'totals':{'failures':len(failures),'shown_findings':len(shown),'by_category':dict(sorted(totals.items()))},'provider_breakdown':[{'provider':p,'total':sum(c.values()),'by_category':dict(sorted(c.items()))} for p,c in sorted(prov.items())],'issue_breakdown':[{'issue_id':i,'total':sum(c.values()),'by_category':dict(sorted(c.items()))} for i,c in sorted(issue.items())],'findings':shown,'missing_tables':sorted(missing_tables or []),'missing_columns':{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},'empty_state':{'is_empty':not failures,'message':'No newsletter delivery failure taxonomy findings found.' if not failures else None}}
def build_newsletter_delivery_failure_taxonomy_report_from_db(db_or_conn:Any,**kw):
 conn=connection(db_or_conn); s=schema(conn)
 if 'newsletter_delivery_events' not in s: return build_newsletter_delivery_failure_taxonomy_report([],missing_tables=['newsletter_delivery_events'],**kw)
 c=s['newsletter_delivery_events']; sel=[expr(c,'status',default="'unknown'",out='status'),expr(c,'error_code',default='NULL',out='error_code'),expr(c,'error_message','provider_error','payload',default='NULL',out='error_message'),expr(c,'provider',default="'unknown'",out='provider'),expr(c,'issue_id','newsletter_issue_id',default='NULL',out='issue_id'),expr(c,'subscriber_id',default='NULL',out='subscriber_id'),expr(c,'occurred_at','created_at',default='NULL',out='occurred_at')]
 return build_newsletter_delivery_failure_taxonomy_report([dict(r) for r in conn.execute(f"SELECT {', '.join(sel)} FROM newsletter_delivery_events ORDER BY rowid")],**kw)
def format_newsletter_delivery_failure_taxonomy_json(r): return json_dumps(r)
def format_newsletter_delivery_failure_taxonomy_text(r):
 lines=['Newsletter Delivery Failure Taxonomy',f"Generated: {r['generated_at']}",f"Totals: failures={r['totals']['failures']} shown={r['totals']['shown_findings']}"]
 if r['missing_tables']: lines.append('Missing tables: '+', '.join(r['missing_tables']))
 if not r['findings']: lines.append(r['empty_state']['message'] or 'No findings.'); return '\n'.join(lines)
 lines+=['','category | provider | issue_id | subscriber_id | occurred_at']
 for f in r['findings']: lines.append(f"{f['category']} | {f['provider']} | {f['issue_id'] or '-'} | {f['subscriber_id'] or '-'} | {f['occurred_at'] or '-'}")
 return '\n'.join(lines)
