"""Flag risky redirect chains for successful publication attempts."""
from __future__ import annotations
from collections import Counter,defaultdict
from typing import Any
from urllib.parse import urlparse
from ._report_utils import clean,connection,expr,json_dumps,lower,now_iso,positive,schema,to_int
ARTIFACT_TYPE="publication_attempt_redirect_chain_risk"; DEFAULT_MAX_HOPS=3; DEFAULT_LIMIT=50; SHORT={"bit.ly","t.co","tinyurl.com","goo.gl","ow.ly"}
def build_publication_attempt_redirect_chain_risk_report(rows:list[dict[str,Any]],*,max_hops:int=DEFAULT_MAX_HOPS,limit:int=DEFAULT_LIMIT,missing_tables:list[str]|None=None,missing_columns:dict[str,list[str]]|None=None,now:Any=None)->dict[str,Any]:
 positive("max_hops",max_hops); positive("limit",limit); findings=[]; platforms=defaultdict(Counter); maxh=defaultdict(int)
 for r in rows:
  if lower(r.get("status")) not in {"success","succeeded","published","ok"}: continue
  reasons=[]; url=clean(r.get("url")); final=clean(r.get("final_url")); hops=to_int(r.get("redirect_hops")) or 0; od=_host(url); fd=_host(final)
  if hops>max_hops: reasons.append("too_many_hops")
  if not final: reasons.append("missing_final_url")
  if od and fd and od!=fd: reasons.append("final_domain_mismatch")
  if url.startswith("https://") and final.startswith("http://"): reasons.append("http_downgrade")
  if od in SHORT or fd in SHORT: reasons.append("shortened_link_chain")
  plat=clean(r.get("platform"),"unknown"); maxh[plat]=max(maxh[plat],hops)
  for reason in reasons: platforms[plat][reason]+=1
  if reasons: findings.append({"platform":plat,"content_id":clean(r.get("content_id"),"unknown"),"url":url,"final_url":final or None,"redirect_hops":hops,"checked_at":clean(r.get("checked_at")) or None,"reasons":reasons,"severity":len(reasons)+max(0,hops-max_hops)})
 findings.sort(key=lambda f:(-f["severity"],-(f["redirect_hops"] or 0),f["checked_at"] or "",f["platform"],f["content_id"]))
 shown=findings[:limit]
 return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(now),"filters":{"max_hops":max_hops,"limit":limit},"totals":{"attempt_count":len(rows),"finding_count":len(findings),"shown_findings":len(shown)},"platform_totals":[{"platform":p,"finding_count":sum(c.values()),"by_reason":dict(sorted(c.items()))} for p,c in sorted(platforms.items())],"max_hop_summaries":[{"platform":p,"max_redirect_hops":h} for p,h in sorted(maxh.items())],"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":{"is_empty":not findings,"message":"No publication attempt redirect chain risks found." if not findings else None}}
def build_publication_attempt_redirect_chain_risk_report_from_db(db_or_conn:Any,**kwargs:Any)->dict[str,Any]:
 conn=connection(db_or_conn); s=schema(conn); mt=[] if "publication_attempts" in s else ["publication_attempts"]; mc={}
 return build_publication_attempt_redirect_chain_risk_report(_load(conn,s,mc) if "publication_attempts" in s else [],missing_tables=mt,missing_columns=mc,**kwargs)
def format_publication_attempt_redirect_chain_risk_json(report:dict[str,Any])->str: return json_dumps(report)
def format_publication_attempt_redirect_chain_risk_text(report:dict[str,Any])->str:
 lines=["Publication Attempt Redirect Chain Risk",f"Generated: {report['generated_at']}",f"Totals: attempts={report['totals']['attempt_count']} findings={report['totals']['finding_count']} shown={report['totals']['shown_findings']}"]
 if report["missing_tables"]: lines.append("Missing tables: "+", ".join(report["missing_tables"]))
 if not report["findings"]: lines.append(report["empty_state"]["message"] or "No findings."); return "\n".join(lines)
 lines+=["","platform | content_id | hops | reasons | url -> final_url"]
 for f in report["findings"]: lines.append(f"{f['platform']} | {f['content_id']} | {f['redirect_hops']} | {','.join(f['reasons'])} | {f['url']} -> {f['final_url'] or '-'}")
 return "\n".join(lines)
def _load(conn:Any,s:dict[str,set[str]],mc:dict[str,list[str]])->list[dict[str,Any]]:
 pa=s["publication_attempts"]; extra=None
 for t in ("url_check_results","link_check_results"):
  if t in s: extra=t; break
 select=[expr(pa,"platform",default="'unknown'",alias="pa",out="platform"),expr(pa,"content_id",alias="pa",out="content_id"),expr(pa,"url","published_url",alias="pa",out="url"),expr(pa,"status",default="'success'",alias="pa",out="status")]
 if extra:
  ec=s[extra]; join=f" LEFT JOIN {extra} u ON "+("u.attempt_id=pa.id" if "attempt_id" in ec and "id" in pa else "u.url=pa.url" if "url" in ec and "url" in pa else "1=0")
  select += [expr(ec,"final_url",alias="u",out="final_url"),expr(ec,"redirect_hops","hops",default="0",alias="u",out="redirect_hops"),expr(ec,"checked_at","created_at",alias="u",out="checked_at")]
 else:
  join=""; select += [expr(pa,"final_url",alias="pa",out="final_url"),expr(pa,"redirect_hops","hops",default="0",alias="pa",out="redirect_hops"),expr(pa,"checked_at","created_at",alias="pa",out="checked_at")]
 return [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM publication_attempts pa{join} ORDER BY pa.rowid")]
def _host(url:str)->str:
 return (urlparse(url if "://" in url else "http://"+url).hostname or "").lower().removeprefix("www.") if url else ""
