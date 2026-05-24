"""Flag claim-check evidence concentrated in too few domains."""
from __future__ import annotations
from collections import Counter,defaultdict
from typing import Any
from urllib.parse import urlparse
import json,re
from ._report_utils import clean,connection,expr,json_dumps,now_iso,positive,schema
ARTIFACT_TYPE="content_claim_evidence_domain_concentration"; DEFAULT_MIN_CLAIMS=2; DEFAULT_MAX_DOMAIN_SHARE=0.6; DEFAULT_LIMIT=50; URL_RE=re.compile(r"\b(?:https?://|www\.)[^\s<>)\"']+",re.I)
def build_content_claim_evidence_domain_concentration_report(rows:list[dict[str,Any]],*,min_claims:int=DEFAULT_MIN_CLAIMS,max_domain_share:float=DEFAULT_MAX_DOMAIN_SHARE,limit:int=DEFAULT_LIMIT,missing_tables:list[str]|None=None,missing_columns:dict[str,list[str]]|None=None,now:Any=None)->dict[str,Any]:
 positive("min_claims",min_claims); positive("limit",limit)
 if not 0<max_domain_share<=1: raise ValueError("max_domain_share must be between 0 and 1")
 by_content=defaultdict(list); missing_url=0
 for r in rows:
  domains=_domains(r)
  if not domains: missing_url+=1
  by_content[clean(r.get("content_id"),"unknown")].append({"row":r,"domains":domains})
 summaries=[]; findings=[]
 for cid,items in by_content.items():
  if len(items)<min_claims: continue
  counts=Counter(d for it in items for d in set(it["domains"])); total_claims=len(items); top,top_count=counts.most_common(1)[0] if counts else ("",0); share=round(top_count/total_claims,4) if total_claims else 0
  summary={"content_id":cid,"claim_count":total_claims,"domain_count":len(counts),"top_domain":top or None,"top_domain_claim_count":top_count,"top_domain_share":share,"domains":dict(sorted(counts.items()))}
  summaries.append(summary)
  if counts and share>max_domain_share: findings.append({"issue_type":"single_domain_overuse",**summary,"severity":round(share*100+total_claims,2)})
  if counts and len(counts)<2 and total_claims>=min_claims: findings.append({"issue_type":"low_domain_diversity",**summary,"severity":round(50+total_claims,2)})
 findings.sort(key=lambda f:(-f["severity"],f["content_id"],f["issue_type"])); shown=findings[:limit]
 return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(now),"filters":{"min_claims":min_claims,"max_domain_share":max_domain_share,"limit":limit},"totals":{"claim_count":len(rows),"content_count":len(by_content),"missing_evidence_url_count":missing_url,"finding_count":len(findings),"shown_findings":len(shown)},"content_domain_summaries":sorted(summaries,key=lambda s:s["content_id"]),"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":{"is_empty":not findings,"message":"No content claim evidence domain concentration found." if not findings else None}}
def build_content_claim_evidence_domain_concentration_report_from_db(db_or_conn:Any,**kwargs:Any)->dict[str,Any]:
 conn=connection(db_or_conn); s=schema(conn)
 if "content_claim_checks" not in s: return build_content_claim_evidence_domain_concentration_report([],missing_tables=["content_claim_checks"],**kwargs)
 mc={}; return build_content_claim_evidence_domain_concentration_report(_load(conn,s["content_claim_checks"],mc),missing_columns=mc,**kwargs)
def format_content_claim_evidence_domain_concentration_json(report:dict[str,Any])->str: return json_dumps(report)
def format_content_claim_evidence_domain_concentration_text(report:dict[str,Any])->str:
 lines=["Content Claim Evidence Domain Concentration",f"Generated: {report['generated_at']}",f"Totals: claims={report['totals']['claim_count']} content={report['totals']['content_count']} findings={report['totals']['finding_count']} shown={report['totals']['shown_findings']}"]
 if report["missing_tables"]: lines.append("Missing tables: "+", ".join(report["missing_tables"]))
 if report["missing_columns"]: lines.append("Missing columns: "+str(report["missing_columns"]))
 if not report["findings"]: lines.append(report["empty_state"]["message"] or "No findings."); return "\n".join(lines)
 lines+=["","issue | content_id | claims | domains | top_domain | share"]
 for f in report["findings"]: lines.append(f"{f['issue_type']} | {f['content_id']} | {f['claim_count']} | {f['domain_count']} | {f['top_domain']} | {f['top_domain_share']}")
 return "\n".join(lines)
def _load(conn:Any,cols:set[str],mc:dict[str,list[str]])->list[dict[str,Any]]:
 if "content_id" not in cols: mc["content_claim_checks"]=["content_id"]; return []
 ev=[c for c in ("evidence_url","evidence_urls","evidence","metadata","result","annotation_text") if c in cols]
 if not ev: mc["content_claim_checks"]=["evidence_url"]
 select=[expr(cols,"id",out="claim_check_id"),"content_id"]+[expr(cols,c,out=c) for c in ev]
 return [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM content_claim_checks ORDER BY content_id,rowid")]
def _domains(row:dict[str,Any])->list[str]:
 urls=[]
 for key in ("evidence_url","evidence_urls","evidence","metadata","result","annotation_text"):
  val=row.get(key)
  if val is None: continue
  urls += URL_RE.findall(str(val))
  try:
   parsed=json.loads(str(val))
  except (TypeError,ValueError): parsed=None
  urls += _urls_json(parsed)
 return sorted({_domain(u) for u in urls if _domain(u)})
def _urls_json(v:Any)->list[str]:
 if isinstance(v,dict): return sum((_urls_json(x) for x in v.values()),[])
 if isinstance(v,list): return sum((_urls_json(x) for x in v),[])
 return URL_RE.findall(v) if isinstance(v,str) else []
def _domain(url:str)->str:
 host=urlparse(url if "://" in url else "http://"+url).hostname or ""
 host=host.lower().removeprefix("www.")
 return ".".join(host.split(".")[-2:]) if host.count(".")>=2 else host
