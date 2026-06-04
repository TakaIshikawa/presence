"""Detect concentration of recent knowledge sources by domain."""
from __future__ import annotations
from collections import Counter,defaultdict
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE="knowledge_source_domain_concentration"; DEFAULT_LOOKBACK_DAYS=30; DEFAULT_MAX_DOMAIN_SHARE=0.5; DEFAULT_MIN_SOURCES=3; DEFAULT_LIMIT=50
def build_knowledge_source_domain_concentration_report(rows:list[dict[str,Any]],*,lookback_days:int=DEFAULT_LOOKBACK_DAYS,max_domain_share:float=DEFAULT_MAX_DOMAIN_SHARE,min_sources:int=DEFAULT_MIN_SOURCES,limit:int=DEFAULT_LIMIT,missing_tables:list[str]|None=None,missing_columns:dict[str,list[str]]|None=None,now=None)->dict[str,Any]:
    positive("lookback_days",lookback_days); bounded_share("max_domain_share",max_domain_share); positive("min_sources",min_sources); positive("limit",limit)
    gen=now_value(now); cutoff=gen-timedelta(days=lookback_days); counts=Counter(); reps=defaultdict(list); total=0
    for row in rows:
        created=dt(row.get("created_at") or row.get("captured_at") or row.get("updated_at"))
        if created and created<cutoff: continue
        d=domain(row.get("url") or row.get("source_url")) or "unknown_domain"
        if "." not in d: d="unknown_domain"
        counts[d]+=1; total+=1
        if len(reps[d])<5: reps[d].append({"source_id":clean(row.get("id") or row.get("source_id")),"url":clean(row.get("url") or row.get("source_url"))})
    findings=[{"domain":d,"domain_count":c,"total_count":total,"share":round(c/total,4),"representative_sources":reps[d]} for d,c in counts.items() if total>=min_sources and c/total>max_domain_share]
    findings.sort(key=lambda i:(-i["share"],-i["domain_count"],i["domain"]))
    findings=findings[:limit]
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":gen.isoformat(),"filters":{"lookback_days":lookback_days,"max_domain_share":max_domain_share,"min_sources":min_sources,"limit":limit},"summary":{"source_count":total,"domain_count":len(counts),"concentration_count":len(findings)},"concentrations":findings,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":empty_state(findings,"No knowledge source domain concentration found.",schema_gap=bool(missing_tables or missing_columns))}
def build_knowledge_source_domain_concentration_report_from_db(db_or_conn:Any,**kwargs:Any)->dict[str,Any]:
    conn=connection(db_or_conn); sch=schema(conn); table=next((t for t in ("knowledge_sources","knowledge_items","sources") if t in sch),None)
    if not table: return build_knowledge_source_domain_concentration_report([],missing_tables=["knowledge_sources"],**kwargs)
    rows=load_table(conn,table,sch[table],{"id":("id","source_id"),"url":("url","source_url"),"created_at":("created_at","captured_at","updated_at")})
    return build_knowledge_source_domain_concentration_report(rows,**kwargs)
def format_knowledge_source_domain_concentration_json(report:dict[str,Any])->str: return json_dumps(report)
def format_knowledge_source_domain_concentration_text(report:dict[str,Any])->str:
    lines=["Knowledge Source Domain Concentration",f"Generated: {report['generated_at']}",f"Totals: sources={report['summary']['source_count']} domains={report['summary']['domain_count']} findings={report['summary']['concentration_count']}"]
    if not report["concentrations"]: lines.append(report["empty_state"]["message"]); return "\n".join(lines)
    lines+=["","domain | count | total | share"]+[f"{i['domain']} | {i['domain_count']} | {i['total_count']} | {i['share']}" for i in report["concentrations"]]
    return "\n".join(lines)
