"""Measure recovery time from failed publication attempts."""
from __future__ import annotations
from collections import Counter, defaultdict
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE="publication_provider_error_recovery_time"; DEFAULT_LIMIT=50; DEFAULT_MAX_HOURS=24.0; FAIL={"failed","failure","error","errored","timeout"}; OK={"success","succeeded","published","posted","sent"}
def build_publication_provider_error_recovery_time_report(rows:list[dict[str,Any]],incidents:list[dict[str,Any]]|None=None,*,limit:int=DEFAULT_LIMIT,max_hours:float=DEFAULT_MAX_HOURS,missing_tables=None,missing_columns=None,now=None):
    positive("limit",limit); positive("max_hours",max_hours); groups=defaultdict(list)
    for r in rows: groups[(clean(r.get("provider"),"unknown"),clean(r.get("platform"),"unknown"),clean(r.get("content_id") or r.get("content_key") or r.get("publication_id"),"unknown"))].append(r)
    findings=[]; unrecovered=0
    for (provider,platform,cid),items in groups.items():
        ordered=sorted(items,key=lambda r:dt(r.get("attempted_at") or r.get("created_at")) or now_value(None))
        failures=[]
        for r in ordered:
            status=lower(r.get("status"))
            ts=dt(r.get("attempted_at") or r.get("created_at"))
            if not ts: continue
            if status in FAIL: failures.append(r)
            elif status in OK and failures:
                for f in failures:
                    ft=dt(f.get("attempted_at") or f.get("created_at")); hrs=round((ts-ft).total_seconds()/3600,2) if ft else None
                    findings.append({"provider":provider,"platform":platform,"content_id":cid,"failed_at":ft.isoformat() if ft else None,"recovered_at":ts.isoformat(),"recovery_hours":hrs,"failure_reason":f.get("failure_reason") or f.get("error") or f.get("error_message"),"incident_overlap":_incident_overlap(provider,platform,ft,ts,incidents or []),"recommended_action":"review provider recovery playbook" if hrs and hrs>max_hours else "monitor recovered publication"})
                failures=[]
        unrecovered+=len(failures)
    findings.sort(key=lambda f:(-(f["recovery_hours"] or 0),f["provider"],f["platform"],str(f["content_id"])))
    shown=findings[:limit]; by=Counter(f"{f['provider']}:{f['platform']}" for f in findings)
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(now),"filters":{"limit":limit,"max_hours":max_hours},"summary":{"attempt_count":len(rows),"recovered_failure_count":len(findings),"unrecovered_failure_count":unrecovered,"shown":len(shown),"by_provider_platform":dict(sorted(by.items()))},"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":empty_state(findings,"No publication provider recovery findings found.",schema_gap=bool(missing_tables or missing_columns))}
def build_publication_provider_error_recovery_time_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); mt=[]; mc={}; rows=[]; inc=[]
    if "publication_attempts" not in s: mt.append("publication_attempts")
    else:
        c=s["publication_attempts"]; 
        for names,label in [({"provider","publication_provider"},"provider|publication_provider"),({"status","state"},"status|state"),({"attempted_at","created_at"},"attempted_at|created_at")]:
            if not names & c: mc.setdefault("publication_attempts",[]).append(label)
        rows=load_table(conn,"publication_attempts",c,{"provider":("provider","publication_provider"),"platform":("platform","channel"),"content_id":("content_id","content_key","publication_id"),"status":("status","state"),"attempted_at":("attempted_at","created_at"),"failure_reason":("failure_reason","error","error_message")}) if "publication_attempts" not in mc else []
    if "publication_provider_status_incidents" in s:
        inc=load_table(conn,"publication_provider_status_incidents",s["publication_provider_status_incidents"],{"provider":("provider","publication_provider"),"platform":("platform","channel"),"started_at":("started_at","start_at","created_at"),"ended_at":("ended_at","resolved_at")})
    return build_publication_provider_error_recovery_time_report(rows,inc,missing_tables=mt,missing_columns=mc,**kw)
def _incident_overlap(provider,platform,start,end,incidents):
    if not start or not end: return False
    for i in incidents:
        if clean(i.get("provider")) not in ("",provider) or clean(i.get("platform")) not in ("",platform): continue
        a=dt(i.get("started_at")); b=dt(i.get("ended_at")) or end
        if a and a<=end and b>=start: return True
    return False
def format_publication_provider_error_recovery_time_json(r): return json_dumps(r)
def format_publication_provider_error_recovery_time_text(r):
    s=r["summary"]; lines=["Publication Provider Error Recovery Time",f"Generated: {r['generated_at']}",f"Totals: attempts={s['attempt_count']} recovered={s['recovered_failure_count']} unrecovered={s['unrecovered_failure_count']}"]
    if r["missing_tables"]: lines.append("Missing tables: "+", ".join(r["missing_tables"]))
    if r["missing_columns"]: lines.append("Missing columns: "+flatten_missing(r["missing_columns"]))
    if not r["findings"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    lines+=["","provider | platform | content_id | recovery_hours"]
    for f in r["findings"]: lines.append(f"{f['provider']} | {f['platform']} | {f['content_id']} | {f['recovery_hours']}")
    return "\n".join(lines)
