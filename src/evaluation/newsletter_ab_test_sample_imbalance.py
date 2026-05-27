"""Detect newsletter A/B test sample imbalance and missing outcomes."""
from __future__ import annotations
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE="newsletter_ab_test_sample_imbalance"; DEFAULT_MAX_IMBALANCE_RATIO=1.5; DEFAULT_WINDOW_DAYS=30; DEFAULT_LIMIT=100
def build_newsletter_ab_test_sample_imbalance_report(rows:list[dict[str,Any]],*,max_imbalance_ratio:float=DEFAULT_MAX_IMBALANCE_RATIO,window_days:int=DEFAULT_WINDOW_DAYS,limit:int=DEFAULT_LIMIT,now=None,missing_tables=None,missing_columns=None):
    positive("max_imbalance_ratio",max_imbalance_ratio); positive("window_days",window_days); positive("limit",limit)
    gen=now_value(now); cutoff=gen-timedelta(days=window_days); groups=defaultdict(list)
    for r in rows:
        created=dt(r.get("created_at") or r.get("sent_at"))
        if created and created<cutoff: continue
        exp=clean(r.get("experiment_id") or r.get("campaign_id") or _meta(r).get("experiment_id"))
        var=clean(r.get("variant") or r.get("variant_id") or _meta(r).get("variant"))
        if exp and var: groups[exp].append({**r,"_variant":var})
    findings=[]
    for exp,items in groups.items():
        if len({i["_variant"] for i in items})<2: continue
        total=sum(max(0,to_int(i.get("sample_size") or i.get("audience_size") or _meta(i).get("sample_size"),0)) for i in items); expected=round(1/len(items),4) if items else 0
        sizes=[max(0,to_int(i.get("sample_size") or i.get("audience_size") or _meta(i).get("sample_size"),0)) for i in items]
        ratio=(max(sizes)/max(1,min([s for s in sizes if s>0] or [1]))) if sizes else 1
        for i,size in zip(items,sizes):
            has_out=any(to_float(i.get(k) or _meta(i).get(k),0)>0 for k in ("opens","clicks","conversions","sent_count","open_rate","click_rate"))
            if ratio>max_imbalance_ratio:
                findings.append({"experiment_id":exp,"variant":i["_variant"],"sample_size":size,"expected_share":expected,"actual_share":round(size/max(1,total),4),"issue_type":"sample_imbalance"})
            if any(any(to_float(j.get(k) or _meta(j).get(k),0)>0 for k in ("opens","clicks","conversions","sent_count","open_rate","click_rate")) for j in items) and not has_out:
                findings.append({"experiment_id":exp,"variant":i["_variant"],"sample_size":size,"expected_share":expected,"actual_share":round(size/max(1,total),4),"issue_type":"missing_outcome_metrics"})
    findings.sort(key=lambda f:(f["experiment_id"],f["issue_type"],f["variant"]))
    shown=findings[:limit]
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":gen.isoformat(),"filters":{"max_imbalance_ratio":max_imbalance_ratio,"window_days":window_days,"limit":limit},"summary":{"experiment_count":len(groups),"finding_count":len(findings),"shown_count":len(shown)},"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items()) if v},"empty_state":empty_state(findings,"No newsletter A/B sample imbalance found.",schema_gap=bool(missing_tables or missing_columns))}
def build_newsletter_ab_test_sample_imbalance_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); table="newsletter_subject_candidates" if "newsletter_subject_candidates" in s else ("newsletter_campaigns" if "newsletter_campaigns" in s else None); mt=[]; rows=[]
    if not table: mt.append("newsletter_subject_candidates|newsletter_campaigns")
    else: rows=load_table(conn,table,s[table],{"experiment_id":("experiment_id","ab_test_id","campaign_id"),"variant":("variant","variant_id","subject_variant"),"sample_size":("sample_size","audience_size","recipient_count"),"opens":("opens","open_count"),"clicks":("clicks","click_count"),"open_rate":("open_rate",),"metadata":("metadata","metrics_metadata"),"created_at":("created_at","sent_at")})
    return build_newsletter_ab_test_sample_imbalance_report(rows,missing_tables=mt,**kw)
def _meta(r):
    try: return json.loads(clean(r.get("metadata"))) if clean(r.get("metadata")) else {}
    except json.JSONDecodeError: return {}
def format_newsletter_ab_test_sample_imbalance_json(r): return json_dumps(r)
def format_newsletter_ab_test_sample_imbalance_text(r):
    lines=["Newsletter AB Test Sample Imbalance",f"Generated: {r['generated_at']}",f"Totals: experiments={r['summary']['experiment_count']} findings={r['summary']['finding_count']} shown={r['summary']['shown_count']}"]
    if not r["findings"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    for f in r["findings"]: lines.append(f"  - experiment={f['experiment_id']} variant={f['variant']} issue={f['issue_type']} sample={f['sample_size']}")
    return "\n".join(lines)
