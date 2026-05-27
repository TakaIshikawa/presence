"""Scan newsletters for spam trigger density and punctuation/caps abuse."""
from __future__ import annotations
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE="newsletter_spam_trigger_density"; DEFAULT_MAX_TRIGGER_DENSITY=0.03; DEFAULT_MAX_EXCLAMATION_COUNT=3; DEFAULT_LIMIT=100
TRIGGERS=("free money","guaranteed","act now","limited time","risk free","winner","buy now","urgent","cash bonus","click here")
def build_newsletter_spam_trigger_density_report(rows:list[dict[str,Any]],*,max_trigger_density:float=DEFAULT_MAX_TRIGGER_DENSITY,max_exclamation_count:int=DEFAULT_MAX_EXCLAMATION_COUNT,issue_id=None,limit:int=DEFAULT_LIMIT,now=None,missing_tables=None,missing_columns=None):
    bounded_share("max_trigger_density",max_trigger_density); positive("max_exclamation_count",max_exclamation_count); positive("limit",limit)
    gen=now_value(now); findings=[]
    for r in rows:
        iid=r.get("issue_id") or r.get("id")
        if issue_id is not None and str(iid)!=str(issue_id): continue
        for field in ("subject","preheader","body"):
            text=clean(r.get(field) or _meta(r).get(field)); words=max(1,len(re.findall(r"\w+",text)))
            lower_text=text.lower()
            for trig in TRIGGERS:
                count=lower_text.count(trig); density=round(count/words,4)
                if count and density>max_trigger_density: findings.append({"issue_id":iid,"field":field,"trigger":trig,"density":density,"severity":3 if density>max_trigger_density*2 else 2})
            bangs=text.count("!")
            if bangs>max_exclamation_count: findings.append({"issue_id":iid,"field":field,"trigger":"exclamation_abuse","density":round(bangs/words,4),"severity":2})
            caps=sum(1 for w in re.findall(r"\b[A-Z]{4,}\b",text))
            if caps>=3: findings.append({"issue_id":iid,"field":field,"trigger":"caps_abuse","density":round(caps/words,4),"severity":2})
    findings.sort(key=lambda f:(-f["severity"],_sid(f["issue_id"]),f["field"],f["trigger"]))
    shown=findings[:limit]
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":gen.isoformat(),"filters":{"max_trigger_density":max_trigger_density,"max_exclamation_count":max_exclamation_count,"issue_id":issue_id,"limit":limit},"summary":{"issue_count":len(rows),"finding_count":len(findings),"shown_count":len(shown)},"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items()) if v},"empty_state":empty_state(findings,"No newsletter spam trigger density issues found.",schema_gap=bool(missing_tables or missing_columns))}
def build_newsletter_spam_trigger_density_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); table="newsletter_issues" if "newsletter_issues" in s else ("newsletter_drafts" if "newsletter_drafts" in s else None); mt=[]; rows=[]
    if not table: mt.append("newsletter_issues|newsletter_drafts")
    else: rows=load_table(conn,table,s[table],{"issue_id":("id","issue_id"),"subject":("subject","subject_line"),"preheader":("preheader","preview_text"),"body":("body","content","html"),"metadata":("metadata",)})
    return build_newsletter_spam_trigger_density_report(rows,missing_tables=mt,**kw)
def _meta(r):
    try:return json.loads(clean(r.get("metadata"))) if clean(r.get("metadata")) else {}
    except json.JSONDecodeError:return {}
def format_newsletter_spam_trigger_density_json(r): return json_dumps(r)
def format_newsletter_spam_trigger_density_text(r):
    lines=["Newsletter Spam Trigger Density",f"Generated: {r['generated_at']}",f"Totals: issues={r['summary']['issue_count']} findings={r['summary']['finding_count']} shown={r['summary']['shown_count']}"]
    if not r["findings"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    for f in r["findings"]: lines.append(f"  - issue={f['issue_id']} field={f['field']} trigger={f['trigger']} density={f['density']}")
    return "\n".join(lines)
def _sid(v):
    try:return(0,int(v))
    except(TypeError,ValueError):return(1,clean(v))
