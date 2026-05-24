"""Flag broken or stale external links in blog drafts."""
from __future__ import annotations
from datetime import timedelta
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE="blog_draft_external_link_rot"; DEFAULT_LIMIT=50; DEFAULT_STALE_DAYS=30; URL_RE=re.compile(r"https?://[^\s)'\"<>]+")
BAD={404,410,500,502,503,504}
def build_blog_draft_external_link_rot_report(drafts:list[dict[str,Any]],snapshots:list[dict[str,Any]]|None=None,*,limit:int=DEFAULT_LIMIT,stale_days:int=DEFAULT_STALE_DAYS,missing_tables=None,missing_columns=None,now=None):
    positive("limit",limit); positive("stale_days",stale_days); gen=now_value(now); latest={}
    for s in snapshots or []:
        u=clean(s.get("url")); checked=dt(s.get("checked_at") or s.get("latest_checked_at") or s.get("created_at"))
        if u and (u not in latest or (checked or gen)>(dt(latest[u].get("checked_at") or latest[u].get("latest_checked_at") or latest[u].get("created_at")) or gen)): latest[u]=s
    findings=[]
    for d in drafts:
        urls=d.get("urls") or URL_RE.findall(clean(d.get("body") or d.get("content") or d.get("markdown")))
        for u in urls:
            if domain(u) in {"","example.com"}: continue
            s=latest.get(u,{})
            code=to_int(s.get("http_code") or s.get("status_code"),0); status=lower(s.get("status"),"unknown")
            checked=dt(s.get("checked_at") or s.get("latest_checked_at") or s.get("created_at")); age=(gen-checked).days if checked else None
            reasons=[]
            if code in BAD or status in {"broken","dead","failed","error"}: reasons.append("broken")
            elif 300<=code<400 or status in {"redirected","redirect"}: reasons.append("redirected")
            if age is None or age>stale_days: reasons.append("stale")
            if reasons:
                sev=3 if "broken" in reasons else (2 if "redirected" in reasons else 1)
                findings.append({"draft_id":d.get("draft_id") or d.get("id"),"draft_title":d.get("title"),"url":u,"host":domain(u),"status":status,"http_code":code or None,"latest_checked_at":checked.isoformat() if checked else None,"evidence_source":s.get("evidence_source") or s.get("source") or ("content_extract" if not s else "link_snapshot"),"severity":sev,"recommended_action":"replace or remove broken destination" if "broken" in reasons else ("verify redirected destination" if "redirected" in reasons else "refresh link crawl")})
    findings.sort(key=lambda f:(-f["severity"],f["latest_checked_at"] or "",f["host"],f["url"]))
    shown=findings[:limit]
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":gen.isoformat(),"filters":{"limit":limit,"stale_days":stale_days},"summary":{"draft_count":len(drafts),"broken_link_count":len(findings),"shown":len(shown)},"broken_links":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":empty_state(findings,"No broken or stale blog draft external links found.",schema_gap=bool(missing_tables or missing_columns))}
def build_blog_draft_external_link_rot_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); mt=[]; mc={}; drafts=[]; snaps=[]
    table="blog_drafts" if "blog_drafts" in s else ("blog_posts" if "blog_posts" in s else None)
    if not table: mt.append("blog_drafts|blog_posts")
    else:
        c=s[table]
        if not ({"body","content","markdown"} & c): mc[table]=["body|content|markdown"]
        drafts=load_table(conn,table,c,{"draft_id":("id","draft_id"),"title":("title","headline"),"body":("body","content","markdown")}) if table not in mc else []
    for t in ("blog_link_snapshots","blog_search_index_snapshots","curated_source_health_snapshots","link_snapshots"):
        if t in s:
            snaps+=load_table(conn,t,s[t],{"url":("url","source_url"),"status":("status","state"),"http_code":("http_code","status_code"),"checked_at":("checked_at","latest_checked_at","created_at"),"evidence_source":("evidence_source","source")})
    return build_blog_draft_external_link_rot_report(drafts,snaps,missing_tables=mt,missing_columns=mc,**kw)
def format_blog_draft_external_link_rot_json(r): return json_dumps(r)
def format_blog_draft_external_link_rot_text(r):
    s=r["summary"]; lines=["Blog Draft External Link Rot",f"Generated: {r['generated_at']}",f"Totals: drafts={s['draft_count']} broken_links={s['broken_link_count']} shown={s['shown']}"]
    if r["missing_tables"]: lines.append("Missing tables: "+", ".join(r["missing_tables"]))
    if r["missing_columns"]: lines.append("Missing columns: "+flatten_missing(r["missing_columns"]))
    if not r["broken_links"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    lines+=["","draft_id | status | code | url"]
    for f in r["broken_links"]: lines.append(f"{f['draft_id']} | {f['status']} | {f['http_code']} | {f['url']}")
    return "\n".join(lines)
