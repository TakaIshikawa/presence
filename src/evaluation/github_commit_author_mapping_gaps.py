"""Group GitHub commits whose authors are not mapped to known people/accounts."""
from __future__ import annotations
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE="github_commit_author_mapping_gaps"; DEFAULT_LIMIT=50
def build_github_commit_author_mapping_gaps_report(rows:list[dict[str,Any]],*,repo:str|None=None,since:str|None=None,limit:int=DEFAULT_LIMIT,missing_tables=None,missing_columns=None,now=None):
    positive("limit",limit); rf=lower(repo); since_dt=dt(since); groups={}
    for r in rows:
        if rf and lower(r.get("repo"))!=rf: continue
        seen=dt(r.get("authored_at") or r.get("committed_at"))
        if since_dt and seen and seen<since_dt: continue
        if clean(r.get("person_id") or r.get("mapped_author_id") or r.get("account_id")): continue
        key=(clean(r.get("repo")),lower(r.get("author_login") or r.get("author_email") or r.get("author_name"),"unknown")); g=groups.setdefault(key,{"repo":key[0],"author_login":clean(r.get("author_login")) or None,"author_email":clean(r.get("author_email")) or None,"author_name":clean(r.get("author_name")) or None,"commit_count":0,"first_seen":None,"last_seen":None,"sample_shas":[]})
        g["commit_count"]+=1
        if len(g["sample_shas"])<5: g["sample_shas"].append(clean(r.get("sha") or r.get("commit_sha")))
        iso=seen.isoformat() if seen else None
        if iso and (not g["first_seen"] or iso<g["first_seen"]): g["first_seen"]=iso
        if iso and (not g["last_seen"] or iso>g["last_seen"]): g["last_seen"]=iso
    findings=sorted(groups.values(),key=lambda g:(-g["commit_count"],g["repo"],g["author_email"] or g["author_login"] or ""))[:limit]
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(now),"filters":{"repo":repo,"since":since,"limit":limit},"totals":{"commits":len(rows),"groups":len(groups),"shown":len(findings)},"findings":findings,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":empty_state(findings,"No GitHub commit author mapping gaps found.",schema_gap=bool(missing_tables or missing_columns))}
def build_github_commit_author_mapping_gaps_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); table="github_commits" if "github_commits" in s else None
    if not table: return build_github_commit_author_mapping_gaps_report([],missing_tables=["github_commits"],**kw)
    rows=load_table(conn,table,s[table],{"repo":("repo","repository"),"sha":("sha","commit_sha"),"author_login":("author_login",),"author_email":("author_email","email"),"author_name":("author_name","name"),"person_id":("person_id","mapped_author_id","account_id"),"authored_at":("authored_at","committed_at","created_at")})
    return build_github_commit_author_mapping_gaps_report(rows,**kw)
def format_github_commit_author_mapping_gaps_json(r): return json_dumps(r)
def format_github_commit_author_mapping_gaps_text(r):
    lines=["GitHub Commit Author Mapping Gaps",f"Generated: {r['generated_at']}",f"Totals: commits={r['totals']['commits']} groups={r['totals']['groups']} shown={r['totals']['shown']}"]
    if not r["findings"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    lines+=["","repo | author | commits | first_seen | last_seen"]
    for f in r["findings"]: lines.append(f"{f['repo']} | {f['author_email'] or f['author_login'] or f['author_name']} | {f['commit_count']} | {f['first_seen']} | {f['last_seen']}")
    return "\n".join(lines)
