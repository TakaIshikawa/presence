"""Find canonical URL conflicts in publication records."""
from __future__ import annotations
from collections import Counter,defaultdict
from typing import Any
from urllib.parse import parse_qsl,urlsplit,urlunsplit,quote
from ._report_utils import clean,connection,expr,json_dumps,now_iso,positive,schema
ARTIFACT_TYPE="publication_canonical_conflicts"; DEFAULT_LIMIT=50; TRACKING={"utm_source","utm_medium","utm_campaign","utm_term","utm_content","fbclid","gclid"}
def normalize_url(url:Any)->str:
 text=clean(url)
 if not text: return ""
 parts=urlsplit(text if "://" in text else "https://"+text); netloc=parts.netloc.lower(); path=quote(parts.path.rstrip("/") or "/",safe="/%")
 query="&".join(f"{k}={v}" for k,v in sorted(parse_qsl(parts.query,keep_blank_values=True)) if k.lower() not in TRACKING)
 return urlunsplit(("https",netloc,path,query,""))
def build_publication_canonical_conflicts_report(rows:list[dict[str,Any]],*,limit:int=DEFAULT_LIMIT,missing_tables:list[str]|None=None,missing_columns:dict[str,list[str]]|None=None,now:Any=None)->dict[str,Any]:
 positive("limit",limit); findings=[]; canon_to_rows=defaultdict(list)
 for r in rows:
  declared=normalize_url(r.get("declared_canonical_url")); observed=normalize_url(r.get("observed_canonical_url")); published=normalize_url(r.get("published_url"))
  conflict=None
  if not declared and (observed or published): conflict="missing_declared"
  elif observed and declared!=observed: conflict="platform_mismatch"
  elif published and declared and normalize_url(r.get("content_canonical_url")) and declared!=normalize_url(r.get("content_canonical_url")): conflict="content_mismatch"
  if declared: canon_to_rows[declared].append(r)
  if conflict: findings.append(_finding(r,declared,observed or published,conflict))
 for canon,items in canon_to_rows.items():
  ids={clean(i.get("content_id")) for i in items}
  if len(ids)>1:
   for r in items: findings.append(_finding(r,canon,normalize_url(r.get("observed_canonical_url") or r.get("published_url")),"duplicate_canonical"))
 findings.sort(key=lambda r:({"high":0,"medium":1}.get(r["severity"],2),r["conflict_type"],r["content_id"] or "",r["publication_id"] or ""))
 shown=findings[:limit]
 return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(now),"thresholds":{"limit":limit},"summary":{"row_count":len(rows),"conflict_count":len(findings),"shown_count":len(shown),"by_conflict_type":dict(sorted(Counter(f["conflict_type"] for f in findings).items()))},"conflicts":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())}}
def _finding(r:dict[str,Any],declared:str,observed:str,typ:str)->dict[str,Any]:
 return {"content_id":clean(r.get("content_id")) or None,"publication_id":clean(r.get("publication_id")) or None,"platform":clean(r.get("platform")) or None,"published_url":clean(r.get("published_url")) or None,"declared_canonical_url":declared or None,"observed_canonical_url":observed or None,"conflict_type":typ,"severity":"high" if typ in {"platform_mismatch","duplicate_canonical"} else "medium"}
def build_publication_canonical_conflicts_report_from_db(db_or_conn:Any,**kwargs:Any)->dict[str,Any]:
 conn=connection(db_or_conn); s=schema(conn); table=next((t for t in ("publications","publication_records","published_content") if t in s),None); miss=[] if table else ["publications|publication_records"]; mc={}
 rows=_load(conn,table,s,mc) if table else []
 return build_publication_canonical_conflicts_report(rows,missing_tables=miss,missing_columns=mc,**kwargs)
def format_publication_canonical_conflicts_json(report:dict[str,Any])->str: return json_dumps(report)
def format_publication_canonical_conflicts_text(report:dict[str,Any])->str:
 lines=["Publication Canonical Conflicts",f"Generated: {report['generated_at']}",f"Totals: rows={report['summary']['row_count']} conflicts={report['summary']['conflict_count']} shown={report['summary']['shown_count']}"]
 if report["missing_tables"]: lines.append("Missing tables: "+", ".join(report["missing_tables"]))
 if not report["conflicts"]: lines.append("No publication canonical conflicts found."); return "\n".join(lines)
 lines+=["","content_id | publication_id | platform | published_url | declared_canonical_url | observed_canonical_url | conflict_type | severity"]
 for f in report["conflicts"]: lines.append(f"{f['content_id'] or '-'} | {f['publication_id'] or '-'} | {f['platform'] or '-'} | {f['published_url'] or '-'} | {f['declared_canonical_url'] or '-'} | {f['observed_canonical_url'] or '-'} | {f['conflict_type']} | {f['severity']}")
 return "\n".join(lines)
def _load(conn:Any,table:str,s:dict[str,set[str]],mc:dict[str,list[str]])->list[dict[str,Any]]:
 pc=s[table]; gid=next((c for c in ("id","publication_id") if c in pc),None)
 if not gid: mc[table]=["id"]; return []
 gc=s.get("generated_content",set()); join="LEFT JOIN generated_content gc ON gc.id = p.content_id" if "generated_content" in s and "content_id" in pc and "id" in gc else ""
 select=[f"p.{gid} AS publication_id",expr(pc,"content_id",default="NULL",alias="p",out="content_id"),expr(pc,"platform",default="NULL",alias="p",out="platform"),expr(pc,"published_url","url",default="NULL",alias="p",out="published_url"),expr(pc,"canonical_url","declared_canonical_url",default="NULL",alias="p",out="declared_canonical_url"),expr(pc,"observed_canonical_url","fetched_canonical_url",default="NULL",alias="p",out="observed_canonical_url"),expr(gc,"canonical_url",default="NULL",alias="gc",out="content_canonical_url")]
 return [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM {table} p {join} ORDER BY p.rowid")]
