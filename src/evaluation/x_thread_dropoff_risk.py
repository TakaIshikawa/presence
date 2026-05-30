"""Audit generated X threads for length, continuity, and engagement dropoff risk."""
from __future__ import annotations
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE="x_thread_dropoff_risk"; DEFAULT_DAYS=30; DEFAULT_LIMIT=50; DEFAULT_MAX_POST_CHARS=280; DEFAULT_DROPOFF_THRESHOLD=.5
def _posts(r):
 meta=clean(r.get("metadata"))
 if meta:
  try:
   obj=json.loads(meta); p=obj.get("posts") or obj.get("thread")
   if isinstance(p,list): return [clean(x.get("text") if isinstance(x,dict) else x) for x in p if clean(x.get("text") if isinstance(x,dict) else x)]
  except Exception: pass
 text=clean(r.get("body") or r.get("content")); parts=[p.strip() for p in re.split(r"\n\s*(?:---+|\d+[/.)]\s*)",text) if p.strip()]
 return parts if len(parts)>1 else [p.strip() for p in text.split("\n\n") if p.strip()] or ([text] if text else [])
def build_x_thread_dropoff_risk_report(rows:list[dict[str,Any]],*,days:int=DEFAULT_DAYS,limit:int=DEFAULT_LIMIT,max_post_chars:int=DEFAULT_MAX_POST_CHARS,dropoff_threshold:float=DEFAULT_DROPOFF_THRESHOLD,missing_tables=None,missing_columns=None,now=None):
 positive("days",days); positive("limit",limit); positive("max_post_chars",max_post_chars); bounded_share("dropoff_threshold",dropoff_threshold); findings=[]
 for r in rows:
  ps=_posts(r); reasons=[]; details={}
  over=[i+1 for i,p in enumerate(ps) if len(p)>max_post_chars]
  if over: reasons.append("overlong_posts"); details["overlong_posts"]=over
  if len(ps)>1 and not re.search(r"\b(because|but|so|next|also|here|why|how|then|first|second)\b|[?:]$",ps[0].lower()): reasons.append("weak_continuation")
  eng=r.get("engagements")
  if eng is None and clean(r.get("metadata")):
   try: eng=json.loads(clean(r.get("metadata"))).get("engagements")
   except Exception: eng=None
  if isinstance(eng,str):
   try: eng=json.loads(eng)
   except Exception: eng=None
  if isinstance(eng,list) and len(eng)>1:
   vals=[to_float(x.get("engagement") if isinstance(x,dict) else x) for x in eng]
   for a,b in zip(vals,vals[1:]):
    if a>0 and (a-b)/a>dropoff_threshold: reasons.append("engagement_dropoff"); details["dropoff_ratio"]=round((a-b)/a,4); break
  if reasons: findings.append({"thread_id":r.get("thread_id") or r.get("id"),"title":clean(r.get("title")) or None,"post_count":len(ps),"reasons":sorted(set(reasons)),"details":details})
 findings.sort(key=lambda f:(-len(f["reasons"]),str(f["thread_id"])))
 return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(now),"filters":{"days":days,"limit":limit,"max_post_chars":max_post_chars,"dropoff_threshold":dropoff_threshold},"summary":{"thread_count":len(rows),"finding_count":len(findings)},"findings":findings[:limit],"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":empty_state(findings,"No X thread dropoff risk found.",schema_gap=bool(missing_tables or missing_columns))}
def build_x_thread_dropoff_risk_report_from_db(db_or_conn:Any,**kw):
 conn=connection(db_or_conn); s=schema(conn)
 if "generated_content" not in s: return build_x_thread_dropoff_risk_report([],missing_tables=["generated_content"],**kw)
 c=s["generated_content"]; miss=[] if "content_type" in c else ["content_type"]
 if not ({"body","content","metadata"}&c): miss.append("body|content|metadata")
 if miss: return build_x_thread_dropoff_risk_report([],missing_columns={"generated_content":miss},**kw)
 where=["LOWER(content_type)='x_thread'"]; params=[]; days=kw.get("days",DEFAULT_DAYS)
 if "created_at" in c: where.append("(created_at IS NULL OR created_at >= ?)"); params.append((now_value(kw.get("now"))-timedelta(days=days)).isoformat())
 rows=[dict(r) for r in conn.execute(f"SELECT {pick(c,'id',out='thread_id')}, {pick(c,'title',out='title')}, {pick(c,'body','content','text',out='body')}, {pick(c,'metadata','meta_json',out='metadata')} FROM generated_content WHERE {' AND '.join(where)} ORDER BY rowid",params)]
 return build_x_thread_dropoff_risk_report(rows,**kw)
def format_x_thread_dropoff_risk_json(r): return json_dumps(r)
def format_x_thread_dropoff_risk_text(r):
 lines=["X Thread Dropoff Risk",f"Artifact: {r['artifact_type']}",f"Generated: {r['generated_at']}",f"Totals: threads={r['summary']['thread_count']} findings={r['summary']['finding_count']}"]
 for f in r["findings"]: lines.append(f"- {f['thread_id']}: {','.join(f['reasons'])}")
 return "\n".join(lines)
