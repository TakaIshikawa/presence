"""Flag repeated blog/newsletter topics inside a cooldown window."""
from __future__ import annotations
from datetime import datetime,timedelta,timezone
from typing import Any
import re
from ._report_utils import clean,connection,dt,expr,json_dumps,now_iso,positive,schema
ARTIFACT_TYPE="blog_newsletter_topic_echo_risk"; DEFAULT_COOLDOWN_DAYS=14; DEFAULT_SIMILARITY=0.5; DEFAULT_LIMIT=50; TOKEN=re.compile(r"[a-z0-9]+")
STOP={"the","and","for","with","from","this","that","your","into","about","newsletter","blog"}
def build_blog_newsletter_topic_echo_risk_report(blog_rows:list[dict[str,Any]],newsletter_rows:list[dict[str,Any]],*,cooldown_days:int=DEFAULT_COOLDOWN_DAYS,similarity_threshold:float=DEFAULT_SIMILARITY,limit:int=DEFAULT_LIMIT,missing_tables:list[str]|None=None,missing_columns:dict[str,list[str]]|None=None,now:Any=None)->dict[str,Any]:
 positive("cooldown_days",cooldown_days); positive("limit",limit)
 if not 0<=similarity_threshold<=1: raise ValueError("similarity_threshold must be between 0 and 1")
 blogs=[_norm(b,"blog") for b in blog_rows]; news=[_norm(n,"newsletter") for n in newsletter_rows]; findings=[]
 for b in blogs:
  for n in news:
   if not b["at"] or not n["at"]: continue
   gap=abs((n["at"]-b["at"]).days)
   if gap>cooldown_days: continue
   sim=_sim(b["tokens"],n["tokens"])
   if sim>=similarity_threshold: findings.append({"blog_id":b["id"],"newsletter_id":n["id"],"blog_title":b["title"],"newsletter_subject":n["title"],"similarity":round(sim,4),"day_gap":gap,"shared_tokens":sorted(b["tokens"]&n["tokens"])})
 findings.sort(key=lambda f:(-f["similarity"],f["day_gap"],f["blog_id"],f["newsletter_id"])); shown=findings[:limit]
 return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(now),"filters":{"cooldown_days":cooldown_days,"similarity_threshold":similarity_threshold,"limit":limit},"totals":{"blog_count":len(blogs),"newsletter_count":len(news),"finding_count":len(findings),"shown_findings":len(shown)},"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":{"is_empty":not findings,"message":"No blog newsletter topic echo risk found." if not findings else None}}
def build_blog_newsletter_topic_echo_risk_report_from_db(db_or_conn:Any,**kwargs:Any)->dict[str,Any]:
 conn=connection(db_or_conn); s=schema(conn); mt=[]; blogs=[]; news=[]; mc={}
 if "blog_posts" in s: blogs=_blogs(conn,"blog_posts",s["blog_posts"])
 elif "generated_content" in s: blogs=_blogs(conn,"generated_content",s["generated_content"])
 else: mt.append("blog_posts")
 if "newsletter_issues" in s: news=_news(conn,s["newsletter_issues"])
 else: mt.append("newsletter_issues")
 return build_blog_newsletter_topic_echo_risk_report(blogs,news,missing_tables=mt,missing_columns=mc,**kwargs)
def format_blog_newsletter_topic_echo_risk_json(report:dict[str,Any])->str: return json_dumps(report)
def format_blog_newsletter_topic_echo_risk_text(report:dict[str,Any])->str:
 lines=["Blog Newsletter Topic Echo Risk",f"Generated: {report['generated_at']}",f"Totals: blogs={report['totals']['blog_count']} newsletters={report['totals']['newsletter_count']} findings={report['totals']['finding_count']} shown={report['totals']['shown_findings']}"]
 if report["missing_tables"]: lines.append("Missing tables: "+", ".join(report["missing_tables"]))
 if not report["findings"]: lines.append(report["empty_state"]["message"] or "No findings."); return "\n".join(lines)
 lines+=["","blog_id | newsletter_id | similarity | day_gap | shared_tokens"]
 for f in report["findings"]: lines.append(f"{f['blog_id']} | {f['newsletter_id']} | {f['similarity']} | {f['day_gap']} | {','.join(f['shared_tokens'])}")
 return "\n".join(lines)
def _blogs(conn:Any,t:str,cols:set[str])->list[dict[str,Any]]:
 select=[expr(cols,"id","post_id",out="id"),expr(cols,"title","content",out="title"),expr(cols,"topic","topics","tags","content_format",out="topic"),expr(cols,"published_at","created_at",out="published_at")]
 where=" WHERE LOWER(content_type) LIKE '%blog%'" if t=="generated_content" and "content_type" in cols else ""
 return [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM {t}{where} ORDER BY rowid")]
def _news(conn:Any,cols:set[str])->list[dict[str,Any]]:
 select=[expr(cols,"id","issue_id",out="id"),expr(cols,"subject","title",out="title"),expr(cols,"topic","topics",out="topic"),expr(cols,"sent_at","published_at","created_at",out="published_at")]
 return [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM newsletter_issues ORDER BY rowid")]
def _norm(r:dict[str,Any],kind:str)->dict[str,Any]: return {"id":clean(r.get("id"),"unknown"),"title":clean(r.get("title")),"at":dt(r.get("published_at") or r.get("created_at")),"tokens":_tokens(clean(r.get("title"))+" "+clean(r.get("topic")))}
def _tokens(text:str)->set[str]: return {t for t in TOKEN.findall(text.lower()) if t not in STOP and len(t)>2}
def _sim(a:set[str],b:set[str])->float: return len(a&b)/len(a|b) if a and b else 0.0
