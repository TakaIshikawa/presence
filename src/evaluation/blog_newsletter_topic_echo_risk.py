"""Blog Newsletter Topic Echo Risk."""
from __future__ import annotations
from collections import Counter,defaultdict
from datetime import datetime,timezone,timedelta
from typing import Any
import re
from ._report_utils import clean,connection,dt,expr,json_dumps,lower,now_iso,schema,to_int

ARTIFACT_TYPE='blog_newsletter_topic_echo_risk'; DEFAULT_LIMIT=50; STOP={'the','a','an','and','or','to','of','for','in','on','with'}
def _tokens(x): return {t for t in re.findall(r'[a-z0-9]+',lower(x)) if t not in STOP and len(t)>2}
def _sim(a,b):
 ta,tb=_tokens(a),_tokens(b)
 return round(len(ta&tb)/max(len(ta|tb),1),4)
def build_blog_newsletter_topic_echo_risk_report(blog_rows:list[dict[str,Any]],newsletter_rows:list[dict[str,Any]],*,cooldown_days:int=14,similarity_threshold:float=.5,limit:int=DEFAULT_LIMIT,missing_tables=None,missing_columns=None,now=None):
 if cooldown_days<=0 or limit<=0: raise ValueError('positive values required')
 if not 0<=similarity_threshold<=1: raise ValueError('similarity_threshold must be between 0 and 1')
 findings=[]
 for b in blog_rows:
  bt=dt(b.get('published_at') or b.get('created_at')); btitle=clean(b.get('title') or b.get('topic'))
  for n in newsletter_rows:
   nt=dt(n.get('sent_at') or n.get('published_at') or n.get('created_at')); ntitle=clean(n.get('title') or n.get('subject') or n.get('topic'))
   if not bt or not nt: continue
   gap=abs((bt.date()-nt.date()).days); sim=_sim(btitle,ntitle)
   if gap<=cooldown_days and sim>=similarity_threshold: findings.append({'blog_id':b.get('blog_id') or b.get('id'),'newsletter_id':n.get('newsletter_id') or n.get('id'),'blog_title':btitle,'newsletter_title':ntitle,'day_gap':gap,'similarity':sim})
 findings.sort(key=lambda f:(-f['similarity'],f['day_gap'],str(f['blog_id']),str(f['newsletter_id'])))
 return {'artifact_type':ARTIFACT_TYPE,'generated_at':now_iso(now),'filters':{'cooldown_days':cooldown_days,'similarity_threshold':similarity_threshold,'limit':limit},'totals':{'blog_posts':len(blog_rows),'newsletter_issues':len(newsletter_rows),'findings':len(findings),'shown_findings':len(findings[:limit])},'findings':findings[:limit],'missing_tables':sorted(missing_tables or []),'missing_columns':missing_columns or {},'empty_state':{'is_empty':not findings,'message':'No blog newsletter topic echo risks found.' if not findings else None}}
def build_blog_newsletter_topic_echo_risk_report_from_db(db_or_conn:Any,**kw):
 conn=connection(db_or_conn); s=schema(conn); bt='blog_posts' if 'blog_posts' in s else 'generated_content' if 'generated_content' in s else None
 missing=[]
 if not bt: missing.append('blog_posts|generated_content')
 if 'newsletter_issues' not in s: missing.append('newsletter_issues')
 if missing: return build_blog_newsletter_topic_echo_risk_report([],[],missing_tables=missing,**kw)
 bc=s[bt]; nc=s['newsletter_issues']; blogs=[dict(r) for r in conn.execute(f"SELECT {expr(bc,'id','blog_id',out='blog_id')}, {expr(bc,'title','topic',default='NULL',out='title')}, {expr(bc,'published_at','created_at',default='NULL',out='published_at')} FROM {bt} ORDER BY rowid")]
 news=[dict(r) for r in conn.execute(f"SELECT {expr(nc,'id','newsletter_id',out='newsletter_id')}, {expr(nc,'title','subject','topic',default='NULL',out='title')}, {expr(nc,'sent_at','published_at','created_at',default='NULL',out='sent_at')} FROM newsletter_issues ORDER BY rowid")]
 return build_blog_newsletter_topic_echo_risk_report(blogs,news,**kw)
def format_blog_newsletter_topic_echo_risk_json(r): return json_dumps(r)
def format_blog_newsletter_topic_echo_risk_text(r):
 lines=['Blog Newsletter Topic Echo Risk',f"Generated: {r['generated_at']}",f"Totals: blog_posts={r['totals']['blog_posts']} newsletter_issues={r['totals']['newsletter_issues']} findings={r['totals']['findings']}"]
 if r['missing_tables']: lines.append('Missing tables: '+', '.join(r['missing_tables']))
 if not r['findings']: lines.append(r['empty_state']['message'] or 'No findings.'); return '\n'.join(lines)
 lines+=['','blog_id | newsletter_id | similarity | day_gap']
 for f in r['findings']: lines.append(f"{f['blog_id']} | {f['newsletter_id']} | {f['similarity']} | {f['day_gap']}")
 return '\n'.join(lines)
