"""Compare blog and newsletter topics for echo risk."""
from __future__ import annotations
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE='blog_newsletter_topic_echo_risk'; DEFAULT_LIMIT=50; DEFAULT_COOLDOWN_DAYS=14; DEFAULT_SIMILARITY=0.5
def build_blog_newsletter_topic_echo_risk_report(blog_rows:list[dict[str,Any]],newsletter_rows:list[dict[str,Any]]|None=None,*,cooldown_days:int=DEFAULT_COOLDOWN_DAYS,similarity_threshold:float=DEFAULT_SIMILARITY,limit:int=DEFAULT_LIMIT,missing_tables=None,missing_columns=None,now=None):
    positive('cooldown_days',cooldown_days); bounded_share('similarity_threshold',similarity_threshold); positive('limit',limit); findings=[]
    news=newsletter_rows or []
    for i,b in enumerate(blog_rows):
        bt=dt(b.get('published_at') or b.get('created_at')); btxt=clean(b.get('topic') or b.get('title') or b.get('headline')); btok=tokens(btxt)
        for j,n in enumerate(news):
            nt=dt(n.get('sent_at') or n.get('published_at') or n.get('created_at')); ntxt=clean(n.get('topic') or n.get('title') or n.get('subject')); sim=jaccard(btok,tokens(ntxt)); gap=abs((bt-nt).days) if bt and nt else None
            if sim>=similarity_threshold and (gap is None or gap<=cooldown_days): findings.append({'blog_id':b.get('blog_id') or b.get('id') or i+1,'newsletter_id':n.get('issue_id') or n.get('id') or j+1,'blog_title':btxt,'newsletter_title':ntxt,'similarity':sim,'day_gap':gap,'severity':round(sim*100-(gap or 0),2)})
    findings.sort(key=lambda f:(-f['similarity'],f['day_gap'] if f['day_gap'] is not None else 999,str(f['blog_id']),str(f['newsletter_id'])))
    return {'artifact_type':ARTIFACT_TYPE,'generated_at':now_iso(now),'filters':{'cooldown_days':cooldown_days,'similarity_threshold':similarity_threshold,'limit':limit},'totals':{'blog_rows':len(blog_rows),'newsletter_rows':len(news),'findings':len(findings)},'findings':findings[:limit],'missing_tables':sorted(missing_tables or []),'missing_columns':{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},'empty_state':empty_state(findings,'No blog newsletter topic echo risk found.',schema_gap=bool(missing_tables or missing_columns))}
def build_blog_newsletter_topic_echo_risk_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); mt=[]; mc={}; blogs=[]; news=[]
    btable='blog_posts' if 'blog_posts' in s else ('generated_content' if 'generated_content' in s else None)
    if not btable: mt.append('blog_posts')
    else: blogs=load_table(conn,btable,s[btable],{'blog_id':('id','blog_id'),'title':('title','headline'),'topic':('topic',),'published_at':('published_at',),'created_at':('created_at',),'content_type':('content_type',)})
    if 'newsletter_issues' not in s: mt.append('newsletter_issues')
    else: news=load_table(conn,'newsletter_issues',s['newsletter_issues'],{'issue_id':('id','issue_id'),'title':('title','subject'),'topic':('topic',),'sent_at':('sent_at',),'published_at':('published_at',),'created_at':('created_at',)})
    return build_blog_newsletter_topic_echo_risk_report(blogs,news,missing_tables=mt,missing_columns=mc,**kw)
def format_blog_newsletter_topic_echo_risk_json(r): return json_dumps(r)
def format_blog_newsletter_topic_echo_risk_text(r):
    lines=['Blog Newsletter Topic Echo Risk',f"Generated: {r['generated_at']}",f"Totals: blogs={r['totals']['blog_rows']} newsletters={r['totals']['newsletter_rows']} findings={r['totals']['findings']}"]
    if r['missing_tables']: lines.append('Missing tables: '+', '.join(r['missing_tables']))
    if not r['findings']: lines.append(r['empty_state']['message']); return '\n'.join(lines)
    lines += ['','blog_id | newsletter_id | similarity | day_gap']
    for f in r['findings']: lines.append(f"{f['blog_id']} | {f['newsletter_id']} | {f['similarity']} | {f['day_gap']}")
    return '\n'.join(lines)
