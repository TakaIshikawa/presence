"""Find question-like inbound mentions without replies."""
from __future__ import annotations
import re
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE='reply_unanswered_question_gaps'; DEFAULT_MIN_AGE_HOURS=24; QUESTION_RE=re.compile(r'\?|(?:^|\b)(how|what|why|when|where|who|can|could|would|should|is|are|do|does)\b',re.I)
def build_reply_unanswered_question_gaps_report(mentions:list[dict[str,Any]],replies:list[dict[str,Any]]|None=None,*,min_age_hours:int=DEFAULT_MIN_AGE_HOURS,platform:str|None=None,missing_tables=None,missing_columns=None,now=None):
    positive('min_age_hours',min_age_hours); nowdt=dt(now) if isinstance(now,str) else now_value(now); replied={str(r.get('mention_id') or r.get('in_reply_to_mention_id') or r.get('parent_id')) for r in (replies or []) if lower(r.get('status')) in {'approved','posted','queued','published'}}
    findings=[]
    for m in mentions:
        p=clean(m.get('platform'),'unknown'); mid=str(m.get('mention_id') or m.get('id'))
        if platform and p!=platform: continue
        text=clean(m.get('text') or m.get('body') or m.get('content')); sig=QUESTION_RE.search(text)
        rec=dt(m.get('received_at') or m.get('created_at')); age=round((nowdt-rec).total_seconds()/3600,2) if rec else None
        if sig and mid not in replied and (age is None or age>=min_age_hours): findings.append({'mention_id':m.get('mention_id') or m.get('id'),'author':m.get('author') or m.get('author_handle'),'received_at':rec.isoformat() if rec else None,'age_hours':age,'platform':p,'question_signal':sig.group(0).strip(),'text_excerpt':text[:120]})
    findings.sort(key=lambda f:(-(f['age_hours'] or 0), f['platform'], str(f['mention_id'])))
    return {'artifact_type':ARTIFACT_TYPE,'generated_at':nowdt.isoformat(),'filters':{'min_age_hours':min_age_hours,'platform':platform},'totals':{'mentions':len(mentions),'findings':len(findings)},'findings':findings,'missing_tables':sorted(missing_tables or []),'missing_columns':{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},'empty_state':empty_state(findings,'No unanswered question gaps found.',schema_gap=bool(missing_tables or missing_columns))}
def build_reply_unanswered_question_gaps_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); mt=[]; mentions=[]; replies=[]
    mt += [] if 'inbound_mentions' in s else ['inbound_mentions']
    if 'inbound_mentions' in s: mentions=load_table(conn,'inbound_mentions',s['inbound_mentions'],{'mention_id':('mention_id','id'),'author':('author','author_handle'),'platform':('platform',),'text':('text','body','content'),'received_at':('received_at','created_at')})
    for t in ('reply_drafts','replies','reply_queue'):
        if t in s: replies+=load_table(conn,t,s[t],{'mention_id':('mention_id','in_reply_to_mention_id','parent_id'),'status':('status',)})
    return build_reply_unanswered_question_gaps_report(mentions,replies,missing_tables=mt,missing_columns={},**kw)
def format_reply_unanswered_question_gaps_json(r): return json_dumps(r)
def format_reply_unanswered_question_gaps_text(r):
    lines=['Reply Unanswered Question Gaps',f"Generated: {r['generated_at']}",f"Totals: mentions={r['totals']['mentions']} findings={r['totals']['findings']}"]
    if r['missing_tables']: lines.append('Missing tables: '+', '.join(r['missing_tables']))
    if not r['findings']: lines.append(r['empty_state']['message']); return '\n'.join(lines)
    lines+=['','mention_id | platform | age_hours | signal']
    for f in r['findings']: lines.append(f"{f['mention_id']} | {f['platform']} | {f['age_hours']} | {f['question_signal']}")
    return '\n'.join(lines)
