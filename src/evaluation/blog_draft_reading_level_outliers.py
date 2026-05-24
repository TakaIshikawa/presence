"""Report blog drafts with simple readability outliers."""
from __future__ import annotations
import re
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE='blog_draft_reading_level_outliers'; DEFAULT_MAX_AVG_SENTENCE=28; DEFAULT_MAX_COMPLEX_RATIO=.25; DEFAULT_MIN_WORDS=50
def _syllables(w): return max(1,len(re.findall(r'[aeiouy]+',w.lower())))
def _metrics(text):
    words=re.findall(r"[A-Za-z][A-Za-z'-]*", clean(text)); sentences=[s for s in re.split(r'[.!?]+', clean(text)) if s.strip()]; paras=[p for p in clean(text).split('\n\n') if p.strip()]
    wc=len(words); sc=max(1,len(sentences)); complex_words=sum(1 for w in words if _syllables(w)>=3)
    return {'word_count':wc,'sentence_count':len(sentences),'average_sentence_words':round(wc/sc,2),'long_paragraph_count':sum(1 for p in paras if len(re.findall(r"[A-Za-z][A-Za-z'-]*",p))>140),'complex_word_ratio':round(complex_words/wc,4) if wc else 0.0}
def build_blog_draft_reading_level_outliers_report(rows:list[dict[str,Any]],*,max_average_sentence_words:float=DEFAULT_MAX_AVG_SENTENCE,max_complex_word_ratio:float=DEFAULT_MAX_COMPLEX_RATIO,min_words:int=DEFAULT_MIN_WORDS,missing_tables=None,missing_columns=None,now=None):
    positive('max_average_sentence_words',max_average_sentence_words); bounded_share('max_complex_word_ratio',max_complex_word_ratio); positive('min_words',min_words); findings=[]
    for r in rows:
        m=_metrics(r.get('content') or r.get('body') or r.get('draft') or ''); triggers=[]
        if m['word_count']<min_words: continue
        if m['average_sentence_words']>max_average_sentence_words: triggers.append('average_sentence_words')
        if m['complex_word_ratio']>max_complex_word_ratio: triggers.append('complex_word_ratio')
        if m['long_paragraph_count']>0: triggers.append('long_paragraph_count')
        if triggers: findings.append({'draft_id':r.get('draft_id') or r.get('id'),'title':r.get('title'),'triggering_metrics':triggers,**m,'severity':round(len(triggers)*100+m['average_sentence_words']+m['complex_word_ratio']*100,2)})
    findings.sort(key=lambda f:(-f['severity'], str(f['draft_id'])))
    return {'artifact_type':ARTIFACT_TYPE,'generated_at':now_iso(now),'filters':{'max_average_sentence_words':max_average_sentence_words,'max_complex_word_ratio':max_complex_word_ratio,'min_words':min_words},'totals':{'drafts':len(rows),'findings':len(findings)},'findings':findings,'missing_tables':sorted(missing_tables or []),'missing_columns':{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},'empty_state':empty_state(findings,'No blog draft reading level outliers found.',schema_gap=bool(missing_tables or missing_columns))}
def build_blog_draft_reading_level_outliers_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); table='blog_drafts' if 'blog_drafts' in s else ('generated_content' if 'generated_content' in s else None); rows=[]; mt=[] if table else ['blog_drafts']
    if table: rows=load_table(conn,table,s[table],{'draft_id':('draft_id','id'),'title':('title','headline'),'content':('content','body','draft')})
    return build_blog_draft_reading_level_outliers_report(rows,missing_tables=mt,missing_columns={},**kw)
def format_blog_draft_reading_level_outliers_json(r): return json_dumps(r)
def format_blog_draft_reading_level_outliers_text(r):
    lines=['Blog Draft Reading Level Outliers',f"Generated: {r['generated_at']}",f"Totals: drafts={r['totals']['drafts']} findings={r['totals']['findings']}"]
    if r['missing_tables']: lines.append('Missing tables: '+', '.join(r['missing_tables']))
    if not r['findings']: lines.append(r['empty_state']['message']); return '\n'.join(lines)
    lines+=['','draft_id | words | avg_sentence | complex_ratio | triggers']
    for f in r['findings']: lines.append(f"{f['draft_id']} | {f['word_count']} | {f['average_sentence_words']} | {f['complex_word_ratio']} | {', '.join(f['triggering_metrics'])}")
    return '\n'.join(lines)
