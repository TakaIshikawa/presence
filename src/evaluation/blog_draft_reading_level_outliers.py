"""Flag blog drafts with simple reading-level outlier metrics."""
from __future__ import annotations
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE="blog_draft_reading_level_outliers"; DEFAULT_MAX_AVG_SENTENCE_WORDS=28.0; DEFAULT_MAX_COMPLEX_WORD_RATIO=0.25; DEFAULT_MIN_WORDS=50
def build_blog_draft_reading_level_outliers_report(rows:list[dict[str,Any]],*,max_average_sentence_words:float=DEFAULT_MAX_AVG_SENTENCE_WORDS,max_complex_word_ratio:float=DEFAULT_MAX_COMPLEX_WORD_RATIO,min_words:int=DEFAULT_MIN_WORDS,missing_tables=None,missing_columns=None,now=None):
    positive("max_average_sentence_words",max_average_sentence_words); bounded_share("max_complex_word_ratio",max_complex_word_ratio); positive("min_words",min_words); findings=[]
    for row in rows:
        textv=clean(row.get("body") or row.get("content") or row.get("draft_text") or row.get("text")); words=re.findall(r"[A-Za-z]+",textv); wc=len(words)
        if wc<min_words: continue
        sentences=[s for s in re.split(r"[.!?]+",textv) if re.search(r"[A-Za-z]",s)]; sc=max(1,len(sentences)); avg=round(wc/sc,2); paras=[p for p in re.split(r"\n\s*\n",textv) if p.strip()]; long_paras=sum(1 for p in paras if len(re.findall(r"[A-Za-z]+",p))>150); complex_ratio=round(sum(1 for w in words if _syllables(w)>=3)/wc,4)
        triggers=[]
        if avg>max_average_sentence_words: triggers.append("average_sentence_words")
        if complex_ratio>max_complex_word_ratio: triggers.append("complex_word_ratio")
        if long_paras: triggers.append("long_paragraphs")
        if triggers: findings.append({"draft_id":row.get("draft_id") or row.get("id"),"title":clean(row.get("title")),"word_count":wc,"sentence_count":sc,"average_sentence_words":avg,"long_paragraph_count":long_paras,"complex_word_ratio":complex_ratio,"triggers":triggers})
    findings.sort(key=lambda f:(-len(f["triggers"]),-f["average_sentence_words"],str(f["draft_id"])))
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(now),"thresholds":{"max_average_sentence_words":max_average_sentence_words,"max_complex_word_ratio":max_complex_word_ratio,"min_words":min_words},"totals":{"drafts":len(rows),"findings":len(findings)},"findings":findings,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":empty_state(findings,"No blog draft reading level outliers found.",schema_gap=bool(missing_tables or missing_columns))}
def build_blog_draft_reading_level_outliers_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); table="blog_drafts" if "blog_drafts" in s else "generated_content" if "generated_content" in s else None
    if not table: return build_blog_draft_reading_level_outliers_report([],missing_tables=["blog_drafts"],**kw)
    rows=load_table(conn,table,s[table],{"draft_id":("id","draft_id"),"title":("title","headline"),"body":("body","content","draft_text","text"),"status":("status",)})
    return build_blog_draft_reading_level_outliers_report(rows,**kw)
def format_blog_draft_reading_level_outliers_json(r): return json_dumps(r)
def format_blog_draft_reading_level_outliers_text(r):
    lines=["Blog Draft Reading Level Outliers",f"Generated: {r['generated_at']}",f"Totals: drafts={r['totals']['drafts']} findings={r['totals']['findings']}"]
    if not r["findings"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    lines+=["","draft_id | words | avg_sentence | complex_ratio | triggers"]
    for f in r["findings"]: lines.append(f"{f['draft_id']} | {f['word_count']} | {f['average_sentence_words']} | {f['complex_word_ratio']} | {','.join(f['triggers'])}")
    return "\n".join(lines)
def _syllables(w:str)->int:
    parts=re.findall(r"[aeiouy]+",w.lower()); return max(1,len(parts)-(1 if w.lower().endswith("e") and len(parts)>1 else 0))
