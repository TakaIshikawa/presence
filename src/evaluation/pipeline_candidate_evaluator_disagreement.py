"""Report pipeline candidate evaluator disagreement."""
from __future__ import annotations
import json
from collections import defaultdict
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE='pipeline_candidate_evaluator_disagreement'; DEFAULT_MIN_SCORE_SPREAD=2.0
def _data(v):
    if isinstance(v,(dict,list)): return v
    try: return json.loads(clean(v) or '{}')
    except json.JSONDecodeError: return {}
def _rows_from_artifact(row):
    data=_data(row.get('artifact') or row.get('artifact_json') or row.get('metadata') or row.get('payload')); out=[]
    evals=data.get('evaluations') if isinstance(data,dict) else None
    if isinstance(evals,list):
        for e in evals: out.append({**row, **e})
    else: out.append(row)
    return out
def build_pipeline_candidate_evaluator_disagreement_report(rows:list[dict[str,Any]],*,min_score_spread:float=DEFAULT_MIN_SCORE_SPREAD,since:str|None=None,missing_tables=None,missing_columns=None,now=None):
    positive('min_score_spread',min_score_spread); since_dt=dt(since); grouped=defaultdict(list)
    for raw in rows:
        for r in _rows_from_artifact(raw):
            ts=dt(r.get('created_at') or r.get('evaluated_at'))
            if since_dt and ts and ts<since_dt: continue
            grouped[(r.get('run_id') or r.get('pipeline_run_id') or r.get('id'), r.get('candidate_id'))].append(r)
    by_run=defaultdict(list)
    for (run,cand),items in grouped.items():
        scores=[to_float(i.get('score'),0) for i in items if i.get('score') is not None]; gates={lower(i.get('gate_decision') or i.get('decision')) for i in items if clean(i.get('gate_decision') or i.get('decision'))}
        spread=round(max(scores)-min(scores),4) if scores else 0.0
        if spread>=min_score_spread or len(gates)>1: by_run[run].append((cand,items,spread,gates))
    findings=[]
    for run,items in by_run.items():
        flat=[i for _,its,_,_ in items for i in its]; selected=next((i.get('selected_candidate_id') for i in flat if i.get('selected_candidate_id')), None) or next((i.get('candidate_id') for i in flat if to_int(i.get('selected'),0)), None)
        ranks=[to_int(i.get('rank'),0) for i in flat if selected and str(i.get('candidate_id'))==str(selected) and i.get('rank') is not None]
        findings.append({'run_id':run,'content_type':clean(flat[0].get('content_type'),'unknown') if flat else 'unknown','score_spread':max(s for _,_,s,_ in items),'disagreeing_evaluators':sorted({clean(i.get('evaluator'),'unknown') for _,its,_,_ in items for i in its}),'selected_candidate_id':selected,'selected_candidate_rank':min(ranks) if ranks else None,'severity':round(max(s for _,_,s,_ in items)*10+len(items)*20+(20 if ranks and min(ranks)>1 else 0),2)})
    findings.sort(key=lambda f:(-f['severity'], str(f['run_id'])))
    return {'artifact_type':ARTIFACT_TYPE,'generated_at':now_iso(now),'filters':{'min_score_spread':min_score_spread,'since':since},'totals':{'rows':len(rows),'findings':len(findings)},'findings':findings,'missing_tables':sorted(missing_tables or []),'missing_columns':{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},'empty_state':empty_state(findings,'No pipeline candidate evaluator disagreement found.',schema_gap=bool(missing_tables or missing_columns))}
def build_pipeline_candidate_evaluator_disagreement_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); rows=[]; mt=[]
    table=next((t for t in ('pipeline_candidate_evaluations','candidate_evaluations','pipeline_runs') if t in s),None)
    if not table: mt.append('pipeline_candidate_evaluations')
    else: rows=load_table(conn,table,s[table],{'run_id':('run_id','pipeline_run_id','id'),'candidate_id':('candidate_id',),'evaluator':('evaluator','evaluator_name'),'score':('score','evaluation_score'),'gate_decision':('gate_decision','decision'),'selected_candidate_id':('selected_candidate_id',),'selected':('selected','is_selected'),'rank':('rank',),'content_type':('content_type',),'created_at':('created_at','evaluated_at'),'artifact':('artifact','artifact_json','metadata','payload')})
    return build_pipeline_candidate_evaluator_disagreement_report(rows,missing_tables=mt,missing_columns={},**kw)
def format_pipeline_candidate_evaluator_disagreement_json(r): return json_dumps(r)
def format_pipeline_candidate_evaluator_disagreement_text(r):
    lines=['Pipeline Candidate Evaluator Disagreement',f"Generated: {r['generated_at']}",f"Totals: rows={r['totals']['rows']} findings={r['totals']['findings']}"]
    if r['missing_tables']: lines.append('Missing tables: '+', '.join(r['missing_tables']))
    if not r['findings']: lines.append(r['empty_state']['message']); return '\n'.join(lines)
    lines+=['','run_id | content_type | score_spread | evaluators | selected']
    for f in r['findings']: lines.append(f"{f['run_id']} | {f['content_type']} | {f['score_spread']} | {', '.join(f['disagreeing_evaluators'])} | {f['selected_candidate_id']}")
    return '\n'.join(lines)
