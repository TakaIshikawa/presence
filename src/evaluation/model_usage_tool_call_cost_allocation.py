"""Allocate model usage cost to tool-heavy stages."""
from __future__ import annotations
from collections import defaultdict
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE='model_usage_tool_call_cost_allocation'
def build_model_usage_tool_call_cost_allocation_report(usage:list[dict[str,Any]],tools:list[dict[str,Any]]|None=None,*,since:str|None=None,stage:str|None=None,tool:str|None=None,missing_tables=None,missing_columns=None,now=None):
    since_dt=dt(since); tool_by_session=defaultdict(list)
    for t in tools or []: tool_by_session[clean(t.get('session_id') or t.get('run_id'))].append(t)
    buckets=defaultdict(lambda:{'input_tokens':0,'output_tokens':0,'total_tokens':0,'estimated_cost':0.0,'usage_count':0,'unallocated_reasons':set()})
    for u in usage:
        ts=dt(u.get('created_at') or u.get('timestamp')); st=clean(u.get('stage') or u.get('pipeline_stage') or 'unallocated'); model=clean(u.get('model'),'unknown'); sess=clean(u.get('session_id') or u.get('run_id'))
        if since_dt and ts and ts<since_dt: continue
        linked=tool_by_session.get(sess,[])
        names=[clean(t.get('tool_name') or t.get('tool'),'unknown') for t in linked] or ['unallocated']
        if not sess: names=['unallocated']
        for name in names:
            if stage and st!=stage: continue
            if tool and name!=tool: continue
            key=(st,name,model,(ts.date().isoformat() if ts else 'unknown')); b=buckets[key]; b['input_tokens']+=to_int(u.get('input_tokens') or u.get('prompt_tokens'),0); b['output_tokens']+=to_int(u.get('output_tokens') or u.get('completion_tokens'),0); b['total_tokens']+=to_int(u.get('total_tokens'),0) or to_int(u.get('input_tokens') or u.get('prompt_tokens'),0)+to_int(u.get('output_tokens') or u.get('completion_tokens'),0); b['estimated_cost']+=to_float(u.get('cost') or u.get('estimated_cost_usd'),0.0); b['usage_count']+=1
            if name=='unallocated': b['unallocated_reasons'].add('missing_tool_call' if sess else 'missing_session_id')
    rows=[]
    for (st,name,model,day),b in buckets.items(): rows.append({'stage':st,'tool_name':name,'model':model,'day':day,'input_tokens':b['input_tokens'],'output_tokens':b['output_tokens'],'total_tokens':b['total_tokens'],'estimated_cost':round(b['estimated_cost'],6),'usage_count':b['usage_count'],'unallocated_reasons':sorted(b['unallocated_reasons'])})
    rows.sort(key=lambda r:(r['stage'],r['tool_name'],r['model'],r['day']))
    return {'artifact_type':ARTIFACT_TYPE,'generated_at':now_iso(now),'filters':{'since':since,'stage':stage,'tool':tool},'totals':{'usage_rows':len(usage),'tool_rows':len(tools or []),'groups':len(rows)},'groups':rows,'missing_tables':sorted(missing_tables or []),'missing_columns':{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},'empty_state':empty_state(rows,'No model usage cost allocation rows found.',schema_gap=bool(missing_tables or missing_columns))}
def build_model_usage_tool_call_cost_allocation_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); mt=[]; usage=[]; tools=[]
    utable=next((t for t in ('model_usage','model_usage_records') if t in s),None)
    if not utable: mt.append('model_usage')
    else: usage=load_table(conn,utable,s[utable],{'session_id':('session_id','run_id'),'stage':('stage','pipeline_stage'),'model':('model',),'input_tokens':('input_tokens','prompt_tokens'),'output_tokens':('output_tokens','completion_tokens'),'total_tokens':('total_tokens',),'cost':('cost','estimated_cost_usd'),'created_at':('created_at','timestamp')})
    for t in ('tool_calls','session_tool_calls'):
        if t in s: tools+=load_table(conn,t,s[t],{'session_id':('session_id','run_id'),'tool_name':('tool_name','tool')})
    return build_model_usage_tool_call_cost_allocation_report(usage,tools,missing_tables=mt,missing_columns={},**kw)
def format_model_usage_tool_call_cost_allocation_json(r): return json_dumps(r)
def format_model_usage_tool_call_cost_allocation_text(r):
    lines=['Model Usage Tool Call Cost Allocation',f"Generated: {r['generated_at']}",f"Totals: usage={r['totals']['usage_rows']} groups={r['totals']['groups']}"]
    if r['missing_tables']: lines.append('Missing tables: '+', '.join(r['missing_tables']))
    if not r['groups']: lines.append(r['empty_state']['message']); return '\n'.join(lines)
    lines+=['','stage | tool | model | day | tokens | cost']
    for g in r['groups']: lines.append(f"{g['stage']} | {g['tool_name']} | {g['model']} | {g['day']} | {g['total_tokens']} | {g['estimated_cost']}")
    return '\n'.join(lines)
