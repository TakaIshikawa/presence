"""Estimate cache savings and low cache-hit groups from model usage."""
from ._focused_report_helpers import *
ARTIFACT_TYPE="model_usage_cache_savings"; DEFAULT_MIN_INPUT_TOKENS=1000; DEFAULT_MIN_CACHE_RATIO=0.2; DEFAULT_WINDOW_DAYS=30
def build_model_usage_cache_savings_report(rows, *, min_input_tokens=DEFAULT_MIN_INPUT_TOKENS, min_cache_ratio=DEFAULT_MIN_CACHE_RATIO, window_days=DEFAULT_WINDOW_DAYS, limit=DEFAULT_LIMIT, now=None, missing_tables=None, missing_columns=None):
 _nonneg('min_input_tokens',min_input_tokens); _nonneg('min_cache_ratio',min_cache_ratio); _positive('window_days',window_days); _positive('limit',limit); gen=_now(now); cutoff=gen-timedelta(days=window_days); agg=defaultdict(lambda:{'input':0,'cached':0,'output':0,'cost':0.0})
 for r in rows:
  dt=_dt(r.get('created_at'))
  if dt and dt<cutoff: continue
  key=(r.get('provider') or 'unknown', r.get('model') or 'unknown', r.get('prompt_name') or 'unknown'); a=agg[key]; a['input']+=_int(r.get('input_tokens')) or 0; a['cached']+=_int(r.get('cached_input_tokens')) or 0; a['output']+=_int(r.get('output_tokens')) or 0; a['cost']+=_float(r.get('cost_usd') or r.get('cost')) or 0.0
 findings=[]; tin=sum(a['input'] for a in agg.values()); tc=sum(a['cached'] for a in agg.values()); cost=sum(a['cost'] for a in agg.values()); saved=(cost/tin*tc) if tin else 0.0
 for (provider,model,prompt),a in agg.items():
  ratio=a['cached']/a['input'] if a['input'] else 0
  if a['input']>=min_input_tokens and ratio<min_cache_ratio: findings.append({'reason':'low_cache_hit_ratio','provider':provider,'model':model,'prompt_name':prompt,'total_input_tokens':a['input'],'cached_input_tokens':a['cached'],'cache_hit_ratio':ratio,'estimated_saved_cost_usd':round((a['cost']/a['input']*a['cached']) if a['input'] else 0,6),'detail':'cache hit ratio below threshold'})
 return _finish(ARTIFACT_TYPE,gen,{'min_input_tokens':min_input_tokens,'min_cache_ratio':min_cache_ratio,'window_days':window_days,'limit':limit},sum(a['input'] for a in agg.values()),findings,limit,missing_tables,missing_columns,{'total_input_tokens':tin,'cached_input_tokens':tc,'cache_hit_ratio':tc/tin if tin else 0,'estimated_saved_cost_usd':round(saved,6)})
def build_model_usage_cache_savings_report_from_db(db_or_conn, **kwargs):
 conn=_conn(db_or_conn); s=_schema(conn); c=s.get('model_usage')
 if c is None: return build_model_usage_cache_savings_report([], missing_tables=['model_usage'], **kwargs)
 sel=[_expr(c,'provider'),_expr(c,'model'),_expr(c,'prompt_name','prompt_type',alias='prompt_name'),_expr(c,'input_tokens'),_expr(c,'output_tokens'),_expr(c,'cached_input_tokens','cache_input_tokens',alias='cached_input_tokens'),_expr(c,'cost_usd','cost',alias='cost_usd'),_expr(c,'created_at')]
 return build_model_usage_cache_savings_report([dict(r) for r in conn.execute(f"SELECT {', '.join(sel)} FROM model_usage")], **kwargs)
def format_model_usage_cache_savings_json(report): return _json(report)
def format_model_usage_cache_savings_text(report): return _text('Model Usage Cache Savings', report)
