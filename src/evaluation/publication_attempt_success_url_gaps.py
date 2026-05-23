"""Detect successful publication attempts without durable URL evidence."""
from ._focused_report_helpers import *
ARTIFACT_TYPE="publication_attempt_success_url_gaps"; DEFAULT_WINDOW_DAYS=30; URL_RE=re.compile(r"https?://[^\s\"']+",re.I)
def build_publication_attempt_success_url_gaps_report(rows, *, platform=None, window_days=DEFAULT_WINDOW_DAYS, limit=DEFAULT_LIMIT, now=None, missing_tables=None, missing_columns=None):
 _positive('window_days',window_days); _positive('limit',limit); gen=_now(now); cutoff=gen-timedelta(days=window_days); findings=[]; scoped=[]
 for r in rows:
  if platform and _lower(r.get('platform'))!=platform.lower(): continue
  dt=_dt(r.get('attempted_at') or r.get('created_at'))
  if dt and dt<cutoff: continue
  scoped.append(r); status=_lower(r.get('status')); success=(r.get('success') in (1,True,'1','true','True')) or status in {'success','succeeded','published','complete','completed'}
  if not success: continue
  url=_clean(r.get('published_url') or r.get('permalink') or r.get('url')); raw=_clean(r.get('raw_response') or r.get('response_payload') or r.get('response_metadata'))
  base={'attempt_id':r.get('attempt_id') or r.get('id'),'content_id':r.get('content_id'),'platform':r.get('platform'),'published_url':url or None,'content_title':r.get('title')}
  if not url: findings.append({**base,'reason':'missing_success_url','detail':'successful attempt lacks published_url/permalink'})
  elif not url.lower().startswith(('http://','https://')): findings.append({**base,'reason':'invalid_success_url','detail':'success URL is not http(s)'})
  if raw and not URL_RE.search(raw): findings.append({**base,'reason':'raw_response_url_gap','detail':'raw response lacks URL-like value'})
  if r.get('content_id') is not None and not (url or _clean(r.get('content_url'))): findings.append({**base,'reason':'content_link_gap','detail':'linked content has no durable URL evidence'})
 return _finish(ARTIFACT_TYPE,gen,{'platform':platform,'window_days':window_days,'limit':limit},len(scoped),findings,limit,missing_tables,missing_columns)
def build_publication_attempt_success_url_gaps_report_from_db(db_or_conn, **kwargs):
 conn=_conn(db_or_conn); s=_schema(conn); c=s.get('publication_attempts')
 if c is None: return build_publication_attempt_success_url_gaps_report([], missing_tables=['publication_attempts'], **kwargs)
 g=s.get('generated_content',set()); sel=[_expr(c,'id',alias='attempt_id'),_expr(c,'content_id'),_expr(c,'platform'),_expr(c,'status'),_expr(c,'success'),_expr(c,'published_url','permalink','url',alias='published_url'),_expr(c,'permalink'),_expr(c,'raw_response','response_payload','response_metadata',alias='raw_response'),_expr(c,'attempted_at','created_at',alias='attempted_at')]
 if g and 'content_id' in c and 'id' in g:
  sel += [_expr(g,'title','content_type',alias='title',prefix='gc.'),_expr(g,'published_url','permalink','url',alias='content_url',prefix='gc.')]
  rows=[dict(r) for r in conn.execute(f"SELECT {', '.join(sel)} FROM publication_attempts pa LEFT JOIN generated_content gc ON pa.content_id=gc.id")]
 else: rows=[dict(r) for r in conn.execute(f"SELECT {', '.join(sel)} FROM publication_attempts pa")]
 return build_publication_attempt_success_url_gaps_report(rows, **kwargs)
def format_publication_attempt_success_url_gaps_json(report): return _json(report)
def format_publication_attempt_success_url_gaps_text(report): return _text('Publication Attempt Success URL Gaps', report)
