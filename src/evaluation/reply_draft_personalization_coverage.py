"""Audit reply drafts for expected personalization signals."""
from ._focused_report_helpers import *
ARTIFACT_TYPE="reply_draft_personalization_coverage"; DEFAULT_MIN_COVERAGE=0.75
def build_reply_draft_personalization_coverage_report(rows, *, min_coverage=DEFAULT_MIN_COVERAGE, limit=DEFAULT_LIMIT, now=None, missing_tables=None, missing_columns=None):
 _nonneg('min_coverage',min_coverage); _positive('limit',limit); gen=_now(now); findings=[]
 for r in rows:
  text=_lower(r.get('draft_text') or r.get('text') or ''); missing=[]
  if _clean(r.get('target_handle')) and _lower(r.get('target_handle')).lstrip('@') not in text: missing.append('target_handle')
  if _clean(r.get('target_name')) and _lower(r.get('target_name')) not in text: missing.append('target_name')
  if not _clean(r.get('conversation_context') or r.get('topic')): missing.append('conversation_context')
  if not _clean(r.get('relationship_context')): missing.append('relationship_context')
  if not _clean(r.get('knowledge_source_ids') or r.get('source_urls')): missing.append('knowledge_links')
  score=(5-len(missing))/5
  if score<min_coverage: findings.append({'reason':'low_personalization_coverage','reply_id':r.get('reply_id') or r.get('id'),'target_handle':r.get('target_handle'),'missing_signals':missing,'coverage_score':score,'status':r.get('status'),'detail':'reply draft is missing personalization signals'})
 return _finish(ARTIFACT_TYPE,gen,{'min_coverage':min_coverage,'limit':limit},len(rows),findings,limit,missing_tables,missing_columns)
def build_reply_draft_personalization_coverage_report_from_db(db_or_conn, **kwargs):
 conn=_conn(db_or_conn); s=_schema(conn); table='reply_drafts' if 'reply_drafts' in s else 'reply_queue' if 'reply_queue' in s else None
 if table is None: return build_reply_draft_personalization_coverage_report([], missing_tables=['reply_drafts'], **kwargs)
 c=s[table]; sel=[_expr(c,'id',alias='reply_id'),_expr(c,'draft_text','reply_text','text',alias='draft_text'),_expr(c,'target_handle','target_author','handle',alias='target_handle'),_expr(c,'target_name','recipient_name',alias='target_name'),_expr(c,'conversation_context','inbound_text','topic',alias='conversation_context'),_expr(c,'relationship_context'),_expr(c,'knowledge_source_ids','knowledge_ids','source_urls',alias='knowledge_source_ids'),_expr(c,'status'),_expr(c,'created_at')]
 return build_reply_draft_personalization_coverage_report([dict(r) for r in conn.execute(f"SELECT {', '.join(sel)} FROM {table}")], **kwargs)
def format_reply_draft_personalization_coverage_json(report): return _json(report)
def format_reply_draft_personalization_coverage_text(report): return _text('Reply Draft Personalization Coverage', report)
