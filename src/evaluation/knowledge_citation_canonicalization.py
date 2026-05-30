"""Group knowledge citations that only differ by canonical URL noise."""
from ._focused_report_helpers import *
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
ARTIFACT_TYPE="knowledge_citation_canonicalization"; TRACK={'utm_source','utm_medium','utm_campaign','utm_term','utm_content','fbclid','gclid'}; DEFAULT_MIN_VARIANTS=2
def _canon(url, include_fragments=False):
 u=_clean(url);
 if not u: return ''
 p=urlsplit(u); q=urlencode([(k,v) for k,v in parse_qsl(p.query,keep_blank_values=True) if k.lower() not in TRACK]); path=p.path.rstrip('/') or '/'
 return urlunsplit(('https',p.netloc.lower(),path,q,p.fragment if include_fragments else ''))
def build_knowledge_citation_canonicalization_report(rows, *, min_variants=DEFAULT_MIN_VARIANTS, include_fragments=False, limit=DEFAULT_LIMIT, now=None, missing_tables=None, missing_columns=None):
 _positive('min_variants',min_variants); _positive('limit',limit); gen=_now(now); groups=defaultdict(list)
 for r in rows:
  url=r.get('url') or r.get('source_url') or r.get('citation_url'); c=_canon(url,include_fragments);
  if c: groups[c].append(r)
 findings=[]
 for c,rs in groups.items():
  vars=sorted({_clean(r.get('url') or r.get('source_url') or r.get('citation_url')) for r in rs if _clean(r.get('url') or r.get('source_url') or r.get('citation_url'))})
  if len(vars)>=min_variants:
   reason='tracking_param_variant' if any(('utm_' in v or 'gclid=' in v or 'fbclid=' in v) for v in vars) else 'scheme_slash_variant'
   findings.append({'reason':reason,'canonical_url':c,'variant_urls':vars,'source_ids':[r.get('source_id') or r.get('id') for r in rs],'content_ids':[r.get('content_id') for r in rs if r.get('content_id') is not None],'variant_count':len(vars),'detail':'canonical URL variants found'})
 return _finish(ARTIFACT_TYPE,gen,{'min_variants':min_variants,'include_fragments':include_fragments,'limit':limit},len(rows),findings,limit,missing_tables,missing_columns)
def build_knowledge_citation_canonicalization_report_from_db(db_or_conn, **kwargs):
 conn=_conn(db_or_conn); s=_schema(conn); rows=[]; missing=[]
 if 'knowledge_sources' in s:
  c=s['knowledge_sources']; rows += [dict(r) for r in conn.execute(f"SELECT {_expr(c,'id',alias='source_id')}, {_expr(c,'url','source_url',alias='url')} FROM knowledge_sources")]
 else: missing.append('knowledge_sources')
 for table in ('content_claims','generated_content'):
  c=s.get(table)
  if c and ({'citation_url','source_url','url'} & c): rows += [dict(r) for r in conn.execute(f"SELECT {_expr(c,'id',alias='content_id')}, {_expr(c,'citation_url','source_url','url',alias='url')} FROM {table}")]
 return build_knowledge_citation_canonicalization_report(rows, missing_tables=missing, **kwargs)
def format_knowledge_citation_canonicalization_json(report): return _json(report)
def format_knowledge_citation_canonicalization_text(report): return _text('Knowledge Citation Canonicalization', report)
