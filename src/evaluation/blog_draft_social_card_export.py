from __future__ import annotations
import json, sqlite3
from typing import Any
PLATFORMS=('x','linkedin','bluesky','newsletter')
def _clean(v): return '' if v is None else str(v).strip()
def _conn(db): c=getattr(db,'conn',db); c.row_factory=sqlite3.Row; return c
def _schema(c): return {str(r[0]):{str(x[1]) for x in c.execute(f'PRAGMA table_info({r[0]})')} for r in c.execute("SELECT name FROM sqlite_master WHERE type IN ('table','view')")}
def _col(cols,*names,fallback='NULL'): return next((n for n in names if n in cols), fallback)
def _trunc(s,n):
    s=_clean(s)
    return s if len(s)<=n else s[:max(0,n-1)].rstrip()+'…'
def build_blog_draft_social_card_export(rows, *, platform=None, limit=100) -> dict[str, Any]:
    if limit<=0: raise ValueError('limit must be positive')
    plats=platform or list(PLATFORMS); packets=[]
    for r in rows[:limit]:
        title=_clean(r.get('title')); desc=_clean(r.get('description') or r.get('summary') or r.get('body'))
        packets.append({'draft_id':r.get('id'),'title':title,'description':desc,'canonical_url':_clean(r.get('canonical_url') or r.get('url')),'image_url':_clean(r.get('image_url')),'alt_text':_clean(r.get('alt_text')),'tags':[t.strip() for t in _clean(r.get('tags')).split(',') if t.strip()],'previews':{p:{'title':_trunc(title,70 if p!='x' else 60),'description':_trunc(desc,200 if p!='x' else 125)} for p in plats}})
    return {'artifact_type':'blog_draft_social_card_export','filters':{'platform':plats,'limit':limit},'totals':{'drafts':len(rows),'packets':len(packets)},'packets':packets,'findings':packets}
def build_blog_draft_social_card_export_report_from_db(db, **kw):
    c=_conn(db); s=_schema(c); table='blog_drafts' if 'blog_drafts' in s else 'generated_content' if 'generated_content' in s else None
    if not table: return {'artifact_type':'blog_draft_social_card_export','filters':kw,'totals':{'drafts':0,'packets':0},'packets':[],'findings':[],'missing_tables':['blog_drafts|generated_content']}
    cols=s[table]; rows=[dict(r) for r in c.execute(f"SELECT {_col(cols,'id',fallback='rowid')} AS id,{_col(cols,'title')} AS title,{_col(cols,'description','summary')} AS description,{_col(cols,'body','content')} AS body,{_col(cols,'canonical_url','url')} AS canonical_url,{_col(cols,'image_url')} AS image_url,{_col(cols,'alt_text','image_alt_text')} AS alt_text,{_col(cols,'tags')} AS tags FROM {table}")]
    return build_blog_draft_social_card_export(rows, **kw)
def format_blog_draft_social_card_export_json(r): return json.dumps(r, indent=2, sort_keys=True)
def format_blog_draft_social_card_export_text(r): return '\n'.join(['Blog Draft Social Card Export', 'Packets: '+str(r['totals']['packets'])]+[f"  - {p['draft_id']} {p['title']}" for p in r.get('packets',[])])
