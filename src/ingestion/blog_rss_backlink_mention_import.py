from __future__ import annotations
import csv,json
from io import StringIO
from urllib.parse import urlsplit,urlunsplit
def _c(v): return "" if v is None else str(v).strip()
def _url(v):
 p=urlsplit(_c(v)); return urlunsplit((p.scheme.lower(),p.netloc.lower(),p.path or "/",p.query,"")) if p.scheme and p.netloc else _c(v)
def _domain(u):
 p=urlsplit(_url(u)); h=p.netloc.lower(); return h[4:] if h.startswith("www.") else h
def _items(t):
 raw=t.strip()
 if raw.startswith("{") and "\n" in raw: return [json.loads(l) for l in raw.splitlines() if l.strip()]
 if raw[0] in "[{":
  o=json.loads(raw); return o if isinstance(o,list) else o.get("rows") or o.get("items") or [o]
 return list(csv.DictReader(StringIO(t)))
def parse_blog_rss_backlink_mention_payload(t):
 out=[]
 for r in _items(t):
  su=_url(r.get("source_url")); tu=_url(r.get("target_url")); row={"source_url":su,"target_url":tu,"source_title":_c(r.get("source_title") or r.get("title")) or None,"source_domain":_c(r.get("source_domain")) or _domain(su),"anchor_text":_c(r.get("anchor_text")) or None,"first_seen_at":_c(r.get("first_seen_at")) or None,"last_seen_at":_c(r.get("last_seen_at") or r.get("seen_at")) or None,"status":_c(r.get("status"),) or "active"}
  if not su or not tu: raise ValueError("source_url and target_url are required")
  out.append(row)
 return out
def import_blog_rss_backlink_mentions(conn,rows,dry_run=False,now=None):
 _create(conn); existing={tuple(x) for x in conn.execute("SELECT source_url,target_url FROM blog_rss_backlink_mentions")}; ins=sum(1 for r in rows if (r["source_url"],r["target_url"]) not in existing)
 if not dry_run: conn.executemany("INSERT INTO blog_rss_backlink_mentions VALUES (:source_url,:target_url,:source_title,:source_domain,:anchor_text,:first_seen_at,:last_seen_at,:status) ON CONFLICT(source_url,target_url) DO UPDATE SET source_title=excluded.source_title,source_domain=excluded.source_domain,anchor_text=excluded.anchor_text,last_seen_at=excluded.last_seen_at,status=excluded.status",rows); conn.commit()
 return {"artifact_type":"blog_rss_backlink_mention_import","dry_run":dry_run,"summary":{"parsed_count":len(rows),"inserted_count":ins,"updated_count":len(rows)-ins,"applied_count":0 if dry_run else len(rows)},"rows":rows}
def format_blog_rss_backlink_mention_import_json(r): return json.dumps(r,indent=2,sort_keys=True)
def format_blog_rss_backlink_mention_import_text(r): return f"Blog RSS Backlink Mention Import\nTotals: parsed={r['summary']['parsed_count']} applied={r['summary']['applied_count']}"
def _create(c): c.execute("CREATE TABLE IF NOT EXISTS blog_rss_backlink_mentions (source_url TEXT, target_url TEXT, source_title TEXT, source_domain TEXT, anchor_text TEXT, first_seen_at TEXT, last_seen_at TEXT, status TEXT, PRIMARY KEY(source_url,target_url))")
