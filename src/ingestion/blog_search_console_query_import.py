from __future__ import annotations
import csv,json
from io import StringIO
from typing import Any
from urllib.parse import urlsplit,urlunsplit
def _clean(v): return "" if v is None else str(v).strip()
def _url(v):
 p=urlsplit(_clean(v)); return urlunsplit((p.scheme.lower(),p.netloc.lower(),p.path or "/",p.query,"")) if p.scheme and p.netloc else _clean(v)
def _num(v,default=0.0):
 try: return float(str(v).strip().rstrip("%"))
 except Exception: return default
def _items(text):
 raw=text.strip()
 if not raw: return []
 if raw.startswith("{") and "\n" in raw: return [json.loads(l) for l in raw.splitlines() if l.strip()]
 if raw[0] in "[{":
  obj=json.loads(raw); return obj if isinstance(obj,list) else obj.get("rows") or obj.get("items") or [obj]
 return list(csv.DictReader(StringIO(text)))
def parse_blog_search_console_query_payload(text:str)->list[dict[str,Any]]:
 out=[]
 for r in _items(text):
  row={"observed_at":_clean(r.get("observed_at") or r.get("date")),"page_url":_url(r.get("page_url") or r.get("page")),"query":_clean(r.get("query")),"clicks":int(_num(r.get("clicks"),0)),"impressions":int(_num(r.get("impressions"),0)),"ctr":_num(r.get("ctr"),0.0),"position":_num(r.get("position"),0.0)}
  if not (row["observed_at"] and row["page_url"] and row["query"]): raise ValueError("observed_at, page_url, and query are required")
  out.append(row)
 return out
def import_blog_search_console_queries(conn,rows,dry_run=False,now=None):
 _create(conn); existing={tuple(x) for x in conn.execute("SELECT observed_at,page_url,query FROM blog_search_console_queries")}; ins=sum(1 for r in rows if (r["observed_at"],r["page_url"],r["query"]) not in existing)
 if not dry_run: conn.executemany("INSERT INTO blog_search_console_queries VALUES (:observed_at,:page_url,:query,:clicks,:impressions,:ctr,:position) ON CONFLICT(observed_at,page_url,query) DO UPDATE SET clicks=excluded.clicks,impressions=excluded.impressions,ctr=excluded.ctr,position=excluded.position",rows); conn.commit()
 return {"artifact_type":"blog_search_console_query_import","dry_run":dry_run,"summary":{"parsed_count":len(rows),"inserted_count":ins,"updated_count":len(rows)-ins,"applied_count":0 if dry_run else len(rows)},"rows":rows}
def format_blog_search_console_query_import_json(r): return json.dumps(r,indent=2,sort_keys=True)
def format_blog_search_console_query_import_text(r): return f"Blog Search Console Query Import\nTotals: parsed={r['summary']['parsed_count']} applied={r['summary']['applied_count']}"
def _create(c): c.execute("CREATE TABLE IF NOT EXISTS blog_search_console_queries (observed_at TEXT, page_url TEXT, query TEXT, clicks INTEGER, impressions INTEGER, ctr REAL, position REAL, PRIMARY KEY(observed_at,page_url,query))")
