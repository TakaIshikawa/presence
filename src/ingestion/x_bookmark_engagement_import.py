from __future__ import annotations
import csv,json
from io import StringIO
def _c(v): return "" if v is None else str(v).strip()
def _i(v):
 try: return int(v)
 except Exception: return 0
def _items(t):
 raw=t.strip()
 if raw.startswith("{") and "\n" in raw: return [json.loads(l) for l in raw.splitlines() if l.strip()]
 if raw[0] in "[{":
  o=json.loads(raw); return o if isinstance(o,list) else o.get("rows") or o.get("items") or [o]
 return list(csv.DictReader(StringIO(t)))
def parse_x_bookmark_engagement_payload(t):
 out=[]
 for r in _items(t):
  row={"post_id":_c(r.get("post_id") or r.get("tweet_id")),"bookmarked_count":_i(r.get("bookmarked_count") or r.get("bookmarks")),"likes":_i(r.get("likes")),"reposts":_i(r.get("reposts")),"replies":_i(r.get("replies")),"impressions":_i(r.get("impressions")),"fetched_at":_c(r.get("fetched_at") or r.get("observed_at")),"platform_account":_c(r.get("platform_account") or r.get("account")) or None}
  if not row["post_id"] or not row["fetched_at"]: raise ValueError("post_id/tweet_id and fetched_at are required")
  out.append(row)
 return out
def import_x_bookmark_engagement(conn,rows,dry_run=False,now=None):
 _create(conn); existing={tuple(x) for x in conn.execute("SELECT post_id,fetched_at FROM x_bookmark_engagement_snapshots")}; ins=sum(1 for r in rows if (r["post_id"],r["fetched_at"]) not in existing)
 if not dry_run: conn.executemany("INSERT INTO x_bookmark_engagement_snapshots VALUES (:post_id,:bookmarked_count,:likes,:reposts,:replies,:impressions,:fetched_at,:platform_account) ON CONFLICT(post_id,fetched_at) DO UPDATE SET bookmarked_count=excluded.bookmarked_count,likes=excluded.likes,reposts=excluded.reposts,replies=excluded.replies,impressions=excluded.impressions,platform_account=excluded.platform_account",rows); conn.commit()
 return {"artifact_type":"x_bookmark_engagement_import","dry_run":dry_run,"summary":{"parsed_count":len(rows),"inserted_count":ins,"updated_count":len(rows)-ins,"applied_count":0 if dry_run else len(rows)},"rows":rows}
def format_x_bookmark_engagement_import_json(r): return json.dumps(r,indent=2,sort_keys=True)
def format_x_bookmark_engagement_import_text(r): return f"X Bookmark Engagement Import\nTotals: parsed={r['summary']['parsed_count']} applied={r['summary']['applied_count']}"
def _create(c): c.execute("CREATE TABLE IF NOT EXISTS x_bookmark_engagement_snapshots (post_id TEXT, bookmarked_count INTEGER, likes INTEGER, reposts INTEGER, replies INTEGER, impressions INTEGER, fetched_at TEXT, platform_account TEXT, PRIMARY KEY(post_id,fetched_at))")
