from __future__ import annotations
import csv,json
from io import StringIO
def _c(v): return "" if v is None else str(v).strip()
def _i(v):
 try: return int(v)
 except Exception: return 0
def _b(v): return str(v).strip().lower() in {"1","true","yes","pr","pull_request"}
def _items(t):
 raw=t.strip()
 if raw.startswith("{") and "\n" in raw: return [json.loads(l) for l in raw.splitlines() if l.strip()]
 if raw[0] in "[{":
  o=json.loads(raw); return o if isinstance(o,list) else o.get("rows") or o.get("items") or [o]
 return list(csv.DictReader(StringIO(t)))
def parse_github_issue_comment_activity_payload(t):
 out=[]
 for r in _items(t):
  row={"repository":_c(r.get("repository")).lower(),"issue_number":_i(r.get("issue_number")),"comment_id":_i(r.get("comment_id") or r.get("id")),"author":_c(r.get("author") or r.get("user")),"body":_c(r.get("body")),"comment_url":_c(r.get("comment_url") or r.get("url")) or None,"created_at":_c(r.get("created_at")),"updated_at":_c(r.get("updated_at")) or None,"is_pr":1 if _b(r.get("is_pr")) else 0}
  if not (row["repository"] and row["issue_number"] and row["comment_id"] and row["author"] and row["created_at"]): raise ValueError("repository, issue_number, comment_id, author, and created_at are required")
  out.append(row)
 return out
def import_github_issue_comment_activity(conn,rows,dry_run=False,now=None):
 _create(conn); existing={tuple(x) for x in conn.execute("SELECT repository,comment_id FROM github_issue_comment_activity")}; ins=sum(1 for r in rows if (r["repository"],r["comment_id"]) not in existing)
 if not dry_run: conn.executemany("INSERT INTO github_issue_comment_activity VALUES (:repository,:issue_number,:comment_id,:author,:body,:comment_url,:created_at,:updated_at,:is_pr) ON CONFLICT(repository,comment_id) DO UPDATE SET body=excluded.body,updated_at=excluded.updated_at,comment_url=excluded.comment_url,is_pr=excluded.is_pr",rows); conn.commit()
 return {"artifact_type":"github_issue_comment_activity_import","dry_run":dry_run,"summary":{"parsed_count":len(rows),"inserted_count":ins,"updated_count":len(rows)-ins,"applied_count":0 if dry_run else len(rows)},"rows":rows}
def format_github_issue_comment_activity_import_json(r): return json.dumps(r,indent=2,sort_keys=True)
def format_github_issue_comment_activity_import_text(r): return f"GitHub Issue Comment Activity Import\nTotals: parsed={r['summary']['parsed_count']} applied={r['summary']['applied_count']}"
def _create(c): c.execute("CREATE TABLE IF NOT EXISTS github_issue_comment_activity (repository TEXT, issue_number INTEGER, comment_id INTEGER, author TEXT, body TEXT, comment_url TEXT, created_at TEXT, updated_at TEXT, is_pr INTEGER, PRIMARY KEY(repository,comment_id))")
