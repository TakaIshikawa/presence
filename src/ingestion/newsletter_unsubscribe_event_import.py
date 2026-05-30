from __future__ import annotations
import csv,json
from io import StringIO
from typing import Any
def _c(v): return "" if v is None else str(v).strip()
def _items(t):
 raw=t.strip()
 if raw.startswith("{") and "\n" in raw: return [json.loads(l) for l in raw.splitlines() if l.strip()]
 if raw[0] in "[{":
  o=json.loads(raw); return o if isinstance(o,list) else o.get("rows") or o.get("items") or [o]
 return list(csv.DictReader(StringIO(t)))
def parse_newsletter_unsubscribe_event_payload(text:str)->list[dict[str,Any]]:
 out=[]
 for r in _items(text):
  meta=r.get("metadata") or {}; row={"subscriber_email":_c(r.get("subscriber_email") or r.get("email")).lower(),"subscriber_id":_c(r.get("subscriber_id") or r.get("id")),"issue_id":_c(r.get("issue_id")) or None,"reason":_c(r.get("reason")) or None,"source":_c(r.get("source")) or None,"unsubscribed_at":_c(r.get("unsubscribed_at") or r.get("event_at")),"metadata":json.dumps(meta,sort_keys=True) if not isinstance(meta,str) else meta}
  if not (row["subscriber_email"] or row["subscriber_id"]) or not row["unsubscribed_at"]: raise ValueError("subscriber_email or subscriber_id plus unsubscribed_at are required")
  out.append(row)
 return out
def import_newsletter_unsubscribe_events(conn,rows,dry_run=False,now=None):
 _create(conn); existing={tuple(x) for x in conn.execute("SELECT COALESCE(subscriber_id,''),COALESCE(subscriber_email,''),unsubscribed_at FROM newsletter_unsubscribe_events")}; ins=sum(1 for r in rows if (r["subscriber_id"] or "",r["subscriber_email"] or "",r["unsubscribed_at"]) not in existing)
 if not dry_run: conn.executemany("INSERT INTO newsletter_unsubscribe_events VALUES (:subscriber_email,:subscriber_id,:issue_id,:reason,:source,:unsubscribed_at,:metadata) ON CONFLICT(subscriber_id,subscriber_email,unsubscribed_at) DO UPDATE SET issue_id=excluded.issue_id,reason=excluded.reason,source=excluded.source,metadata=excluded.metadata",rows); conn.commit()
 return {"artifact_type":"newsletter_unsubscribe_event_import","dry_run":dry_run,"summary":{"parsed_count":len(rows),"inserted_count":ins,"updated_count":len(rows)-ins,"applied_count":0 if dry_run else len(rows)},"rows":rows}
def format_newsletter_unsubscribe_event_import_json(r): return json.dumps(r,indent=2,sort_keys=True)
def format_newsletter_unsubscribe_event_import_text(r): return f"Newsletter Unsubscribe Event Import\nTotals: parsed={r['summary']['parsed_count']} applied={r['summary']['applied_count']}"
def _create(c): c.execute("CREATE TABLE IF NOT EXISTS newsletter_unsubscribe_events (subscriber_email TEXT NOT NULL DEFAULT '', subscriber_id TEXT NOT NULL DEFAULT '', issue_id TEXT, reason TEXT, source TEXT, unsubscribed_at TEXT NOT NULL, metadata TEXT, PRIMARY KEY(subscriber_id,subscriber_email,unsubscribed_at))")
