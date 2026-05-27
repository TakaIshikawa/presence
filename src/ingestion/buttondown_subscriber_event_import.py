from __future__ import annotations
import csv,json
from io import StringIO
def _c(v): return "" if v is None else str(v).strip()
def _items(t):
 raw=t.strip()
 if raw.startswith("{") and "\n" in raw: return [json.loads(l) for l in raw.splitlines() if l.strip()]
 if raw[0] in "[{":
  o=json.loads(raw); return o if isinstance(o,list) else o.get("rows") or o.get("items") or [o]
 return list(csv.DictReader(StringIO(t)))
def parse_buttondown_subscriber_event_payload(t):
 out=[]
 for r in _items(t):
  tags=r.get("tags") or []; meta=r.get("metadata") or {}
  if isinstance(tags,str): tags=[x.strip() for x in tags.split(",") if x.strip()]
  row={"subscriber_id":_c(r.get("subscriber_id") or r.get("id")),"email":_c(r.get("email")).lower(),"event_type":_c(r.get("event_type") or r.get("type")).lower(),"event_at":_c(r.get("event_at") or r.get("created_at")),"source":_c(r.get("source")) or None,"tags":json.dumps(sorted(tags),sort_keys=True),"metadata":json.dumps(meta,sort_keys=True) if not isinstance(meta,str) else meta}
  if not (row["subscriber_id"] or row["email"]) or not row["event_type"] or not row["event_at"]: raise ValueError("subscriber_id or email plus event_type and event_at are required")
  out.append(row)
 return out
def import_buttondown_subscriber_events(conn,rows,dry_run=False,now=None):
 _create(conn); existing={tuple(x) for x in conn.execute("SELECT subscriber_id,email,event_type,event_at FROM buttondown_subscriber_events")}; ins=sum(1 for r in rows if (r["subscriber_id"],r["email"],r["event_type"],r["event_at"]) not in existing)
 if not dry_run: conn.executemany("INSERT INTO buttondown_subscriber_events VALUES (:subscriber_id,:email,:event_type,:event_at,:source,:tags,:metadata) ON CONFLICT(subscriber_id,email,event_type,event_at) DO UPDATE SET source=excluded.source,tags=excluded.tags,metadata=excluded.metadata",rows); conn.commit()
 return {"artifact_type":"buttondown_subscriber_event_import","dry_run":dry_run,"summary":{"parsed_count":len(rows),"inserted_count":ins,"updated_count":len(rows)-ins,"applied_count":0 if dry_run else len(rows)},"rows":rows}
def format_buttondown_subscriber_event_import_json(r): return json.dumps(r,indent=2,sort_keys=True)
def format_buttondown_subscriber_event_import_text(r): return f"Buttondown Subscriber Event Import\nTotals: parsed={r['summary']['parsed_count']} applied={r['summary']['applied_count']}"
def _create(c): c.execute("CREATE TABLE IF NOT EXISTS buttondown_subscriber_events (subscriber_id TEXT NOT NULL DEFAULT '', email TEXT NOT NULL DEFAULT '', event_type TEXT, event_at TEXT, source TEXT, tags TEXT, metadata TEXT, PRIMARY KEY(subscriber_id,email,event_type,event_at))")
