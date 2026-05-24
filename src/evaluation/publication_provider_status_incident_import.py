"""Import provider status incident exports."""
from __future__ import annotations
import hashlib,json
from typing import Any
from ._report_utils import clean,connection,json_dumps,now_iso
ARTIFACT_TYPE="publication_provider_status_incident_import"
def parse_incident_payload(text:str,provider:str)->list[dict[str,Any]]:
 raw=text.strip(); items=[json.loads(line) for line in raw.splitlines() if line.strip()] if not raw.startswith("[") else json.loads(raw)
 if not isinstance(items,list): raise ValueError("incident payload must be a JSON array or JSONL")
 out=[]
 for item in items:
  if not isinstance(item,dict): continue
  comps=item.get("affected_components") or item.get("components") or []
  if isinstance(comps,str): comps=[comps]
  raw_json=json.dumps(item,sort_keys=True,separators=(",",":"))
  out.append({"provider":provider,"incident_id":clean(item.get("incident_id") or item.get("id")),"title":clean(item.get("title") or item.get("name")),"status":clean(item.get("status"),"unknown"),"impact":clean(item.get("impact"),"unknown"),"started_at":clean(item.get("started_at") or item.get("created_at")) or None,"resolved_at":clean(item.get("resolved_at") or item.get("updated_at")) or None,"affected_components":json.dumps(sorted(clean(c) for c in comps if clean(c)),sort_keys=True),"source_url":clean(item.get("source_url") or item.get("url")) or None,"raw_payload_hash":hashlib.sha256(raw_json.encode()).hexdigest()})
 return out
def import_publication_provider_status_incidents(db_or_conn:Any,rows:list[dict[str,Any]],*,dry_run:bool=False,now:Any=None)->dict[str,Any]:
 conn=connection(db_or_conn); _create(conn); existing={r[0] for r in conn.execute("SELECT provider||':'||incident_id FROM publication_provider_incidents")}
 inserted=sum(1 for r in rows if f"{r['provider']}:{r['incident_id']}" not in existing); updated=len(rows)-inserted
 if not dry_run:
  conn.executemany("""INSERT INTO publication_provider_incidents (provider,incident_id,title,status,impact,started_at,resolved_at,affected_components,source_url,raw_payload_hash)
  VALUES (:provider,:incident_id,:title,:status,:impact,:started_at,:resolved_at,:affected_components,:source_url,:raw_payload_hash)
  ON CONFLICT(provider,incident_id) DO UPDATE SET title=excluded.title,status=excluded.status,impact=excluded.impact,started_at=excluded.started_at,resolved_at=excluded.resolved_at,affected_components=excluded.affected_components,source_url=excluded.source_url,raw_payload_hash=excluded.raw_payload_hash""",rows); conn.commit()
 return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(now),"dry_run":dry_run,"summary":{"input_count":len(rows),"inserted_count":inserted,"updated_count":updated,"applied_count":0 if dry_run else len(rows)},"incidents":rows}
def format_publication_provider_status_incident_import_json(report:dict[str,Any])->str: return json_dumps(report)
def format_publication_provider_status_incident_import_text(report:dict[str,Any])->str:
 s=report["summary"]; lines=["Publication Provider Status Incident Import",f"Generated: {report['generated_at']}",f"Dry run: {report['dry_run']}",f"Totals: input={s['input_count']} inserted={s['inserted_count']} updated={s['updated_count']} applied={s['applied_count']}"]
 for r in report["incidents"]: lines.append(f"- {r['provider']}:{r['incident_id']} {r['status']} {r['impact']} {r['title']}")
 return "\n".join(lines)
def _create(conn:Any)->None:
 conn.execute("""CREATE TABLE IF NOT EXISTS publication_provider_incidents (provider TEXT NOT NULL, incident_id TEXT NOT NULL, title TEXT, status TEXT, impact TEXT, started_at TEXT, resolved_at TEXT, affected_components TEXT, source_url TEXT, raw_payload_hash TEXT, PRIMARY KEY(provider, incident_id))""")
