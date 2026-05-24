"""Import content calendar holidays and observances."""
from __future__ import annotations
import csv,json
from datetime import date
from io import StringIO
from typing import Any
from ._report_utils import clean,connection,json_dumps,now_iso
ARTIFACT_TYPE="content_calendar_holiday_import"
def parse_holiday_payload(text:str,*,region_default:str="global")->list[dict[str,Any]]:
 raw=text.strip()
 if raw.startswith("["): items=json.loads(raw)
 else: items=list(csv.DictReader(StringIO(text)))
 out=[]
 for i in items:
  starts=clean(i.get("starts_on") or i.get("date")); ends=clean(i.get("ends_on")) or starts
  date.fromisoformat(starts); date.fromisoformat(ends)
  out.append({"calendar_id":clean(i.get("calendar_id"),"default"),"region":clean(i.get("region"),region_default),"name":clean(i.get("name")),"category":clean(i.get("category"),"observance"),"starts_on":starts,"ends_on":ends,"source_url":clean(i.get("source_url")) or None,"notes":clean(i.get("notes")) or None})
 return out
def import_content_calendar_holidays(db_or_conn:Any,rows:list[dict[str,Any]],*,dry_run:bool=False,now:Any=None)->dict[str,Any]:
 conn=connection(db_or_conn); _create(conn); existing={tuple(r) for r in conn.execute("SELECT calendar_id,region,name,starts_on FROM content_calendar_holidays")}
 inserted=sum(1 for r in rows if (r["calendar_id"],r["region"],r["name"],r["starts_on"]) not in existing); updated=len(rows)-inserted
 if not dry_run:
  conn.executemany("""INSERT INTO content_calendar_holidays (calendar_id,region,name,category,starts_on,ends_on,source_url,notes) VALUES (:calendar_id,:region,:name,:category,:starts_on,:ends_on,:source_url,:notes)
  ON CONFLICT(calendar_id,region,name,starts_on) DO UPDATE SET category=excluded.category,ends_on=excluded.ends_on,source_url=excluded.source_url,notes=excluded.notes""",rows); conn.commit()
 return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(now),"dry_run":dry_run,"summary":{"input_count":len(rows),"inserted_count":inserted,"updated_count":updated,"applied_count":0 if dry_run else len(rows)},"holidays":rows}
def format_content_calendar_holiday_import_json(report:dict[str,Any])->str: return json_dumps(report)
def format_content_calendar_holiday_import_text(report:dict[str,Any])->str:
 s=report["summary"]; lines=["Content Calendar Holiday Import",f"Generated: {report['generated_at']}",f"Dry run: {report['dry_run']}",f"Totals: input={s['input_count']} inserted={s['inserted_count']} updated={s['updated_count']} applied={s['applied_count']}"]
 for r in report["holidays"]: lines.append(f"- {r['calendar_id']} {r['region']} {r['starts_on']} {r['name']}")
 return "\n".join(lines)
def _create(conn:Any)->None:
 conn.execute("""CREATE TABLE IF NOT EXISTS content_calendar_holidays (calendar_id TEXT NOT NULL, region TEXT NOT NULL, name TEXT NOT NULL, category TEXT, starts_on TEXT NOT NULL, ends_on TEXT, source_url TEXT, notes TEXT, UNIQUE(calendar_id,region,name,starts_on))""")
