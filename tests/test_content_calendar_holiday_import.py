from __future__ import annotations
import importlib.util,json,sqlite3
from pathlib import Path
from evaluation.content_calendar_holiday_import import import_content_calendar_holidays,parse_holiday_payload,format_content_calendar_holiday_import_json,format_content_calendar_holiday_import_text
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_content_calendar_holidays.py"; spec=importlib.util.spec_from_file_location("import_content_calendar_holidays_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_holiday_import_csv_json_and_cli(tmp_path,capsys):
 rows=parse_holiday_payload("calendar_id,name,starts_on\ned,Launch Day,2026-05-24\n",region_default="US")
 assert rows[0]["ends_on"]=="2026-05-24" and rows[0]["region"]=="US"
 c=sqlite3.connect(":memory:"); r=import_content_calendar_holidays(c,rows); assert r["summary"]["inserted_count"]==1
 r2=import_content_calendar_holidays(c,parse_holiday_payload('[{"calendar_id":"ed","region":"US","name":"Launch Day","starts_on":"2026-05-24","category":"promo"}]'),dry_run=True); assert r2["summary"]["updated_count"]==1
 assert json.loads(format_content_calendar_holiday_import_json(r))["artifact_type"]=="content_calendar_holiday_import"; assert "Content Calendar Holiday" in format_content_calendar_holiday_import_text(r)
 db=tmp_path/"db.sqlite"; payload=tmp_path/"holidays.csv"; payload.write_text("name,starts_on\nTest,2026-05-25\n")
 assert script.main([str(payload),"--db",str(db),"--region-default","JP","--format","text"])==0; assert "Content Calendar Holiday" in capsys.readouterr().out
