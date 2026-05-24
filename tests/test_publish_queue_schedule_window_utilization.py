from __future__ import annotations
import importlib.util,json,sqlite3
from pathlib import Path
from evaluation.publish_queue_schedule_window_utilization import build_publish_queue_schedule_window_utilization_report_from_db,format_publish_queue_schedule_window_utilization_json
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"publish_queue_schedule_window_utilization.py"; spec=importlib.util.spec_from_file_location("publish_queue_schedule_window_utilization_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_window_utilization_and_cli(tmp_path):
 c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.executescript("CREATE TABLE publish_queue (id TEXT,content_id TEXT,platform TEXT,scheduled_at TEXT); CREATE TABLE publish_windows (platform TEXT,day TEXT,start_time TEXT,end_time TEXT,capacity INTEGER);")
 c.executemany("INSERT INTO publish_queue VALUES (?,?,?,?)",[("q1","c1","x","2026-01-05T09:30:00+00:00"),("q2","c2","x","2026-01-05T09:45:00+00:00"),("q3","c3","x","2026-01-05T15:00:00+00:00")])
 c.execute("INSERT INTO publish_windows VALUES ('x','monday','09:00','10:00',1)")
 r=build_publish_queue_schedule_window_utilization_report_from_db(c,overfilled_threshold=1,underused_threshold=.1)
 assert {f["type"] for f in r["findings"]}=={"outside_window","overfilled"}; assert json.loads(format_publish_queue_schedule_window_utilization_json(r))["artifact_type"]=="publish_queue_schedule_window_utilization"
 p=tmp_path/"db.sqlite"; c.commit()
 with sqlite3.connect(p) as out: c.backup(out)
 assert script.main(["--db",str(p),"--timezone","UTC"])==0; assert script.main(["--db",str(p),"--limit","0"])==2
def test_missing_table():
 assert build_publish_queue_schedule_window_utilization_report_from_db(sqlite3.connect(":memory:"))["missing_tables"]==["publish_queue"]
