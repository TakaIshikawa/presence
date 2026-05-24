from __future__ import annotations
import importlib.util,json,sqlite3
from pathlib import Path
from evaluation.newsletter_delivery_failure_taxonomy import build_newsletter_delivery_failure_taxonomy_report_from_db,format_newsletter_delivery_failure_taxonomy_json
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"newsletter_delivery_failure_taxonomy.py"; spec=importlib.util.spec_from_file_location("newsletter_delivery_failure_taxonomy_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_taxonomy_and_cli(tmp_path):
 c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.executescript("CREATE TABLE newsletter_delivery_events (status TEXT,error_code TEXT,error_message TEXT,provider TEXT,issue_id TEXT,subscriber_id TEXT,occurred_at TEXT);")
 c.executemany("INSERT INTO newsletter_delivery_events VALUES (?,?,?,?,?,?,?)",[("failed","BOUNCE","mailbox full","ses","i1","s1","2026-01-01"),("failed","SUPPRESS","suppression list","ses","i1","s2","2026-01-02"),("failed","REJECT","policy blocked","mailgun","i2","s3","2026-01-03"),("ok",None,None,"ses","i2","s4","2026-01-04")])
 r=build_newsletter_delivery_failure_taxonomy_report_from_db(c)
 assert r["totals"]["by_category"]=={"bounce":1,"provider_reject":1,"suppression":1}; assert json.loads(format_newsletter_delivery_failure_taxonomy_json(r))["artifact_type"]=="newsletter_delivery_failure_taxonomy"
 p=tmp_path/"db.sqlite"; c.commit()
 with sqlite3.connect(p) as out: c.backup(out)
 assert script.main(["--db",str(p),"--format","text","--limit","2"])==0; assert script.main(["--db",str(p),"--limit","0"])==2
def test_missing_table():
 assert build_newsletter_delivery_failure_taxonomy_report_from_db(sqlite3.connect(":memory:"))["missing_tables"]==["newsletter_delivery_events"]
