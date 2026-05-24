from __future__ import annotations
import importlib.util, sqlite3
from pathlib import Path
from evaluation.publish_queue_manual_override_audit import build_publish_queue_manual_override_audit_report_from_db, format_publish_queue_manual_override_audit_text
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"publish_queue_manual_override_audit.py"; spec=importlib.util.spec_from_file_location("publish_queue_manual_override_audit_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_override_audit_and_cli(tmp_path,capsys):
 c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.execute("CREATE TABLE publish_queue (id TEXT, content_id TEXT, override_type TEXT, override_actor TEXT, override_reason TEXT, status TEXT, metadata TEXT, updated_at TEXT)")
 c.execute("INSERT INTO publish_queue VALUES ('q1','c1','priority',NULL,NULL,NULL,'{}','2026-05-24T00:00:00+00:00')")
 c.execute("INSERT INTO publish_queue VALUES ('q2','c2',NULL,NULL,NULL,NULL,'{\"manual_override\":\"status\",\"actor\":\"ann\"}','2026-05-24T00:00:00+00:00')")
 r=build_publish_queue_manual_override_audit_report_from_db(c,now=__import__("datetime").datetime(2026,5,25,tzinfo=__import__("datetime").timezone.utc))
 assert {"missing_actor","missing_reason","missing_publication_outcome"} <= {x["issue_type"] for x in r["findings"]}
 assert "Manual Override" in format_publish_queue_manual_override_audit_text(r)
 c.commit(); path=tmp_path/"db.sqlite"
 with sqlite3.connect(path) as out: c.backup(out)
 assert script.main(["--db",str(path),"--format","text"])==0; assert "Manual Override" in capsys.readouterr().out
 assert script.main(["--db",str(path),"--limit","0"])==2
def test_override_schema_gap(): assert build_publish_queue_manual_override_audit_report_from_db(sqlite3.connect(":memory:"))["missing_tables"]==["publish_queue"]
