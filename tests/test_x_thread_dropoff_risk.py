from __future__ import annotations
import importlib.util,json,sqlite3
from pathlib import Path
from evaluation.x_thread_dropoff_risk import build_x_thread_dropoff_risk_report_from_db
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"x_thread_dropoff_risk.py"; spec=importlib.util.spec_from_file_location("x_thread_dropoff_risk_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
 c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.execute("CREATE TABLE generated_content (id TEXT, content_type TEXT, title TEXT, content TEXT, metadata TEXT, created_at TEXT)")
 meta=json.dumps({"posts":["Plain opener","x"*40],"engagements":[100,20]}); c.execute("INSERT INTO generated_content VALUES (?,?,?,?,?,?)",("t1","x_thread","T",None,meta,"2026-05-26T00:00:00+00:00")); c.commit(); return c
def test_flags_reasons_and_cli(tmp_path,capsys):
 r=build_x_thread_dropoff_risk_report_from_db(_db(),max_post_chars=30,dropoff_threshold=.5)
 assert set(r["findings"][0]["reasons"])=={"engagement_dropoff","overlong_posts","weak_continuation"}
 db=tmp_path/"db.sqlite"; out=sqlite3.connect(db); _db().backup(out); out.close(); assert script.main(["--db",str(db),"--max-post-chars","30","--format","json"])==0; assert json.loads(capsys.readouterr().out)["artifact_type"]=="x_thread_dropoff_risk"
