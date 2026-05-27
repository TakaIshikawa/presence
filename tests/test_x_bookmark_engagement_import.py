from __future__ import annotations
import importlib.util,json,sqlite3
from pathlib import Path
from ingestion.x_bookmark_engagement_import import *
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_x_bookmark_engagement.py"; spec=importlib.util.spec_from_file_location("s",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_x_bookmark_import_alias_and_cli(tmp_path,capsys):
 rows=parse_x_bookmark_engagement_payload('[{"tweet_id":"t1","bookmarked_count":3,"fetched_at":"2026"}]'); assert rows[0]["post_id"]=="t1" and rows[0]["likes"]==0
 c=sqlite3.connect(":memory:"); assert import_x_bookmark_engagement(c,rows)["summary"]["inserted_count"]==1; assert import_x_bookmark_engagement(c,rows,dry_run=True)["summary"]["updated_count"]==1
 p=tmp_path/"in.json"; p.write_text(json.dumps(rows)); db=tmp_path/"d.sqlite"; assert script.main([str(p),"--db",str(db),"--format","json"])==0; assert json.loads(capsys.readouterr().out)["artifact_type"]=="x_bookmark_engagement_import"
