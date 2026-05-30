from __future__ import annotations
import importlib.util, sqlite3, json
from pathlib import Path
from ingestion.linkedin_follower_snapshot_import import parse_linkedin_follower_snapshots, upsert_linkedin_follower_snapshots
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_linkedin_follower_snapshots.py"; spec=importlib.util.spec_from_file_location("script_li",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_linkedin_import(tmp_path):
    rows=parse_linkedin_follower_snapshots('[{"profile_id":"p","captured_at":"d","follower_count":"2","extra":"x"}]'); assert json.loads(rows[0]["raw_payload"])["extra"]=="x"
    c=sqlite3.connect(":memory:"); upsert_linkedin_follower_snapshots(c,rows); upsert_linkedin_follower_snapshots(c,[{**rows[0],"visitor_count":5}]); assert c.execute("SELECT count(*),visitor_count FROM linkedin_follower_snapshots").fetchone()==(1,5)
    p=tmp_path/"li.csv"; p.write_text("profile_id,captured_at\np,d\n"); assert script.main(["--db",str(tmp_path/"db.sqlite"),"--input",str(p)])==0
