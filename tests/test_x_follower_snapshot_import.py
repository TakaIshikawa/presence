from __future__ import annotations
import importlib.util, sqlite3
from pathlib import Path
from ingestion.x_follower_snapshot_import import parse_x_follower_snapshots, upsert_x_follower_snapshots
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_x_follower_snapshots.py"; spec=importlib.util.spec_from_file_location("script_x",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_x_import(tmp_path):
    rows=parse_x_follower_snapshots('[{"profile":"me","captured_at":"d","follower_count":"1"}]'); c=sqlite3.connect(":memory:"); upsert_x_follower_snapshots(c,rows); upsert_x_follower_snapshots(c,[{**rows[0],"follower_count":2}])
    assert c.execute("SELECT count(*),follower_count FROM x_follower_snapshots").fetchone()==(1,2)
    p=tmp_path/"x.csv"; p.write_text("profile,captured_at\nme,d\n"); assert script.main(["--db",str(tmp_path/"db.sqlite"),"--input",str(p)])==0
