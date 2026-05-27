from __future__ import annotations
import importlib.util, sqlite3
from pathlib import Path
from ingestion.bluesky_follower_snapshot_import import parse_bluesky_follower_snapshots, upsert_bluesky_follower_snapshots
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_bluesky_follower_snapshots.py"; spec=importlib.util.spec_from_file_location("script_bsky",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_bluesky_import(tmp_path):
    rows,errors=parse_bluesky_follower_snapshots('[{"handle":"ME.BSKY","indexed_at":"d"},{"captured_at":"d"}]'); assert rows[0]["handle"]=="me.bsky" and errors
    c=sqlite3.connect(":memory:"); upsert_bluesky_follower_snapshots(c,(rows,errors)); upsert_bluesky_follower_snapshots(c,([{**rows[0],"post_count":3}],[])); assert c.execute("SELECT count(*),post_count FROM bluesky_follower_snapshots").fetchone()==(1,3)
    p=tmp_path/"b.json"; p.write_text('[{"did":"did:1","captured_at":"d"}]'); assert script.main(["--db",str(tmp_path/"db.sqlite"),"--input",str(p)])==0
