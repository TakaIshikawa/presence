from __future__ import annotations
import importlib.util, sqlite3
from pathlib import Path
from ingestion.mastodon_follower_snapshot_import import parse_mastodon_follower_snapshots, upsert_mastodon_follower_snapshots
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_mastodon_follower_snapshots.py"; spec=importlib.util.spec_from_file_location("script_masto",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_mastodon_import(tmp_path):
    rows=parse_mastodon_follower_snapshots("account_handle,date,followers_count\n@ME@EX,2026-05-01,nope\n"); assert rows[0]["account_handle"]=="@me@ex" and rows[0]["followers_count"]==0
    c=sqlite3.connect(":memory:"); upsert_mastodon_follower_snapshots(c,rows); upsert_mastodon_follower_snapshots(c,[{**rows[0],"followers_count":2}]); assert c.execute("SELECT count(*),followers_count FROM mastodon_follower_snapshots").fetchone()==(1,2)
    p=tmp_path/"m.json"; p.write_text('[{"account_handle":"A","snapshot_date":"d"}]'); assert script.main(["--db",str(tmp_path/"db.sqlite"),"--input",str(p)])==0
