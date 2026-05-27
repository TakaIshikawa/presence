from __future__ import annotations
import importlib.util, sqlite3
from pathlib import Path
from ingestion.blog_rss_entry_snapshot_import import parse_blog_rss_entry_snapshots, upsert_blog_rss_entry_snapshots
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_blog_rss_entry_snapshots.py"; spec=importlib.util.spec_from_file_location("script_rss",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_rss_import(tmp_path):
    rows=parse_blog_rss_entry_snapshots('{"feed_url":"https://ex.com/feed","entries":[{"link":"https://ex.com/a?x=1","title":"A"}]}'); assert rows[0]["entry_key"]=="https://ex.com/a" and rows[0]["entry_url"]=="https://ex.com/a"
    c=sqlite3.connect(":memory:"); upsert_blog_rss_entry_snapshots(c,rows); upsert_blog_rss_entry_snapshots(c,[{**rows[0],"title":"B","author":"me"}]); assert c.execute("SELECT count(*),title,author FROM blog_rss_entry_snapshots").fetchone()==(1,"B","me")
    p=tmp_path/"rss.json"; p.write_text('[{"feed_url":"f","guid":"g"}]'); assert script.main(["--db",str(tmp_path/"db.sqlite"),"--input",str(p)])==0
