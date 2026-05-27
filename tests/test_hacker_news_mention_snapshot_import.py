from __future__ import annotations
import importlib.util, sqlite3
from pathlib import Path
from ingestion.hacker_news_mention_snapshot_import import parse_hacker_news_mention_snapshots, upsert_hacker_news_mention_snapshots
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_hacker_news_mention_snapshots.py"; spec=importlib.util.spec_from_file_location("hn_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_algolia_flat_and_cli(tmp_path):
    rows=parse_hacker_news_mention_snapshots('{"hits":[{"objectID":"1","story_title":"Post","story_url":"https://EXAMPLE.com/a#frag","created_at":"2026-01-01","author":"pg","points":"5","num_comments":"2","query":"presence"}]}')
    assert rows[0]["hn_object_id"]=="1" and rows[0]["url"]=="https://example.com/a" and rows[0]["comment_count"]==2
    csv_rows=parse_hacker_news_mention_snapshots("title,url,detected_at,points\nT,https://x.test/p#f,now,3\n")
    assert csv_rows[0]["identity"]=="https://x.test/p"
    c=sqlite3.connect(":memory:"); upsert_hacker_news_mention_snapshots(c,rows); upsert_hacker_news_mention_snapshots(c,[{**rows[0],"points":6}])
    assert c.execute("SELECT COUNT(*), points FROM hacker_news_mention_snapshots").fetchone()==(1,6)
    p=tmp_path/"h.jsonl"; p.write_text('{"objectID":"2","detected_at":"now"}'); db=tmp_path/"db.sqlite"
    assert script.main(["--db",str(db),"--input",str(p),"--format","text"])==0
    bad=tmp_path/"bad.json"; bad.write_text('{"title":"x"}'); assert script.main(["--db",str(db),"--input",str(bad)])==1
