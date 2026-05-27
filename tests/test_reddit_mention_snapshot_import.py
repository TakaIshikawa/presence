from __future__ import annotations
import importlib.util, sqlite3
from pathlib import Path
import pytest
from ingestion.reddit_mention_snapshot_import import parse_reddit_mention_snapshots, upsert_reddit_mention_snapshots
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_reddit_mention_snapshots.py"; spec=importlib.util.spec_from_file_location("import_reddit_mention_snapshots_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_reddit_mentions_parse_posts_comments_and_upsert(tmp_path):
    raw='[{"thing_id":"t3_a","kind":"submission","subreddit":"Python","title":"Hi","score":"4","permalink":"/r/Python/a","created_at":"2026-05-01","matched_term":"x"},{"thing_id":"t1_b","type":"t1","author":"me","body":"Body","score":"7.0","created_utc":"2026-05-02"}]'
    rows=parse_reddit_mention_snapshots(raw); assert rows[0]["kind"]=="comment"; assert rows[1]["kind"]=="post"; assert rows[1]["permalink"]=="/r/Python/a"; assert rows[0]["score"]==7
    c=sqlite3.connect(":memory:"); upsert_reddit_mention_snapshots(c,rows); upsert_reddit_mention_snapshots(c,[{**rows[0],"body":"Updated","score":9}])
    assert c.execute("SELECT COUNT(*) FROM reddit_mention_snapshots").fetchone()[0]==2; assert c.execute("SELECT body,score FROM reddit_mention_snapshots WHERE thing_id='t1_b'").fetchone()==("Updated",9)
    p=tmp_path/"reddit.json"; p.write_text(raw); db=tmp_path/"db.sqlite"; assert script.main(["--db",str(db),"--input",str(p),"--dry-run","--format","text"])==0
def test_reddit_mentions_require_identity_and_created_at():
    with pytest.raises(ValueError,match="thing_id is required"): parse_reddit_mention_snapshots('{"created_at":"2026-05-01"}')
    with pytest.raises(ValueError,match="created_at is required"): parse_reddit_mention_snapshots('{"thing_id":"t3_a"}')
