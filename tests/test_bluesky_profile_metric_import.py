from __future__ import annotations
import importlib.util, sqlite3
from pathlib import Path
from ingestion.bluesky_profile_metric_import import parse_bluesky_profile_metrics, upsert_bluesky_profile_metrics
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_bluesky_profile_metrics.py"; spec=importlib.util.spec_from_file_location("import_bluesky_profile_metrics_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_bluesky_profile_metrics_atproto_aliases_and_upsert(tmp_path):
    rows=parse_bluesky_profile_metrics('{"profiles":[{"did":"did:plc:1","handle":"User.BSKY.Social","fetched_at":"2026-05-01","followersCount":"3","followsCount":"bad","postsCount":"4","displayName":"User","indexedAt":"i","avatar":"u"}]}')
    assert rows[0]["identity"]=="did:plc:1"; assert rows[0]["handle"]=="user.bsky.social"; assert rows[0]["followers_count"]==3; assert rows[0]["follows_count"]==0
    c=sqlite3.connect(":memory:"); upsert_bluesky_profile_metrics(c,rows); upsert_bluesky_profile_metrics(c,[{**rows[0],"posts_count":9}])
    assert c.execute("SELECT COUNT(*),posts_count FROM bluesky_profile_metrics").fetchone()==(1,9)
    assert parse_bluesky_profile_metrics("handle,date,followers_count\nA.B,2026-05-01,1\n")[0]["identity"]=="a.b"
    p=tmp_path/"bsky.jsonl"; p.write_text('{"handle":"a.b","date":"2026-05-01"}\n'); db=tmp_path/"db.sqlite"; assert script.main(["--db",str(db),"--input",str(p),"--dry-run"])==0
