from __future__ import annotations
import importlib.util, sqlite3
from pathlib import Path
from ingestion.product_hunt_mention_snapshot_import import parse_product_hunt_mention_snapshots, upsert_product_hunt_mention_snapshots
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_product_hunt_mention_snapshots.py"; spec=importlib.util.spec_from_file_location("import_product_hunt_mention_snapshots_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_product_hunt_mentions_parse_formats_identity_and_counts(tmp_path):
    rows=parse_product_hunt_mention_snapshots('{"data":[{"product_id":"p1","product_name":"Thing","votes_count":"bad","comments_count":"2"},{"product_id":"p1","discussion_id":"d1","votes":"5","comments":"bad"},{"product_id":"p1","comment_id":"c1","body":"Hi"}]}')
    assert [r["kind"] for r in rows]==["launch","comment","discussion"]; assert rows[0]["votes_count"]==0; assert rows[2]["comments_count"]==0
    c=sqlite3.connect(":memory:"); upsert_product_hunt_mention_snapshots(c,rows); upsert_product_hunt_mention_snapshots(c,[{**rows[0],"votes_count":8}])
    assert c.execute("SELECT COUNT(*) FROM product_hunt_mention_snapshots").fetchone()[0]==3; assert c.execute("SELECT votes_count FROM product_hunt_mention_snapshots WHERE kind='launch'").fetchone()[0]==8
    assert len(parse_product_hunt_mention_snapshots("product_id,comment_id,votes_count\np2,c2,3\n"))==1
    assert len(parse_product_hunt_mention_snapshots('{"product_id":"p3"}\n{"product_id":"p4"}\n'))==2
    p=tmp_path/"ph.jsonl"; p.write_text('{"product_id":"p3"}\n'); db=tmp_path/"db.sqlite"; assert script.main(["--db",str(db),"--input",str(p),"--dry-run"])==0
