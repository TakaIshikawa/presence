from __future__ import annotations
import importlib.util, sqlite3
from pathlib import Path
from ingestion.mastodon_status_snapshot_import import parse_mastodon_status_snapshots, upsert_mastodon_status_snapshots
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_mastodon_status_snapshots.py"; spec=importlib.util.spec_from_file_location("import_mastodon_status_snapshots_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_mastodon_status_snapshots_parse_nested_flat_and_upsert(tmp_path):
    rows=parse_mastodon_status_snapshots('{"statuses":[{"id":"1","uri":"https://mastodon.social/users/me/statuses/1","account":{"acct":"me@example.com"},"content":"<p>Hello <a href=\\"https://e.test\\">world</a>&amp; friends</p>","replies_count":"bad","reblogs_count":"2","favourites_count":null,"language":"en","visibility":"public"},{"instance":"example.com","status_id":"2","account_acct":"you","content_text":"Plain","replies_count":"3","reblogs_count":"bad","favourites_count":"4"}]}')
    assert [r["status_id"] for r in rows]==["2","1"]
    nested=rows[1]; assert nested["instance"]=="mastodon.social"; assert nested["account_acct"]=="me@example.com"; assert nested["content_text"]=="Hello world& friends"
    assert nested["replies_count"]==0; assert nested["reblogs_count"]==2; assert nested["favourites_count"]==0
    csv_rows=parse_mastodon_status_snapshots("instance,status_id,content,replies_count,reblogs_count,favourites_count\ncsv.example,9,<p>CSV<br>row</p>,,oops,7\n")
    assert csv_rows[0]["content_text"]=="CSV row"; assert csv_rows[0]["replies_count"]==0; assert csv_rows[0]["reblogs_count"]==0
    c=sqlite3.connect(":memory:"); upsert_mastodon_status_snapshots(c,rows); upsert_mastodon_status_snapshots(c,[{**rows[0],"favourites_count":8}])
    assert c.execute("SELECT COUNT(*) FROM mastodon_status_snapshots").fetchone()[0]==2
    assert c.execute("SELECT favourites_count FROM mastodon_status_snapshots WHERE instance='example.com' AND status_id='2'").fetchone()[0]==8
    p=tmp_path/"statuses.jsonl"; p.write_text('{"instance":"cli.example","status_id":"c1","content":"<p>ok</p>"}\n'); db=tmp_path/"db.sqlite"
    assert script.main(["--db",str(db),"--input",str(p),"--dry-run"])==0
