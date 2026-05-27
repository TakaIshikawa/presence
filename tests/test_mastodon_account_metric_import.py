from __future__ import annotations
import importlib.util, sqlite3
from pathlib import Path
from ingestion.mastodon_account_metric_import import parse_mastodon_account_metrics, upsert_mastodon_account_metrics
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_mastodon_account_metrics.py"; spec=importlib.util.spec_from_file_location("mastodon_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_api_and_flat_records_upsert(tmp_path):
    rows=parse_mastodon_account_metrics('{"id":"42","acct":"@User","url":"https://Mastodon.Social/@User","snapshot_at":"2026-01-01","followers_count":"10","following_count":"2","statuses_count":"3","bot":true,"locked":"false"}')
    assert rows[0]["instance"]=="mastodon.social" and rows[0]["acct"]=="user" and rows[0]["followers_count"]==10 and rows[0]["bot"]==1
    csv_rows=parse_mastodon_account_metrics("instance,acct,snapshot_at,followers_count\nExample.COM,Name,now,5\n")
    assert csv_rows[0]["instance"]=="example.com" and csv_rows[0]["account_id"]=="name"
    c=sqlite3.connect(":memory:"); upsert_mastodon_account_metrics(c,rows); upsert_mastodon_account_metrics(c,[{**rows[0],"followers_count":11}])
    assert c.execute("SELECT COUNT(*), followers_count FROM mastodon_account_metrics").fetchone()==(1,11)
    p=tmp_path/"m.jsonl"; p.write_text('{"acct":"a","url":"https://x.test/@a","snapshot_at":"now"}'); db=tmp_path/"db.sqlite"
    assert script.main(["--db",str(db),"--input",str(p),"--format","text"])==0
    bad=tmp_path/"bad.json"; bad.write_text('{"acct":"a"}'); assert script.main(["--db",str(db),"--input",str(bad)])==1
