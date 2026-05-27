from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from ingestion.threads_post_snapshot_import import parse_threads_post_snapshots, upsert_threads_post_snapshots
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_threads_post_snapshots.py"; spec=importlib.util.spec_from_file_location("import_threads_post_snapshots_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_threads_post_snapshot_import_cli(tmp_path,capsys):
    raw='{"posts":[{"account_handle":"@Alice","post_id":"p1","url":"HTTPS://Threads.NET/@Alice/Post/ABC?x=1","posted_at":"2026-05-01T00:00:00Z","text":"Hi","likes":"3","replies":"2","reposts":"1","views":"10","captured_at":"2026-05-02T00:00:00Z"}]}'
    rows=parse_threads_post_snapshots(raw); assert rows[0]["account_handle"]=="Alice"; assert rows[0]["url"]=="https://threads.net/@Alice/Post/ABC?x=1"; assert rows[0]["like_count"]==3
    assert parse_threads_post_snapshots('{"items":[{"post_id":"p2","captured_at":"t"}]}')[0]["post_id"]=="p2"
    assert parse_threads_post_snapshots('post_id,captured_at,view_count\np3,t,5\n')[0]["view_count"]==5
    c=sqlite3.connect(":memory:"); upsert_threads_post_snapshots(c,rows); upsert_threads_post_snapshots(c,[{**rows[0],"view_count":25}]); assert c.execute("SELECT view_count FROM threads_post_snapshots").fetchone()[0]==25
    p=tmp_path/"threads.jsonl"; p.write_text('{"post_id":"p4","captured_at":"t","views":4}\n'); db=tmp_path/"db.sqlite"; assert script.main(["--db",str(db),"--input",str(p),"--dry-run","--format","json"])==0
    out=json.loads(capsys.readouterr().out); assert out["parsed_count"]==1 and out["dry_run"] is True
