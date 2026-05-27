from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from ingestion.discord_mention_snapshot_import import parse_discord_mention_snapshots, upsert_discord_mention_snapshots
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_discord_mention_snapshots.py"; spec=importlib.util.spec_from_file_location("import_discord_mention_snapshots_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_discord_mention_snapshot_import_cli(tmp_path,capsys):
    rows=parse_discord_mention_snapshots('{"mentions":[{"server_id":" s ","channel_id":"c","message_id":"m1","author_handle":"@alice","message_url":"HTTPS://Discord.COM/channels/s/c/m1","content":"hello","created_at":"t0","reactions":"3","replies":"2","captured_at":"t1"}]}')
    assert rows[0]["server_id"]=="s" and rows[0]["author_handle"]=="alice" and rows[0]["message_url"]=="https://discord.com/channels/s/c/m1"
    assert parse_discord_mention_snapshots("message_id,captured_at,reaction_count\nm2,t,4\n")[0]["reaction_count"]==4
    assert parse_discord_mention_snapshots('{"messages":[{"message_id":"m3","captured_at":"t"}]}')[0]["snapshot_key"]
    c=sqlite3.connect(":memory:"); upsert_discord_mention_snapshots(c,rows); upsert_discord_mention_snapshots(c,[{**rows[0],"reply_count":8}]); assert c.execute("SELECT reply_count FROM discord_mention_snapshots").fetchone()[0]==8
    p=tmp_path/"d.jsonl"; p.write_text('{"message_id":"m4","captured_at":"t"}\n'); db=tmp_path/"db.sqlite"; assert script.main(["--db",str(db),"--input",str(p),"--dry-run","--format","json"])==0
    assert json.loads(capsys.readouterr().out)["parsed_count"]==1
