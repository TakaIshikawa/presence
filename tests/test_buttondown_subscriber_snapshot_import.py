from __future__ import annotations
import importlib.util, sqlite3
from pathlib import Path
from ingestion.buttondown_subscriber_snapshot_import import parse_buttondown_subscriber_snapshots, upsert_buttondown_subscriber_snapshots
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_buttondown_subscriber_snapshots.py"; spec=importlib.util.spec_from_file_location("import_buttondown_subscriber_snapshots_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_buttondown_subscriber_snapshots_wrappers_identity_and_tags(tmp_path):
    rows=parse_buttondown_subscriber_snapshots('{"subscribers":[{"email":"USER@EXAMPLE.COM","snapshot_at":"2026-05-01","tags":["pro","alpha"]},{"subscriber_id":"s1","email":"Other@EXAMPLE.COM","fetched_at":"2026-05-01","tags":"beta, alpha"}]}')
    assert rows[0]["email"]=="other@example.com"; assert rows[0]["identity"]=="s1"; assert rows[0]["tags"]=="alpha,beta"; assert rows[1]["identity"]=="user@example.com"
    c=sqlite3.connect(":memory:"); upsert_buttondown_subscriber_snapshots(c,rows); upsert_buttondown_subscriber_snapshots(c,[{**rows[1],"status":"active"}])
    assert c.execute("SELECT COUNT(*) FROM buttondown_subscriber_snapshots").fetchone()[0]==2; assert c.execute("SELECT status FROM buttondown_subscriber_snapshots WHERE identity='user@example.com'").fetchone()[0]=="active"
    assert len(parse_buttondown_subscriber_snapshots("email,snapshot_at\nx@y.com,2026-05-01\n"))==1
    assert len(parse_buttondown_subscriber_snapshots('{"email":"a@b.com","date":"2026-05-01"}\n'))==1
    p=tmp_path/"subs.json"; p.write_text('{"items":[{"email":"a@b.com","snapshot_at":"2026-05-01"}]}'); db=tmp_path/"db.sqlite"; assert script.main(["--db",str(db),"--input",str(p),"--dry-run"])==0
