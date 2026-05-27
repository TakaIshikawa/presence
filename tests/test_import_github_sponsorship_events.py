from __future__ import annotations
import importlib.util, sqlite3
from pathlib import Path
from ingestion.github_sponsorship_event_import import parse_github_sponsorship_events, upsert_github_sponsorship_events
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_github_sponsorship_events.py"; spec=importlib.util.spec_from_file_location("import_github_sponsorship_events_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_github_sponsorship_event_import_cli(tmp_path):
    rows=parse_github_sponsorship_events('{"sponsorships":[{"sponsor_login":"Alice","event_type":"Tier Changed","monthly_amount_cents":"500","occurred_at":"t","external_event_id":"e1"}]}')
    assert rows[0]["sponsor_login"]=="alice" and rows[0]["event_type"]=="tier_changed"
    assert parse_github_sponsorship_events('{"events":[{"sponsor_login":"bob","event_type":"created","occurred_at":"t"}]}')[0]["idempotency_key"]
    c=sqlite3.connect(":memory:"); upsert_github_sponsorship_events(c,rows); upsert_github_sponsorship_events(c,[{**rows[0],"tier_name":"Gold"}]); assert c.execute("SELECT tier_name FROM github_sponsorship_events").fetchone()[0]=="Gold"
    p=tmp_path/"s.csv"; p.write_text("sponsor_login,event_type,occurred_at\nc,created,t\n"); db=tmp_path/"db.sqlite"; assert script.main(["--db",str(db),"--input",str(p),"--dry-run"])==0
