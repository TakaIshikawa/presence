from __future__ import annotations
import importlib.util, sqlite3
from pathlib import Path
from ingestion.newsletter_open_event_import import parse_newsletter_open_events, upsert_newsletter_open_events
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_newsletter_open_events.py"; spec=importlib.util.spec_from_file_location("script_open",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_open_event_import(tmp_path):
    rows=parse_newsletter_open_events('[{"subscriber_email":"A@EX.COM","issue_id":"i1","event_time":"t","provider":"p"}]'); assert rows[0]["email"]=="a@ex.com" and rows[0]["campaign_id"]=="i1"
    c=sqlite3.connect(":memory:"); upsert_newsletter_open_events(c,rows); upsert_newsletter_open_events(c,[{**rows[0],"user_agent":"ua","ip_hash":"h","link_context":"ctx"}])
    assert c.execute("SELECT count(*),user_agent,ip_hash,link_context FROM newsletter_open_events").fetchone()==(1,"ua","h","ctx")
    p=tmp_path/"bad.json"; p.write_text('{"email":"x"}'); assert script.main(["--db",str(tmp_path/"db.sqlite"),"--input",str(p)])==1
