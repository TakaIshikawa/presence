from __future__ import annotations
import importlib.util, sqlite3
from pathlib import Path
from ingestion.newsletter_referral_source_import import parse_newsletter_referral_sources, upsert_newsletter_referral_sources
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_newsletter_referral_sources.py"; spec=importlib.util.spec_from_file_location("script_ref",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_referral_import(tmp_path):
    rows=parse_newsletter_referral_sources('[{"subscriber_id":"s","referral_url":"EX.com/a#x","subscribed_at":"d","campaign":"c"}]'); assert rows[0]["referral_url"]=="https://ex.com/a"
    c=sqlite3.connect(":memory:"); upsert_newsletter_referral_sources(c,rows); upsert_newsletter_referral_sources(c,[{**rows[0],"campaign":"d"}]); assert c.execute("SELECT count(*),campaign FROM newsletter_referral_sources").fetchone()==(1,"d")
    p=tmp_path/"r.json"; p.write_text('[{"subscriber_email_hash":"h","captured_at":"d"}]'); assert script.main(["--db",str(tmp_path/"db.sqlite"),"--input",str(p)])==0
