from __future__ import annotations
import importlib.util, sqlite3
from pathlib import Path
from ingestion.newsletter_spam_complaint_import import parse_newsletter_spam_complaints, upsert_newsletter_spam_complaints
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_newsletter_spam_complaints.py"; spec=importlib.util.spec_from_file_location("script_spam",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_spam_complaint_import(tmp_path):
    rows=parse_newsletter_spam_complaints('[{"subscriber_email":"A@EX.COM","provider":"MAIL","event_time":"2026-05-01","message_id":"","reason":"spam"}]')
    assert rows[0]["email"]=="a@ex.com" and rows[0]["provider"]=="mail"
    c=sqlite3.connect(":memory:"); upsert_newsletter_spam_complaints(c,rows); upsert_newsletter_spam_complaints(c,[{**rows[0],"campaign_id":"c1","raw_payload_url":"u"}])
    assert c.execute("SELECT count(*),campaign_id FROM newsletter_spam_complaints").fetchone()==(1,"c1")
    p=tmp_path/"in.csv"; p.write_text("email,provider,complained_at\nB@EX.COM,MAIL,2026-05-02\n"); db=tmp_path/"db.sqlite"; assert script.main(["--db",str(db),"--input",str(p),"--format","text"])==0
