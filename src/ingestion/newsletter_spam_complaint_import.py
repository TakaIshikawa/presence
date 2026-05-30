"""Import newsletter spam complaint events."""
from __future__ import annotations
from pathlib import Path
from ._simple_import_common import *
SCHEMA="""CREATE TABLE IF NOT EXISTS newsletter_spam_complaints (email TEXT NOT NULL, provider TEXT NOT NULL, complained_at TEXT NOT NULL, campaign_id TEXT, message_id TEXT NOT NULL DEFAULT '', reason TEXT, raw_payload_url TEXT, PRIMARY KEY(provider,email,complained_at,message_id))"""
def parse_newsletter_spam_complaints(raw:str):
    out=[]
    for r in records(raw,"complaints","events"):
        out.append({"email":req(r,"email","subscriber_email").lower(),"provider":req(r,"provider").lower(),"complained_at":req(r,"complained_at","event_time"),"campaign_id":text(one(r,"campaign_id")) or None,"message_id":text(one(r,"message_id")),"reason":text(one(r,"reason")) or None,"raw_payload_url":text(one(r,"raw_payload_url")) or None})
    return out
def upsert_newsletter_spam_complaints(conn,rows,dry_run=False):
    if dry_run:return summary("newsletter_spam_complaint_import",len(rows),0,True)
    conn.execute(SCHEMA)
    for r in rows: conn.execute("""INSERT INTO newsletter_spam_complaints VALUES (:email,:provider,:complained_at,:campaign_id,:message_id,:reason,:raw_payload_url) ON CONFLICT(provider,email,complained_at,message_id) DO UPDATE SET campaign_id=excluded.campaign_id,reason=excluded.reason,raw_payload_url=excluded.raw_payload_url""",r)
    conn.commit(); return summary("newsletter_spam_complaint_import",len(rows),len(rows),False)
def import_newsletter_spam_complaints(conn,path,dry_run=False): return upsert_newsletter_spam_complaints(conn,parse_newsletter_spam_complaints(Path(path).read_text()),dry_run)
def format_newsletter_spam_complaint_import_json(s): return fmt_json(s)
def format_newsletter_spam_complaint_import_text(s): return fmt_text("Newsletter Spam Complaint Import",s)
