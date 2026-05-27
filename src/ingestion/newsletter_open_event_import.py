"""Import newsletter open events."""
from __future__ import annotations
from pathlib import Path
from ._simple_import_common import *
SCHEMA="""CREATE TABLE IF NOT EXISTS newsletter_open_events (email TEXT NOT NULL, campaign_id TEXT NOT NULL, opened_at TEXT NOT NULL, user_agent TEXT, ip_hash TEXT, link_context TEXT, provider TEXT, PRIMARY KEY(campaign_id,email,opened_at))"""
def parse_newsletter_open_events(raw:str):
    return [{"email":req(r,"email","subscriber_email").lower(),"campaign_id":req(r,"campaign_id","issue_id"),"opened_at":req(r,"opened_at","event_time"),"user_agent":text(one(r,"user_agent")) or None,"ip_hash":text(one(r,"ip_hash")) or None,"link_context":text(one(r,"link_context")) or None,"provider":text(one(r,"provider")) or None} for r in records(raw,"opens","events")]
def upsert_newsletter_open_events(conn,rows,dry_run=False):
    if dry_run:return summary("newsletter_open_event_import",len(rows),0,True)
    conn.execute(SCHEMA)
    for r in rows: conn.execute("""INSERT INTO newsletter_open_events VALUES (:email,:campaign_id,:opened_at,:user_agent,:ip_hash,:link_context,:provider) ON CONFLICT(campaign_id,email,opened_at) DO UPDATE SET provider=excluded.provider,user_agent=excluded.user_agent,ip_hash=excluded.ip_hash,link_context=excluded.link_context""",r)
    conn.commit(); return summary("newsletter_open_event_import",len(rows),len(rows),False)
def import_newsletter_open_events(conn,path,dry_run=False): return upsert_newsletter_open_events(conn,parse_newsletter_open_events(Path(path).read_text()),dry_run)
def format_newsletter_open_event_import_json(s): return fmt_json(s)
def format_newsletter_open_event_import_text(s): return fmt_text("Newsletter Open Event Import",s)
