"""Import newsletter referral sources."""
from __future__ import annotations
from pathlib import Path
from ._simple_import_common import *
SCHEMA="""CREATE TABLE IF NOT EXISTS newsletter_referral_sources (subscriber_identity TEXT NOT NULL, captured_key TEXT NOT NULL, subscriber_email_hash TEXT, subscriber_id TEXT, referral_source TEXT, referral_url TEXT, campaign TEXT, subscribed_at TEXT, captured_at TEXT, raw_payload TEXT, PRIMARY KEY(subscriber_identity,captured_key))"""
def parse_newsletter_referral_sources(raw:str):
    rows=[]
    for r in records(raw,"referrals","sources"):
        email_hash=text(one(r,"subscriber_email_hash")); sid=text(one(r,"subscriber_id")); ident=email_hash or sid
        if not ident: raise ValueError("subscriber_email_hash or subscriber_id is required")
        captured=text(one(r,"captured_at")) or text(one(r,"subscribed_at"))
        if not captured: raise ValueError("captured_at or subscribed_at is required")
        rows.append({"subscriber_identity":ident,"captured_key":captured,"subscriber_email_hash":email_hash or None,"subscriber_id":sid or None,"referral_source":text(one(r,"referral_source")) or None,"referral_url":norm_url(one(r,"referral_url")) or None,"campaign":text(one(r,"campaign")) or None,"subscribed_at":text(one(r,"subscribed_at")) or None,"captured_at":text(one(r,"captured_at")) or None,"raw_payload":raw_payload(r)})
    return rows
def upsert_newsletter_referral_sources(conn,rows,dry_run=False):
    if dry_run:return summary("newsletter_referral_source_import",len(rows),0,True)
    conn.execute(SCHEMA)
    for r in rows: conn.execute("""INSERT INTO newsletter_referral_sources VALUES (:subscriber_identity,:captured_key,:subscriber_email_hash,:subscriber_id,:referral_source,:referral_url,:campaign,:subscribed_at,:captured_at,:raw_payload) ON CONFLICT(subscriber_identity,captured_key) DO UPDATE SET referral_source=excluded.referral_source,referral_url=excluded.referral_url,campaign=excluded.campaign,subscribed_at=excluded.subscribed_at,captured_at=excluded.captured_at,raw_payload=excluded.raw_payload""",r)
    conn.commit(); return summary("newsletter_referral_source_import",len(rows),len(rows),False)
def import_newsletter_referral_sources(conn,path,dry_run=False): return upsert_newsletter_referral_sources(conn,parse_newsletter_referral_sources(Path(path).read_text()),dry_run)
def format_newsletter_referral_source_import_json(s): return fmt_json(s)
def format_newsletter_referral_source_import_text(s): return fmt_text("Newsletter Referral Source Import",s)
