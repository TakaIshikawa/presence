"""Import curated knowledge source author profiles."""
from __future__ import annotations
import json, re
from pathlib import Path
from ._simple_import_common import *
SCHEMA="""CREATE TABLE IF NOT EXISTS knowledge_source_author_profiles (source_url TEXT NOT NULL, author_key TEXT NOT NULL, captured_at TEXT NOT NULL, author_name TEXT, author_url TEXT, platform TEXT, follower_count INTEGER, bio TEXT, expertise_tags TEXT, raw_payload TEXT, PRIMARY KEY(source_url,author_key,captured_at))"""
def _tags(v):
    if isinstance(v,list): return json.dumps([text(x) for x in v if text(x)],sort_keys=True)
    raw=text(v)
    if raw.startswith("["):
        try: return json.dumps([text(x) for x in json.loads(raw) if text(x)],sort_keys=True)
        except json.JSONDecodeError: pass
    return json.dumps([x.strip() for x in re.split(r"[;,|]",raw) if x.strip()],sort_keys=True)
def _optional_url(v):
    raw=text(v)
    if not raw: return ""
    return norm_url(raw) if "." in raw or "://" in raw else raw
def parse_knowledge_source_author_profiles(raw:str):
    rows=[]
    for r in records(raw,"profiles","authors"):
        source=norm_url(req(r,"source_url")); name=text(one(r,"author_name")); author_url=_optional_url(one(r,"author_url")); key=author_url or name
        if not key: raise ValueError("author_url or author_name is required")
        rows.append({"source_url":source,"author_key":key,"captured_at":req(r,"captured_at"),"author_name":name or None,"author_url":author_url or None,"platform":text(one(r,"platform")) or None,"follower_count":int0(one(r,"follower_count")) if text(one(r,"follower_count")) else None,"bio":text(one(r,"bio")) or None,"expertise_tags":_tags(one(r,"expertise_tags","tags")),"raw_payload":raw_payload(r)})
    return rows
def upsert_knowledge_source_author_profiles(conn,rows,dry_run=False):
    if dry_run:return summary("knowledge_source_author_profile_import",len(rows),0,True)
    conn.execute(SCHEMA)
    for r in rows: conn.execute("""INSERT INTO knowledge_source_author_profiles VALUES (:source_url,:author_key,:captured_at,:author_name,:author_url,:platform,:follower_count,:bio,:expertise_tags,:raw_payload) ON CONFLICT(source_url,author_key,captured_at) DO UPDATE SET author_name=excluded.author_name,author_url=excluded.author_url,platform=excluded.platform,follower_count=excluded.follower_count,bio=excluded.bio,expertise_tags=excluded.expertise_tags,raw_payload=excluded.raw_payload""",r)
    conn.commit(); return summary("knowledge_source_author_profile_import",len(rows),len(rows),False)
def import_knowledge_source_author_profiles(conn,path,dry_run=False): return upsert_knowledge_source_author_profiles(conn,parse_knowledge_source_author_profiles(Path(path).read_text()),dry_run)
def format_knowledge_source_author_profile_import_json(s): return fmt_json(s)
def format_knowledge_source_author_profile_import_text(s): return fmt_text("Knowledge Source Author Profile Import",s)
