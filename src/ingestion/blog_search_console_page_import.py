"""Import Search Console page performance rows."""
from __future__ import annotations
from pathlib import Path
from ._simple_import_common import *
SCHEMA="""CREATE TABLE IF NOT EXISTS blog_search_console_pages (page_url TEXT NOT NULL, date TEXT NOT NULL, query TEXT NOT NULL DEFAULT '', country TEXT NOT NULL DEFAULT '', device TEXT NOT NULL DEFAULT '', clicks INTEGER, impressions INTEGER, ctr REAL, position REAL, raw_payload TEXT, PRIMARY KEY(page_url,date,query,country,device))"""
def parse_blog_search_console_pages(raw:str):
    return [{"page_url":norm_url(req(r,"page_url","page")),"date":req(r,"date"),"query":text(one(r,"query")),"country":text(one(r,"country")).lower(),"device":text(one(r,"device")).lower(),"clicks":int0(one(r,"clicks")),"impressions":int0(one(r,"impressions")),"ctr":float0(one(r,"ctr")),"position":float0(one(r,"position")),"raw_payload":raw_payload(r)} for r in records(raw,"rows","pages")]
def upsert_blog_search_console_pages(conn,rows,dry_run=False):
    if dry_run:return summary("blog_search_console_page_import",len(rows),0,True)
    conn.execute(SCHEMA)
    for r in rows: conn.execute("""INSERT INTO blog_search_console_pages VALUES (:page_url,:date,:query,:country,:device,:clicks,:impressions,:ctr,:position,:raw_payload) ON CONFLICT(page_url,date,query,country,device) DO UPDATE SET clicks=excluded.clicks,impressions=excluded.impressions,ctr=excluded.ctr,position=excluded.position,raw_payload=excluded.raw_payload""",r)
    conn.commit(); return summary("blog_search_console_page_import",len(rows),len(rows),False)
def import_blog_search_console_pages(conn,path,dry_run=False): return upsert_blog_search_console_pages(conn,parse_blog_search_console_pages(Path(path).read_text()),dry_run)
def format_blog_search_console_page_import_json(s): return fmt_json(s)
def format_blog_search_console_page_import_text(s): return fmt_text("Blog Search Console Page Import",s)
