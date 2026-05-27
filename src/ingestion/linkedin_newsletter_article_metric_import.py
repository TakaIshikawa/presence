"""Import LinkedIn newsletter article metric snapshots."""
from __future__ import annotations
import csv, io, json
from pathlib import Path
from typing import Any
from evaluation._batch_report_utils import dump_json, text
SCHEMA="""CREATE TABLE IF NOT EXISTS linkedin_newsletter_article_metrics (newsletter_id TEXT, article_id TEXT NOT NULL, article_url TEXT, published_at TEXT, impressions INTEGER, clicks INTEGER, reactions INTEGER, comments INTEGER, shares INTEGER, new_subscribers INTEGER, captured_at TEXT NOT NULL, PRIMARY KEY (article_id, captured_at))"""
def parse_linkedin_newsletter_article_metrics(raw:str)->list[dict[str,Any]]:
    rows=[]
    for i in _records(raw,("articles","metrics","items")):
        article=text(i.get("article_id") or i.get("id")); captured=text(i.get("captured_at") or i.get("collected_at") or i.get("snapshot_at"))
        if not article or not captured: raise ValueError("article_id and captured_at are required")
        rows.append({"newsletter_id":text(i.get("newsletter_id")) or None,"article_id":article,"article_url":text(i.get("article_url") or i.get("url")) or None,"published_at":text(i.get("published_at")) or None,"impressions":_int(i.get("impressions") or i.get("views")),"clicks":_int(i.get("clicks")),"reactions":_int(i.get("reactions") or i.get("reactions_count")),"comments":_int(i.get("comments") or i.get("comments_count")),"shares":_int(i.get("shares") or i.get("shares_count")),"new_subscribers":_int(i.get("new_subscribers") or i.get("subscribers")),"captured_at":captured})
    rows.sort(key=lambda r:(r["article_id"],r["captured_at"])); return rows
def upsert_linkedin_newsletter_article_metrics(conn,rows,dry_run=False):
    if dry_run: return {"artifact_type":"linkedin_newsletter_article_metric_import","dry_run":True,"parsed_count":len(rows),"upserted_count":0}
    conn.execute(SCHEMA)
    for r in rows: conn.execute("""INSERT INTO linkedin_newsletter_article_metrics VALUES (:newsletter_id,:article_id,:article_url,:published_at,:impressions,:clicks,:reactions,:comments,:shares,:new_subscribers,:captured_at) ON CONFLICT(article_id,captured_at) DO UPDATE SET newsletter_id=excluded.newsletter_id,article_url=excluded.article_url,published_at=excluded.published_at,impressions=excluded.impressions,clicks=excluded.clicks,reactions=excluded.reactions,comments=excluded.comments,shares=excluded.shares,new_subscribers=excluded.new_subscribers""",r)
    conn.commit(); return {"artifact_type":"linkedin_newsletter_article_metric_import","dry_run":False,"parsed_count":len(rows),"upserted_count":len(rows)}
def import_linkedin_newsletter_article_metrics(conn,path,dry_run=False): return upsert_linkedin_newsletter_article_metrics(conn,parse_linkedin_newsletter_article_metrics(Path(path).read_text()),dry_run=dry_run)
def format_linkedin_newsletter_article_metric_import_json(s): return dump_json(s)
def format_linkedin_newsletter_article_metric_import_text(s): return f"LinkedIn Newsletter Article Metric Import\nparsed={s['parsed_count']} upserted={s['upserted_count']} dry_run={s['dry_run']}"
def _int(v):
    try: return int(float(v))
    except (TypeError,ValueError): return 0
def _records(raw,keys):
    raw=raw.strip()
    if not raw: return []
    if raw[0] in "[{":
        d=json.loads(raw)
        if isinstance(d,dict):
            for k in keys:
                if k in d: return d[k]
            return [d]
        return d
    if "," in raw.splitlines()[0]: return list(csv.DictReader(io.StringIO(raw)))
    return [json.loads(l) for l in raw.splitlines() if l.strip()]
