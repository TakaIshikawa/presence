"""Import LinkedIn organization/page metric snapshots."""
from __future__ import annotations
import csv, hashlib, io, json, sqlite3
from pathlib import Path
from typing import Any
from evaluation._batch_report_utils import dump_json, text

SCHEMA = """CREATE TABLE IF NOT EXISTS linkedin_page_metrics (
id TEXT PRIMARY KEY, organization_urn TEXT, page_id TEXT, snapshot_at TEXT NOT NULL,
follower_count INTEGER, impressions INTEGER, clicks INTEGER, reactions INTEGER, comments INTEGER, shares INTEGER, source TEXT)"""


def parse_linkedin_page_metrics(raw: str) -> list[dict[str, Any]]:
    rows = []
    for item in _records(raw):
        org = text(item.get("organization_urn") or item.get("organization") or item.get("urn")) or None
        page_id = text(item.get("page_id") or item.get("id")) or None
        snapshot = text(item.get("snapshot_at") or item.get("date") or item.get("created_at"))
        if not (org or page_id) or not snapshot: raise ValueError("organization_urn or page_id plus snapshot_at are required")
        rows.append({"id": _id(org, page_id, snapshot), "organization_urn": org, "page_id": page_id, "snapshot_at": snapshot, "follower_count": _int(item.get("follower_count") or item.get("followers")), "impressions": _int(item.get("impressions")), "clicks": _int(item.get("clicks")), "reactions": _int(item.get("reactions")), "comments": _int(item.get("comments")), "shares": _int(item.get("shares")), "source": text(item.get("source")) or None})
    rows.sort(key=lambda r: r["id"]); return rows


def upsert_linkedin_page_metrics(conn: sqlite3.Connection, rows: list[dict[str, Any]], dry_run=False):
    if dry_run: return {"artifact_type": "linkedin_page_metric_import", "dry_run": True, "parsed_count": len(rows), "upserted_count": 0}
    conn.execute(SCHEMA)
    for r in rows:
        conn.execute("""INSERT INTO linkedin_page_metrics VALUES (:id,:organization_urn,:page_id,:snapshot_at,:follower_count,:impressions,:clicks,:reactions,:comments,:shares,:source)
        ON CONFLICT(id) DO UPDATE SET follower_count=excluded.follower_count,impressions=excluded.impressions,clicks=excluded.clicks,reactions=excluded.reactions,comments=excluded.comments,shares=excluded.shares,source=excluded.source""", r)
    conn.commit(); return {"artifact_type": "linkedin_page_metric_import", "dry_run": False, "parsed_count": len(rows), "upserted_count": len(rows)}


def import_linkedin_page_metrics(conn, path, dry_run=False): return upsert_linkedin_page_metrics(conn, parse_linkedin_page_metrics(Path(path).read_text()), dry_run=dry_run)
def format_linkedin_page_metric_import_json(s): return dump_json(s)
def format_linkedin_page_metric_import_text(s): return f"LinkedIn Page Metric Import\nparsed={s['parsed_count']} upserted={s['upserted_count']} dry_run={s['dry_run']}"


def _records(raw):
    raw = raw.strip()
    if not raw: return []
    if raw[0] in "[{":
        data = json.loads(raw)
        if isinstance(data, dict): return data.get("organizations") or data.get("pages") or data.get("items") or [data]
        return data
    if "," in raw.splitlines()[0]: return list(csv.DictReader(io.StringIO(raw)))
    return [json.loads(l) for l in raw.splitlines() if l.strip()]
def _int(v):
    try: return max(0, int(float(str(v).replace(",", ""))))
    except (TypeError, ValueError): return 0
def _id(*parts): return hashlib.sha256("|".join(text(p) for p in parts).encode()).hexdigest()[:24]
