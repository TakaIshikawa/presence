"""Import public newsletter archive metric snapshots."""
from __future__ import annotations
import csv, hashlib, io, json, sqlite3
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from evaluation._batch_report_utils import dump_json, text

SCHEMA = """CREATE TABLE IF NOT EXISTS newsletter_archive_metrics (
id TEXT PRIMARY KEY, issue_id TEXT, archive_url TEXT, metric_date TEXT NOT NULL, views INTEGER, unique_views INTEGER, shares INTEGER, referrals INTEGER, source TEXT)"""


def parse_newsletter_archive_metrics(raw: str) -> list[dict[str, Any]]:
    rows = []
    for item in _records(raw):
        issue = text(item.get("issue_id") or item.get("id") or item.get("archive_id")) or None
        url = _url(item.get("archive_url") or item.get("url")) or None
        metric_date = text(item.get("metric_date") or item.get("date"))
        if not (issue or url) or not metric_date: raise ValueError("issue_id or archive_url plus metric_date are required")
        rows.append({"id": _id(issue, url, metric_date), "issue_id": issue, "archive_url": url, "metric_date": metric_date, "views": _int(item.get("views")), "unique_views": _int(item.get("unique_views")), "shares": _int(item.get("shares")), "referrals": _int(item.get("referrals")), "source": text(item.get("source")) or None})
    rows.sort(key=lambda r: r["id"]); return rows


def upsert_newsletter_archive_metrics(conn: sqlite3.Connection, rows: list[dict[str, Any]], dry_run=False):
    if dry_run: return {"artifact_type": "newsletter_archive_metric_import", "dry_run": True, "parsed_count": len(rows), "upserted_count": 0}
    conn.execute(SCHEMA)
    for r in rows:
        conn.execute("""INSERT INTO newsletter_archive_metrics VALUES (:id,:issue_id,:archive_url,:metric_date,:views,:unique_views,:shares,:referrals,:source)
        ON CONFLICT(id) DO UPDATE SET views=excluded.views,unique_views=excluded.unique_views,shares=excluded.shares,referrals=excluded.referrals,archive_url=excluded.archive_url,source=excluded.source""", r)
    conn.commit(); return {"artifact_type": "newsletter_archive_metric_import", "dry_run": False, "parsed_count": len(rows), "upserted_count": len(rows)}


def import_newsletter_archive_metrics(conn, path, dry_run=False): return upsert_newsletter_archive_metrics(conn, parse_newsletter_archive_metrics(Path(path).read_text()), dry_run=dry_run)
def format_newsletter_archive_metric_import_json(s): return dump_json(s)
def format_newsletter_archive_metric_import_text(s): return f"Newsletter Archive Metric Import\nparsed={s['parsed_count']} upserted={s['upserted_count']} dry_run={s['dry_run']}"


def _records(raw):
    raw = raw.strip()
    if not raw: return []
    if raw[0] in "[{":
        data = json.loads(raw)
        if isinstance(data, dict): return data.get("metrics") or data.get("items") or [data]
        return data
    if "," in raw.splitlines()[0]: return list(csv.DictReader(io.StringIO(raw)))
    return [json.loads(l) for l in raw.splitlines() if l.strip()]
def _url(v):
    raw = text(v)
    if not raw: return ""
    p = urlsplit(raw); query = p.query if not (p.path and (p.netloc or p.scheme)) else p.query
    return urlunsplit((p.scheme.lower() or "https", p.netloc.lower(), p.path, query, ""))
def _int(v):
    try: return max(0, int(float(str(v).replace(",", ""))))
    except (TypeError, ValueError): return 0
def _id(*parts): return hashlib.sha256("|".join(text(p) for p in parts).encode()).hexdigest()[:24]
