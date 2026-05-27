"""Import blog sitemap URL snapshots."""
from __future__ import annotations

import csv
import io
import json
import sqlite3
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit
import xml.etree.ElementTree as ET

from evaluation._batch_report_utils import dump_json, text

SCHEMA = """CREATE TABLE IF NOT EXISTS blog_sitemap_urls (
canonical_url TEXT PRIMARY KEY,
path TEXT,
lastmod TEXT,
changefreq TEXT,
priority REAL,
discovered_at TEXT,
sitemap_url TEXT
)"""


def parse_blog_sitemap_urls(raw: str) -> list[dict[str, Any]]:
    rows = []
    for item in _records(raw):
        canonical_url = _canonical_url(item.get("canonical_url") or item.get("url") or item.get("loc"))
        if not canonical_url:
            raise ValueError("canonical_url or loc is required")
        rows.append(
            {
                "canonical_url": canonical_url,
                "path": _path(canonical_url),
                "lastmod": text(item.get("lastmod")) or None,
                "changefreq": text(item.get("changefreq")) or None,
                "priority": _float(item.get("priority")),
                "discovered_at": text(item.get("discovered_at") or item.get("fetched_at") or item.get("observed_at")) or None,
                "sitemap_url": _canonical_url(item.get("sitemap_url")) or None,
            }
        )
    rows.sort(key=lambda row: row["canonical_url"])
    return rows


def upsert_blog_sitemap_urls(conn: sqlite3.Connection, rows: list[dict[str, Any]], dry_run: bool = False) -> dict[str, Any]:
    if dry_run:
        return {
            "artifact_type": "blog_sitemap_url_import",
            "dry_run": True,
            "parsed_count": len(rows),
            "upserted_count": 0,
        }
    conn.execute(SCHEMA)
    for row in rows:
        conn.execute(
            """INSERT INTO blog_sitemap_urls VALUES (:canonical_url,:path,:lastmod,:changefreq,:priority,:discovered_at,:sitemap_url)
               ON CONFLICT(canonical_url) DO UPDATE SET
               path=excluded.path,lastmod=excluded.lastmod,changefreq=excluded.changefreq,
               priority=excluded.priority,discovered_at=excluded.discovered_at,sitemap_url=excluded.sitemap_url""",
            row,
        )
    conn.commit()
    return {
        "artifact_type": "blog_sitemap_url_import",
        "dry_run": False,
        "parsed_count": len(rows),
        "upserted_count": len(rows),
    }


def import_blog_sitemap_urls(conn: sqlite3.Connection, path: str, dry_run: bool = False) -> dict[str, Any]:
    return upsert_blog_sitemap_urls(conn, parse_blog_sitemap_urls(Path(path).read_text()), dry_run=dry_run)


def format_blog_sitemap_url_import_json(summary: dict[str, Any]) -> str:
    return dump_json(summary)


def format_blog_sitemap_url_import_text(summary: dict[str, Any]) -> str:
    return (
        "Blog Sitemap URL Import\n"
        f"parsed={summary['parsed_count']} upserted={summary['upserted_count']} dry_run={summary['dry_run']}"
    )


def _records(raw: str) -> list[dict[str, Any]]:
    source = raw.strip()
    if not source:
        return []
    if source.startswith("<"):
        return _xml_records(source)
    if source[0] in "[{":
        data = json.loads(source)
        if isinstance(data, dict):
            value = data.get("urls") or data.get("items") or data.get("records") or [data]
            return value if isinstance(value, list) else [value]
        return data
    if "," in source.splitlines()[0]:
        return list(csv.DictReader(io.StringIO(source)))
    return [json.loads(line) for line in source.splitlines() if line.strip()]


def _xml_records(raw: str) -> list[dict[str, Any]]:
    root = ET.fromstring(raw)
    records = []
    for url in root.findall(".//{*}url"):
        item = {}
        for child in list(url):
            key = child.tag.rsplit("}", 1)[-1]
            item[key] = text(child.text)
        records.append(item)
    return records


def _canonical_url(value: Any) -> str:
    raw = text(value)
    if not raw:
        return ""
    parsed = urlsplit(raw)
    if not parsed.scheme and not parsed.netloc:
        return raw.split("?", 1)[0].split("#", 1)[0]
    scheme = parsed.scheme.lower() or "https"
    netloc = parsed.netloc.lower()
    path = parsed.path or "/"
    return urlunsplit((scheme, netloc, path, "", ""))


def _path(canonical_url: str) -> str:
    parsed = urlsplit(canonical_url)
    return parsed.path or "/"


def _float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
