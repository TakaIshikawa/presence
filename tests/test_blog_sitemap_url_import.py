from __future__ import annotations

import importlib.util
import sqlite3
from pathlib import Path

from ingestion.blog_sitemap_url_import import (
    parse_blog_sitemap_urls,
    upsert_blog_sitemap_urls,
)

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "import_blog_sitemap_urls.py"
spec = importlib.util.spec_from_file_location("import_blog_sitemap_urls_script", SCRIPT)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_parses_xml_and_normalizes_canonical_url():
    raw = """<?xml version="1.0"?>
    <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
      <url><loc>HTTPS://Example.COM/post?a=1#top</loc><lastmod>2026-05-01</lastmod><changefreq>weekly</changefreq><priority>0.7</priority></url>
    </urlset>"""
    rows = parse_blog_sitemap_urls(raw)
    assert rows == [
        {
            "canonical_url": "https://example.com/post",
            "path": "/post",
            "lastmod": "2026-05-01",
            "changefreq": "weekly",
            "priority": 0.7,
            "discovered_at": None,
            "sitemap_url": None,
        }
    ]


def test_parses_csv_json_and_upserts(tmp_path):
    csv_rows = parse_blog_sitemap_urls(
        "canonical_url,lastmod,changefreq,priority,discovered_at,sitemap_url\n"
        "https://example.com/a?x=1,2026-05-01,daily,1.0,2026-05-02,https://example.com/sitemap.xml?cache=1\n"
    )
    json_rows = parse_blog_sitemap_urls('[{"url":"https://example.com/b#frag","lastmod":"2026-05-03"}]')
    conn = sqlite3.connect(":memory:")
    summary = upsert_blog_sitemap_urls(conn, csv_rows + json_rows)
    assert summary["parsed_count"] == 2
    assert summary["upserted_count"] == 2
    assert conn.execute("SELECT path FROM blog_sitemap_urls WHERE canonical_url='https://example.com/a'").fetchone()[0] == "/a"
    changed = [{**csv_rows[0], "changefreq": "weekly"}]
    upsert_blog_sitemap_urls(conn, changed)
    assert conn.execute("SELECT changefreq FROM blog_sitemap_urls WHERE canonical_url='https://example.com/a'").fetchone()[0] == "weekly"
    path = tmp_path / "sitemap.json"
    path.write_text('[{"loc":"https://example.com/c?utm=1","lastmod":"2026-05-04"}]')
    db = tmp_path / "sitemap.sqlite"
    assert script.main(["--db", str(db), "--input", str(path), "--format", "text"]) == 0
