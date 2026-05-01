"""Tests for curated link metadata enrichment."""

from __future__ import annotations

from contextlib import contextmanager
import json
import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

import enrich_link_metadata as cli  # noqa: E402
from knowledge.link_metadata_enricher import (  # noqa: E402
    enrich_link_metadata,
    extract_enriched_link_metadata,
    format_link_metadata_enrichment_text,
    normalize_canonical_url,
)


ARTICLE_HTML = """<!doctype html>
<html>
  <head>
    <title>Fallback</title>
    <link rel="canonical" href="/article?utm_source=rss&b=2&a=1&fbclid=abc">
    <meta property="og:title" content="Fetched Title">
    <meta property="og:site_name" content="Example Journal">
    <meta property="article:published_time" content="2026-04-20T09:30:00Z">
    <meta property="og:image" content="/card.png?utm_campaign=social">
  </head>
  <body>Body</body>
</html>"""


def _insert_knowledge(
    db,
    *,
    source_id: str = "https://example.com/article?utm_source=newsletter",
    source_url: str | None = "https://example.com/article?utm_source=newsletter",
    metadata: dict | None = None,
    published_at: str | None = None,
) -> int:
    cursor = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, insight,
            attribution_required, license, approved, published_at, metadata)
           VALUES ('curated_article', ?, ?, 'author', 'content', 'insight',
                   1, 'attribution_required', 1, ?, ?)""",
        (
            source_id,
            source_url,
            published_at,
            json.dumps(metadata or {}, sort_keys=True),
        ),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def _fetcher(html: str = ARTICLE_HTML, failures: set[str] | None = None):
    failures = failures or set()
    calls: list[tuple[str, float]] = []

    def fetch(url: str, timeout: float) -> str:
        calls.append((url, timeout))
        if url in failures:
            raise TimeoutError("network timeout")
        return html

    fetch.calls = calls
    return fetch


def test_extract_metadata_normalizes_canonical_tracking_parameters():
    metadata = extract_enriched_link_metadata(
        ARTICLE_HTML,
        "https://example.com/source?utm_medium=email",
    )

    assert metadata.canonical_url == "https://example.com/article?a=1&b=2"
    assert metadata.title == "Fetched Title"
    assert metadata.site_name == "Example Journal"
    assert metadata.published_at == "2026-04-20T09:30:00Z"
    assert normalize_canonical_url(
        "HTTPS://Example.com/article/?utm_source=x&gclid=1&keep=yes"
    ) == "https://example.com/article?keep=yes"


def test_dry_run_reports_updates_without_writing(db):
    row_id = _insert_knowledge(db)
    fetch = _fetcher()

    report = enrich_link_metadata(
        db,
        source_type="knowledge",
        limit=5,
        apply=False,
        timeout=3.5,
        http_client=fetch,
    )

    assert fetch.calls == [("https://example.com/article?utm_source=newsletter", 3.5)]
    assert report.to_dict()["summary"] == {
        "scanned": 1,
        "updated": 1,
        "unchanged": 0,
        "failed": 0,
        "applied": 0,
    }
    result = report.results[0]
    assert result.row_id == row_id
    assert result.updated_fields == [
        "link_metadata.canonical_url",
        "link_metadata.title",
        "link_metadata.site_name",
        "link_metadata.published_at",
        "link_metadata.image",
    ]

    row = db.conn.execute("SELECT published_at, metadata FROM knowledge WHERE id = ?", (row_id,)).fetchone()
    assert row["published_at"] is None
    assert json.loads(row["metadata"]) == {}


def test_apply_updates_missing_knowledge_metadata(db):
    row_id = _insert_knowledge(db)

    report = enrich_link_metadata(
        db,
        source_type="knowledge",
        apply=True,
        http_client=_fetcher(),
    )

    assert report.results[0].applied is True
    row = db.conn.execute("SELECT published_at, metadata FROM knowledge WHERE id = ?", (row_id,)).fetchone()
    metadata = json.loads(row["metadata"])
    assert row["published_at"] == "2026-04-20T09:30:00Z"
    assert metadata["link_metadata"]["canonical_url"] == "https://example.com/article?a=1&b=2"
    assert metadata["link_metadata"]["title"] == "Fetched Title"
    assert metadata["link_metadata"]["site_name"] == "Example Journal"
    assert metadata["link_metadata"]["published_at"] == "2026-04-20T09:30:00Z"


def test_apply_preserves_existing_non_empty_metadata(db):
    row_id = _insert_knowledge(
        db,
        metadata={
            "link_metadata": {
                "title": "Existing Title",
                "canonical_url": "https://example.com/existing",
            },
            "other": "kept",
        },
        published_at="2026-01-01T00:00:00Z",
    )

    report = enrich_link_metadata(db, source_type="knowledge", apply=True, http_client=_fetcher())

    assert report.results[0].updated_fields == [
        "link_metadata.site_name",
        "link_metadata.image",
    ]
    row = db.conn.execute("SELECT published_at, metadata FROM knowledge WHERE id = ?", (row_id,)).fetchone()
    metadata = json.loads(row["metadata"])
    assert row["published_at"] == "2026-01-01T00:00:00Z"
    assert metadata["other"] == "kept"
    assert metadata["link_metadata"]["title"] == "Existing Title"
    assert metadata["link_metadata"]["site_name"] == "Example Journal"
    assert metadata["link_metadata"]["canonical_url"] == "https://example.com/existing"
    assert metadata["link_metadata"]["image"] == "https://example.com/card.png"


def test_curated_source_apply_updates_missing_columns(db):
    db.sync_config_sources(
        [{"identifier": "example.com", "name": "Example", "feed_url": "https://example.com/feed"}],
        "blog",
    )

    report = enrich_link_metadata(
        db,
        source_type="curated_sources",
        apply=True,
        http_client=_fetcher(),
    )

    assert report.results[0].source_table == "curated_sources"
    row = db.get_curated_source("blog", "example.com")
    assert row["canonical_url"] == "https://example.com/article?a=1&b=2"
    assert row["link_title"] == "Fetched Title"
    assert row["site_name"] == "Example Journal"
    assert row["published_at"] == "2026-04-20T09:30:00Z"


def test_fetch_failure_is_captured_per_item(db):
    _insert_knowledge(db, source_url="https://example.com/fail")
    _insert_knowledge(db, source_id="https://example.com/ok", source_url="https://example.com/ok")

    report = enrich_link_metadata(
        db,
        source_type="knowledge",
        limit=5,
        apply=True,
        http_client=_fetcher(failures={"https://example.com/fail"}),
    )

    assert [result.status for result in report.results] == ["failed", "updated"]
    assert "TimeoutError: network timeout" == report.results[0].error
    assert report.to_dict()["summary"]["failed"] == 1
    assert report.to_dict()["summary"]["applied"] == 1


def test_text_and_cli_json_formats(db, capsys):
    _insert_knowledge(db)
    report = enrich_link_metadata(db, source_type="knowledge", http_client=_fetcher())
    text = format_link_metadata_enrichment_text(report)
    assert "Link metadata enrichment report" in text
    assert "fields=link_metadata.canonical_url" in text

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("enrich_link_metadata.script_context", fake_script_context), patch(
        "enrich_link_metadata.enrich_link_metadata",
        return_value=report,
    ):
        result = cli.main(["--source-type", "knowledge", "--limit", "1", "--format", "json"])

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["summary"]["scanned"] == 1
