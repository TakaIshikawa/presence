"""Tests for read-only link metadata refresh planning."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from knowledge.link_metadata_refresh import (
    format_link_metadata_refresh_text,
    plan_link_metadata_refresh,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "plan_link_metadata_refresh.py"
spec = importlib.util.spec_from_file_location("plan_link_metadata_refresh", SCRIPT_PATH)
plan_link_metadata_refresh_cli = importlib.util.module_from_spec(spec)
spec.loader.exec_module(plan_link_metadata_refresh_cli)

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


def _metadata(
    *,
    canonical_url: str = "https://example.com/article",
    title: str = "Title",
    site_name: str = "Example",
    image: str = "https://example.com/card.png",
    published_at: str = "2026-04-01T00:00:00+00:00",
    refreshed_at: str | None = None,
) -> dict:
    link_metadata = {
        "canonical_url": canonical_url,
        "title": title,
        "site_name": site_name,
        "image": image,
        "published_at": published_at,
    }
    if refreshed_at is not None:
        link_metadata["refreshed_at"] = refreshed_at
    return {"link_metadata": link_metadata}


def _insert_knowledge(
    db,
    *,
    source_type: str = "curated_article",
    source_id: str = "article-1",
    source_url: str | None = "https://example.com/article",
    metadata: dict | None = None,
    published_at: str | None = None,
) -> int:
    cursor = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, insight,
            attribution_required, license, approved, published_at, metadata)
           VALUES (?, ?, ?, 'author', 'content', 'insight',
                   1, 'attribution_required', 1, ?, ?)""",
        (
            source_type,
            source_id,
            source_url,
            published_at,
            json.dumps(metadata or {}, sort_keys=True),
        ),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def _insert_curated_source(
    db,
    *,
    source_type: str = "blog",
    identifier: str = "example.com",
    feed_url: str | None = "https://example.com/feed",
    canonical_url: str | None = "https://example.com/feed",
    link_title: str | None = "Example Feed",
    site_name: str | None = "Example",
    published_at: str | None = "2026-04-01T00:00:00+00:00",
) -> int:
    cursor = db.conn.execute(
        """INSERT INTO curated_sources
           (source_type, identifier, name, feed_url, canonical_url, link_title,
            site_name, published_at, status)
           VALUES (?, ?, 'Example', ?, ?, ?, ?, ?, 'active')""",
        (
            source_type,
            identifier,
            feed_url,
            canonical_url,
            link_title,
            site_name,
            published_at,
        ),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def _candidate_by_id(report: dict) -> dict[int, dict]:
    return {candidate["row_id"]: candidate for candidate in report["candidates"]}


def test_missing_metadata_is_planned_and_rows_without_urls_are_counted(db):
    missing_id = _insert_knowledge(db, metadata={})
    _insert_knowledge(db, source_id="no-url", source_url=None, metadata={})

    report = plan_link_metadata_refresh(db, source_type="knowledge", now=NOW)

    assert report["summary"]["scanned_count"] == 2
    assert report["summary"]["skipped_no_url_count"] == 1
    assert report["summary"]["candidate_count"] == 1
    candidate = report["candidates"][0]
    assert candidate["source_table"] == "knowledge"
    assert candidate["row_id"] == missing_id
    assert candidate["refresh_reason"] == "missing_metadata"
    assert candidate["missing_fields"] == (
        "canonical_url",
        "title",
        "site_name",
        "image",
        "published_at",
    )
    assert "missing_metadata" in format_link_metadata_refresh_text(report)


def test_stale_metadata_uses_refresh_timestamp(db):
    row_id = _insert_knowledge(
        db,
        metadata=_metadata(refreshed_at=(NOW - timedelta(days=45)).isoformat()),
        published_at="2026-04-01T00:00:00+00:00",
    )

    report = plan_link_metadata_refresh(db, source_type="knowledge", stale_days=30, now=NOW)

    candidate = report["candidates"][0]
    assert candidate["row_id"] == row_id
    assert candidate["refresh_reason"] == "stale_metadata"
    assert candidate["missing_fields"] == ()
    assert candidate["stale_fields"] == (
        "canonical_url",
        "title",
        "site_name",
        "image",
        "published_at",
    )


def test_canonical_conflict_ignores_tracking_only_differences(db):
    tracking_only_id = _insert_knowledge(
        db,
        source_url="https://example.com/article?utm_source=newsletter&keep=1",
        metadata=_metadata(
            canonical_url="https://example.com/article?keep=1",
            refreshed_at=NOW.isoformat(),
        ),
        published_at="2026-04-01T00:00:00+00:00",
    )
    conflict_id = _insert_knowledge(
        db,
        source_id="article-conflict",
        source_url="https://example.com/article?utm_source=newsletter&keep=1",
        metadata=_metadata(
            canonical_url="https://example.com/other?keep=1&utm_campaign=x",
            refreshed_at=NOW.isoformat(),
        ),
        published_at="2026-04-01T00:00:00+00:00",
    )

    report = plan_link_metadata_refresh(db, source_type="knowledge", now=NOW)

    by_id = _candidate_by_id(report)
    assert set(by_id) == {tracking_only_id, conflict_id}
    assert by_id[tracking_only_id]["refresh_reason"] == "tracking_source_url"
    assert by_id[tracking_only_id]["reasons"] == ("tracking_source_url",)
    assert by_id[conflict_id]["refresh_reason"] == "canonical_conflict"
    assert by_id[conflict_id]["reasons"] == ("canonical_conflict", "tracking_source_url")


def test_source_type_filtering_and_curated_source_missing_image(db):
    blog_id = _insert_curated_source(db, source_type="blog", identifier="blog.example")
    _insert_curated_source(db, source_type="newsletter", identifier="news.example")
    _insert_knowledge(db, source_type="curated_article", source_id="article")

    report = plan_link_metadata_refresh(db, source_type="blog", now=NOW)

    assert report["summary"]["scanned_count"] == 1
    assert [candidate["row_id"] for candidate in report["candidates"]] == [blog_id]
    candidate = report["candidates"][0]
    assert candidate["source_table"] == "curated_sources"
    assert candidate["source_type"] == "blog"
    assert candidate["missing_fields"] == ("image",)


def test_cli_json_output(db, capsys):
    _insert_knowledge(db, metadata={})

    @contextmanager
    def fake_script_context():
        yield SimpleNamespace(), db

    with patch.object(plan_link_metadata_refresh_cli, "script_context", fake_script_context), patch.object(
        plan_link_metadata_refresh_cli,
        "plan_link_metadata_refresh",
        wraps=lambda db, **kwargs: plan_link_metadata_refresh(db, now=NOW, **kwargs),
    ):
        assert plan_link_metadata_refresh_cli.main(
            ["--source-type", "knowledge", "--stale-days", "14", "--limit", "5", "--format", "json"]
        ) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"] == {
        "source_type": "knowledge",
        "stale_days": 14,
        "limit": 5,
    }
    assert payload["summary"]["candidate_count"] == 1
    assert payload["candidates"][0]["source_table"] == "knowledge"
