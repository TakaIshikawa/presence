"""Tests for semantic knowledge freshness reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.freshness_report import build_knowledge_freshness_report
from knowledge_freshness import main
from storage.db import Database


@pytest.fixture
def conn():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    schema_path = Path(__file__).parent.parent / "schema.sql"
    connection.executescript(schema_path.read_text())
    yield connection
    connection.close()


@pytest.fixture
def test_db(tmp_path):
    db = Database(str(tmp_path / "presence.db"))
    db.connect()
    db.init_schema(str(Path(__file__).parent.parent / "schema.sql"))
    yield db
    db.close()


def _insert_knowledge(
    conn,
    *,
    source_type: str,
    source_id: str,
    author: str,
    published_at: str | None,
    license: str = "attribution_required",
) -> None:
    conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, approved,
            published_at, ingested_at, license)
           VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?)""",
        (
            source_type,
            source_id,
            f"https://example.com/{source_id}",
            author,
            f"content for {source_id}",
            published_at,
            published_at,
            license,
        ),
    )
    conn.commit()


def test_empty_store_returns_valid_report(conn):
    report = build_knowledge_freshness_report(
        conn,
        stale_after_days=7,
        now=datetime(2026, 4, 25, tzinfo=timezone.utc),
    )

    assert report.source_count == 0
    assert report.stale_source_count == 0
    assert report.sources == []
    assert report.to_dict()["sources"] == []


def test_mixed_source_types_group_by_source_and_license_mix(conn):
    _insert_knowledge(
        conn,
        source_type="curated_x",
        source_id="tweet-1",
        author="expert",
        published_at="2026-04-20T00:00:00+00:00",
        license="open",
    )
    _insert_knowledge(
        conn,
        source_type="curated_x",
        source_id="tweet-2",
        author="expert",
        published_at="2026-04-24T00:00:00+00:00",
        license="restricted",
    )
    _insert_knowledge(
        conn,
        source_type="curated_article",
        source_id="https://blog.example.com/a",
        author="blog.example.com",
        published_at="2026-04-01T00:00:00+00:00",
        license="attribution_required",
    )

    report = build_knowledge_freshness_report(
        conn,
        stale_after_days=30,
        now=datetime(2026, 4, 25, tzinfo=timezone.utc),
    )

    assert report.source_count == 2
    assert report.stale_source_count == 0

    by_key = {
        (source.source_type, source.source_identifier): source
        for source in report.sources
    }
    x_source = by_key[("curated_x", "expert")]
    assert x_source.item_count == 2
    assert x_source.newest_item_timestamp == "2026-04-24T00:00:00+00:00"
    assert x_source.oldest_item_timestamp == "2026-04-20T00:00:00+00:00"
    assert x_source.license_mix == {"open": 1, "restricted": 1}

    article_source = by_key[("curated_article", "blog.example.com")]
    assert article_source.item_count == 1
    assert article_source.license_mix == {"attribution_required": 1}


def test_sources_older_than_threshold_are_marked_stale(conn):
    _insert_knowledge(
        conn,
        source_type="curated_newsletter",
        source_id="issue-1",
        author="newsletter",
        published_at="2026-04-10T00:00:00+00:00",
    )
    _insert_knowledge(
        conn,
        source_type="curated_x",
        source_id="tweet-new",
        author="fresh",
        published_at="2026-04-24T00:00:00+00:00",
    )

    report = build_knowledge_freshness_report(
        conn,
        stale_after_days=7,
        now=datetime(2026, 4, 25, tzinfo=timezone.utc),
    )

    stale_sources = [source for source in report.sources if source.stale]
    assert report.stale_source_count == 1
    assert stale_sources[0].source_identifier == "newsletter"
    assert stale_sources[0].days_since_newest_item == 15.0


def test_cli_source_type_filter_changes_text_and_json_output(test_db, capsys):
    _insert_knowledge(
        test_db.conn,
        source_type="curated_x",
        source_id="tweet-1",
        author="expert",
        published_at="2026-04-24T00:00:00+00:00",
    )
    _insert_knowledge(
        test_db.conn,
        source_type="own_post",
        source_id="own-1",
        author="self",
        published_at="2026-04-24T00:00:00+00:00",
        license="open",
    )
    mock_config = MagicMock()

    with patch("knowledge_freshness.script_context") as mock_context, patch(
        "sys.argv",
        ["knowledge_freshness.py", "--source-type", "curated_x"],
    ):
        mock_context.return_value.__enter__ = lambda self: (mock_config, test_db)
        mock_context.return_value.__exit__ = lambda self, *args: None
        main()

    text_output = capsys.readouterr().out
    assert "Filter: source_type=curated_x" in text_output
    assert "curated_x" in text_output
    assert "own_post" not in text_output

    with patch("knowledge_freshness.script_context") as mock_context, patch(
        "sys.argv",
        ["knowledge_freshness.py", "--source-type", "curated_x", "--json"],
    ):
        mock_context.return_value.__enter__ = lambda self: (mock_config, test_db)
        mock_context.return_value.__exit__ = lambda self, *args: None
        main()

    payload = json.loads(capsys.readouterr().out)
    assert payload["source_type"] == "curated_x"
    assert payload["source_count"] == 1
    assert payload["sources"][0]["source_type"] == "curated_x"
