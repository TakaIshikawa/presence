"""Tests for item-level and source-summary knowledge freshness reporting."""

from __future__ import annotations

import json
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.freshness import age_days, build_freshness_report, report_to_dict
from knowledge.freshness_report import build_knowledge_freshness_report
from knowledge_freshness import format_text_report, main
from storage.db import Database


NOW = datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc)


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


def iso(days_ago: int) -> str:
    return (NOW - timedelta(days=days_ago)).isoformat()


def insert_knowledge(
    db,
    *,
    source_type: str = "curated_x",
    source_id: str = "src-1",
    author: str = "alice",
    source_url: str | None = "https://example.test/item",
    content: str = "Useful knowledge content",
    insight: str | None = "Useful insight",
    approved: int = 1,
    published_days_ago: int | None = 10,
    ingested_days_ago: int = 10,
) -> int:
    published_at = iso(published_days_ago) if published_days_ago is not None else None
    conn = db.conn if hasattr(db, "conn") else db
    cursor = conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, insight, approved,
            published_at, ingested_at, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            source_type,
            source_id,
            source_url,
            author,
            content,
            insight,
            approved,
            published_at,
            iso(ingested_days_ago),
            iso(ingested_days_ago),
        ),
    )
    conn.commit()
    return cursor.lastrowid


def test_summary_report_handles_empty_store(conn):
    report = build_knowledge_freshness_report(
        conn,
        stale_after_days=7,
        now=datetime(2026, 4, 25, tzinfo=timezone.utc),
    )

    assert report.source_count == 0
    assert report.sources == []


def test_summary_report_groups_sources_and_license_mix(conn):
    insert_knowledge(
        conn,
        source_type="curated_x",
        source_id="tweet-1",
        author="expert",
        published_days_ago=5,
    )
    insert_knowledge(
        conn,
        source_type="curated_x",
        source_id="tweet-2",
        author="expert",
        published_days_ago=1,
        content="Another item",
    )

    report = build_knowledge_freshness_report(
        conn,
        stale_after_days=30,
        now=datetime(2026, 4, 25, tzinfo=timezone.utc),
    )

    assert report.source_count == 1
    assert report.sources[0].item_count == 2


def test_item_report_stale_age_calculations_use_published_then_ingested(test_db):
    published_id = insert_knowledge(
        test_db, source_id="published-old", published_days_ago=181, ingested_days_ago=2
    )
    ingested_id = insert_knowledge(
        test_db, source_id="ingested-old", published_days_ago=None, ingested_days_ago=181
    )

    findings = build_freshness_report(
        test_db.conn, stale_days=180, unused_days=999, now=NOW
    )

    by_id = {finding.knowledge_id: finding for finding in findings}
    assert by_id[published_id].stale is True
    assert by_id[ingested_id].stale is True
    assert age_days(iso(3), NOW) == pytest.approx(3.0)


def test_item_report_detects_unused_knowledge(test_db):
    unused_id = insert_knowledge(test_db, source_id="unused", ingested_days_ago=120)
    findings = build_freshness_report(
        test_db.conn, stale_days=999, unused_days=90, now=NOW
    )

    assert [finding.knowledge_id for finding in findings] == [unused_id]
    assert findings[0].recommendations == ["retire"]


def test_item_report_text_output(test_db):
    insert_knowledge(test_db, source_id="text", author="Grace", ingested_days_ago=100)
    findings = build_freshness_report(
        test_db.conn, stale_days=999, unused_days=90, now=NOW
    )
    payload = report_to_dict(
        findings,
        stale_days=999,
        unused_days=90,
        source_type="curated_x",
        limit=5,
    )

    output = format_text_report(payload)

    assert "Knowledge Freshness Report" in output
    assert "recommend=retire" in output


def test_cli_json_output_for_item_mode(test_db, capsys):
    knowledge_id = insert_knowledge(test_db, source_id="cli", ingested_days_ago=100)

    @contextmanager
    def fake_script_context():
        yield None, test_db

    with patch("knowledge_freshness.script_context", fake_script_context):
        assert main(
            [
                "--stale-days",
                "999",
                "--unused-days",
                "90",
                "--source-type",
                "curated_x",
                "--format",
                "json",
                "--limit",
                "1",
            ]
        ) == 0

    payload = json.loads(capsys.readouterr().out)
    assert [row["knowledge_id"] for row in payload["findings"]] == [knowledge_id]


def test_cli_summary_mode_filters_source_type(test_db, capsys):
    insert_knowledge(
        test_db,
        source_type="curated_x",
        source_id="tweet-1",
        author="expert",
        published_days_ago=1,
    )
    insert_knowledge(
        test_db,
        source_type="own_post",
        source_id="own-1",
        author="self",
        published_days_ago=1,
    )

    @contextmanager
    def fake_script_context():
        yield None, test_db

    with patch("knowledge_freshness.script_context", fake_script_context):
        assert main(
            ["--mode", "summary", "--source-type", "curated_x", "--format", "json"]
        ) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["source_type"] == "curated_x"
    assert payload["source_count"] == 1


def test_cli_rejects_invalid_values():
    with pytest.raises(SystemExit, match="--limit must be at least 1"):
        main(["--limit", "0"])
