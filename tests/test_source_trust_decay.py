"""Tests for source trust decay reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from knowledge.source_trust_decay import (
    build_source_trust_decay_report,
    format_json_report,
    format_text_report,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "source_trust_decay.py"
spec = importlib.util.spec_from_file_location("source_trust_decay_script", SCRIPT_PATH)
source_trust_decay_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(source_trust_decay_script)

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _iso(days: int) -> str:
    return (NOW - timedelta(days=days)).isoformat()


def _source(
    db,
    identifier: str,
    *,
    source_type: str = "blog",
    license: str | None = "open",
    active: int = 1,
    status: str = "active",
    last_fetch_status: str | None = "success",
    consecutive_failures: int = 0,
    last_success_days: int | None = 2,
    last_failure_days: int | None = None,
) -> int:
    return db.conn.execute(
        """INSERT INTO curated_sources
           (source_type, identifier, name, license, active, status, last_fetch_status,
            consecutive_failures, last_success_at, last_failure_at, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            source_type,
            identifier,
            identifier.title(),
            license,
            active,
            status,
            last_fetch_status,
            consecutive_failures,
            _iso(last_success_days) if last_success_days is not None else None,
            _iso(last_failure_days) if last_failure_days is not None else None,
            _iso(180),
        ),
    ).lastrowid


def _knowledge(
    db,
    source_identifier: str,
    *,
    knowledge_type: str = "curated_article",
    days: int = 5,
    author: str | None = None,
) -> int:
    return db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, approved, published_at, ingested_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            knowledge_type,
            f"https://{source_identifier}/post",
            f"https://{source_identifier}/post",
            author or source_identifier,
            f"Knowledge from {source_identifier}",
            1,
            _iso(days),
            _iso(days),
        ),
    ).lastrowid


def _citation(db, knowledge_id: int, *, days: int = 3) -> None:
    content_id = db.conn.execute(
        """INSERT INTO generated_content (content_type, content, eval_score, created_at)
           VALUES ('x_post', 'post', 8, ?)""",
        (_iso(days),),
    ).lastrowid
    db.conn.execute(
        """INSERT INTO content_knowledge_links (content_id, knowledge_id, relevance_score, created_at)
           VALUES (?, ?, 0.9, ?)""",
        (content_id, knowledge_id, _iso(days)),
    )


def test_ranks_quarantined_and_repeated_failures_ahead_of_old_sources(db):
    _source(
        db,
        "broken.example",
        active=0,
        status="paused",
        last_fetch_status="failure",
        consecutive_failures=4,
        last_success_days=120,
        last_failure_days=1,
    )
    _knowledge(db, "broken.example", days=120)
    _source(db, "old.example", last_success_days=140)
    _knowledge(db, "old.example", days=140)
    db.conn.commit()

    report = build_source_trust_decay_report(db, days=30, now=NOW)

    assert [item["identifier"] for item in report["items"]] == [
        "broken.example",
        "old.example",
    ]
    assert report["items"][0]["status"] == "critical"
    assert "quarantined_or_inactive" in report["items"][0]["drivers"]
    assert report["items"][1]["status"] == "review"


def test_recently_cited_and_recently_fetched_source_is_not_flagged(db):
    _source(db, "fresh.example", last_success_days=2)
    knowledge_id = _knowledge(db, "fresh.example", days=120)
    _citation(db, knowledge_id, days=1)
    db.conn.commit()

    hidden_report = build_source_trust_decay_report(db, days=30, now=NOW)
    full_report = build_source_trust_decay_report(
        db,
        days=30,
        include_healthy=True,
        now=NOW,
    )

    assert hidden_report["items"] == []
    item = full_report["items"][0]
    assert item["identifier"] == "fresh.example"
    assert item["status"] == "healthy"
    assert item["decay_score"] < 30
    assert {"recently_cited", "recently_fetched"}.issubset(item["drivers"])


def test_missing_license_and_stale_usage_create_stable_recommendation(db):
    _source(db, "license.example", license=None, last_success_days=45)
    knowledge_id = _knowledge(db, "license.example", days=45)
    _citation(db, knowledge_id, days=40)
    db.conn.commit()

    report = build_source_trust_decay_report(db, days=30, now=NOW)
    item = report["items"][0]

    assert item["decay_score"] == 64
    assert item["status"] == "review"
    assert item["drivers"] == [
        "missing_license",
        "old_source",
        "stale_fetch",
        "stale_citation_usage",
    ]
    assert item["recommendation"] == "review source freshness and refresh or replace stale knowledge"


def test_text_and_json_output_are_deterministic(db):
    _source(db, "old.example", last_success_days=140)
    _knowledge(db, "old.example", days=140)
    db.conn.commit()

    report = build_source_trust_decay_report(db, days=30, now=NOW)

    assert json.loads(format_json_report(report))["artifact_type"] == "source_trust_decay"
    assert format_text_report(report) == "\n".join(
        [
            "Source Trust Decay Report",
            "Generated: 2026-05-01T12:00:00+00:00",
            "Filters: days=30 limit=none include_healthy=no",
            "Counts: sources=1 critical=0 review=1 watch=0 healthy=0",
            "",
            "Sources",
            "  - #1 blog old.example score=69 status=review last_seen=2025-12-12T12:00:00+00:00 last_fetched=2025-12-12T12:00:00+00:00 last_cited=- drivers=very_old_source,stale_fetch,never_cited recommendation=review source freshness and refresh or replace stale knowledge",
        ]
    )


def test_partial_schema_treats_missing_optional_tables_as_unknown_signals():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE curated_sources (
            id INTEGER PRIMARY KEY,
            source_type TEXT,
            identifier TEXT,
            status TEXT,
            last_success_at TEXT
        );
        INSERT INTO curated_sources
          (id, source_type, identifier, status, last_success_at)
        VALUES
          (1, 'blog', 'minimal.example', 'active', '2026-01-01T12:00:00+00:00');
        """
    )

    report = build_source_trust_decay_report(conn, days=30, now=NOW)

    assert report["missing_required_tables"] == []
    assert report["unknown_optional_signals"] == [
        "knowledge",
        "content_knowledge_links",
        "reply_knowledge_links",
    ]
    assert report["items"][0]["identifier"] == "minimal.example"
    assert "unknown_citation_usage" in report["items"][0]["drivers"]


def test_missing_curated_sources_returns_empty_report():
    conn = sqlite3.connect(":memory:")

    report = build_source_trust_decay_report(conn, now=NOW)

    assert report["missing_required_tables"] == ["curated_sources"]
    assert report["items"] == []


def test_invalid_arguments_raise():
    with pytest.raises(ValueError, match="days"):
        build_source_trust_decay_report(sqlite3.connect(":memory:"), days=0)
    with pytest.raises(ValueError, match="limit"):
        build_source_trust_decay_report(sqlite3.connect(":memory:"), limit=0)


def test_cli_outputs_json(db, capsys, monkeypatch):
    _source(db, "old.example", last_success_days=140)
    _knowledge(db, "old.example", days=140)
    db.conn.commit()
    monkeypatch.setattr(source_trust_decay_script, "script_context", lambda: _script_context(db))

    assert source_trust_decay_script.main(["--format", "json", "--days", "30"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "source_trust_decay"
    assert payload["items"][0]["identifier"] == "old.example"
