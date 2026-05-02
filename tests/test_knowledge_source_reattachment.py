"""Tests for knowledge source reattachment planning."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

from knowledge.orphan_source_reattachment import (
    build_knowledge_source_reattachment_plan,
    format_knowledge_source_reattachment_json,
    format_knowledge_source_reattachment_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "plan_knowledge_source_reattachment.py"
spec = importlib.util.spec_from_file_location("plan_knowledge_source_reattachment", SCRIPT_PATH)
plan_reattachment_cli = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(plan_reattachment_cli)

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_knowledge(
    db,
    *,
    source_type: str = "curated_article",
    source_id: str | None = None,
    source_url: str | None = None,
    content: str = "Latency checklist made releases quieter",
    insight: str = "Latency checklist release reliability",
    metadata: dict | None = None,
    published_at: str = "2026-04-20T10:00:00+00:00",
) -> int:
    cursor = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, insight,
            attribution_required, license, approved, published_at, metadata)
           VALUES (?, ?, ?, 'author', ?, ?, 1, 'attribution_required', 1, ?, ?)""",
        (
            source_type,
            source_id,
            source_url,
            content,
            insight,
            published_at,
            json.dumps(metadata or {}, sort_keys=True),
        ),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def _insert_curated_source(
    db,
    *,
    identifier: str = "example.com",
    source_type: str = "blog",
    canonical_url: str = "https://example.com/latency-checklist",
    feed_url: str = "https://example.com/feed",
    link_title: str = "Latency checklist release reliability",
    published_at: str = "2026-04-20T09:30:00+00:00",
) -> int:
    cursor = db.conn.execute(
        """INSERT INTO curated_sources
           (source_type, identifier, name, feed_url, canonical_url, link_title,
            site_name, published_at, active, status)
           VALUES (?, ?, ?, ?, ?, ?, 'Example', ?, 1, 'active')""",
        (
            source_type,
            identifier,
            identifier,
            feed_url,
            canonical_url,
            link_title,
            published_at,
        ),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def test_exact_url_match_from_orphan_source_id_to_curated_source(db):
    orphan_id = _insert_knowledge(
        db,
        source_id="https://example.com/latency-checklist?utm_source=newsletter",
        source_url=None,
        metadata={"link_metadata": {"title": "Old title"}},
    )
    curated_id = _insert_curated_source(db)

    plan = build_knowledge_source_reattachment_plan(
        db,
        min_confidence=0.5,
        now=NOW,
    )

    assert plan.totals["orphaned_item_count"] == 1
    item = plan.orphaned_items[0]
    assert item.knowledge_id == orphan_id
    assert "missing_source_url" in item.reason_codes
    candidate = item.candidates[0]
    assert candidate.source_table == "curated_sources"
    assert candidate.source_row_id == curated_id
    assert "exact_url_match" in candidate.reason_codes
    assert candidate.confidence >= 0.9
    assert candidate.recommended_update["set"]["source_url"] == "https://example.com/latency-checklist"


def test_title_overlap_match_recommends_candidate_without_url_hint(db):
    orphan_id = _insert_knowledge(
        db,
        source_id=None,
        source_url="",
        content="Latency checklist release reliability",
        insight="Latency checklist release reliability",
        metadata={"link_metadata": {"title": "Latency checklist release reliability"}},
    )
    _insert_curated_source(
        db,
        canonical_url="https://ops.example.com/checklist",
        identifier="ops.example.com",
        link_title="Latency checklist release reliability",
    )

    plan = build_knowledge_source_reattachment_plan(db, min_confidence=0.55, now=NOW)

    item = plan.orphaned_items[0]
    assert item.knowledge_id == orphan_id
    assert item.candidates
    assert "title_overlap_match" in item.candidates[0].reason_codes
    assert item.candidates[0].canonical_url == "https://ops.example.com/checklist"


def test_low_confidence_candidates_are_suppressed(db):
    _insert_knowledge(
        db,
        source_id=None,
        source_url=None,
        content="Queue backpressure reduced deploy risk",
        insight="Queue backpressure reduced deploy risk",
    )
    _insert_curated_source(
        db,
        canonical_url="https://example.com/gardening",
        link_title="Tomato planting calendar",
    )

    plan = build_knowledge_source_reattachment_plan(
        db,
        min_confidence=0.8,
        now=NOW,
    )

    assert len(plan.orphaned_items) == 1
    assert plan.orphaned_items[0].candidates == ()
    assert plan.totals["candidate_count"] == 0


def test_nearby_ingested_knowledge_can_be_candidate(db):
    _insert_knowledge(
        db,
        source_url="https://example.com/source",
        content="Deployment checklist catches missing rollback step",
        insight="Deployment checklist catches rollback gaps",
        metadata={"link_metadata": {"title": "Deployment checklist catches rollback gaps"}},
    )
    _insert_knowledge(
        db,
        source_url=None,
        content="Deployment checklist catches rollback gaps",
        insight="Deployment checklist catches rollback gaps",
        metadata={"link_metadata": {"title": "Deployment checklist catches rollback gaps"}},
    )

    plan = build_knowledge_source_reattachment_plan(db, min_confidence=0.55, now=NOW)

    candidate = plan.orphaned_items[0].candidates[0]
    assert candidate.source_table == "knowledge"
    assert "nearby_ingested_item" in candidate.reason_codes
    assert candidate.source_url == "https://example.com/source"


def test_missing_optional_link_metadata_table_returns_capability_warning():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE knowledge (
            id INTEGER PRIMARY KEY,
            source_type TEXT,
            source_id TEXT,
            source_url TEXT,
            content TEXT,
            insight TEXT,
            published_at TEXT,
            ingested_at TEXT,
            metadata TEXT,
            created_at TEXT
        )"""
    )
    conn.execute(
        """INSERT INTO knowledge
           (id, source_type, content, insight, created_at)
           VALUES (1, 'curated_article', 'content', 'insight', '2026-04-20T00:00:00+00:00')"""
    )

    plan = build_knowledge_source_reattachment_plan(conn, now=NOW)

    assert plan.missing_optional_tables == ("link_metadata",)
    assert plan.capability_warnings
    assert "Optional link metadata table 'link_metadata' is unavailable" in plan.capability_warnings[0]
    assert plan.missing_required_tables == ()
    assert plan.orphaned_items[0].knowledge_id == 1


def test_missing_knowledge_table_returns_stable_empty_plan():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    plan = build_knowledge_source_reattachment_plan(conn, now=NOW)

    assert plan.missing_required_tables == ("knowledge",)
    assert plan.orphaned_items == ()
    assert plan.totals["orphaned_item_count"] == 0


def test_json_text_and_cli_output_are_stable(db, capsys):
    _insert_knowledge(
        db,
        source_id="https://example.com/latency-checklist",
        source_url=None,
    )
    _insert_curated_source(db)

    report = build_knowledge_source_reattachment_plan(db, now=NOW)
    payload = json.loads(format_knowledge_source_reattachment_json(report))

    assert payload["artifact_type"] == "knowledge_source_reattachment_plan"
    assert payload["totals"]["candidate_count"] == 1
    assert sorted(payload) == [
        "artifact_type",
        "capability_warnings",
        "filters",
        "generated_at",
        "missing_optional_tables",
        "missing_required_columns",
        "missing_required_tables",
        "orphaned_items",
        "totals",
    ]
    text = format_knowledge_source_reattachment_text(report)
    assert "Knowledge Source Reattachment Plan" in text
    assert "exact_url_match" in text

    with patch.object(
        plan_reattachment_cli,
        "script_context",
        return_value=_script_context(db),
    ):
        exit_code = plan_reattachment_cli.main(
            ["--limit", "5", "--min-confidence", "0.5", "--source-type", "curated_article", "--json"]
        )

    assert exit_code == 0
    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["filters"]["source_type"] == "curated_article"
    assert cli_payload["totals"]["item_with_candidate_count"] == 1
