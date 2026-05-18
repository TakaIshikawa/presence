"""Tests for knowledge citation freshness decay reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from evaluation.knowledge_citation_freshness_decay import (
    build_knowledge_citation_freshness_decay_report,
    build_knowledge_citation_freshness_decay_report_from_db,
    format_knowledge_citation_freshness_decay_json,
    format_knowledge_citation_freshness_decay_text,
)


NOW = datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "knowledge_citation_freshness_decay.py"
spec = importlib.util.spec_from_file_location("knowledge_citation_freshness_decay_script", SCRIPT_PATH)
knowledge_citation_freshness_decay_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(knowledge_citation_freshness_decay_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_buckets_citations_and_flags_stale_patterns():
    rows = [
        {"knowledge_id": "fresh", "evidence_at": (NOW - timedelta(days=5)).isoformat(), "used_at": NOW.isoformat(), "published": 1},
        {"knowledge_id": "aging", "evidence_at": (NOW - timedelta(days=60)).isoformat(), "used_at": NOW.isoformat(), "gate_status": "pass"},
        {"knowledge_id": "old-1", "evidence_at": (NOW - timedelta(days=140)).isoformat(), "used_at": NOW.isoformat(), "engagement": 0},
        {"knowledge_id": "old-2", "evidence_at": (NOW - timedelta(days=160)).isoformat(), "used_at": NOW.isoformat(), "published": 0},
        {"knowledge_id": "unknown", "used_at": NOW.isoformat(), "gate_status": "approved"},
    ]

    report = build_knowledge_citation_freshness_decay_report(rows, stale_usage_threshold=2, now=NOW)

    buckets = {row["age_bucket"]: row for row in report["age_buckets"]}
    assert set(buckets) == {"fresh", "aging", "stale", "unknown"}
    assert buckets["fresh"]["outcome_rate"] == 1.0
    assert buckets["stale"]["outcome_rate"] == 0.0
    assert {row["pattern"] for row in report["patterns"]} == {"stale_high_usage", "stale_low_outcome"}
    assert json.loads(format_knowledge_citation_freshness_decay_json(report))["totals"]["stale_citation_count"] == 2


def test_missing_engagement_columns_are_tolerated():
    report = build_knowledge_citation_freshness_decay_report(
        [{"knowledge_id": "k1", "evidence_at": (NOW - timedelta(days=100)).isoformat(), "used_at": NOW.isoformat()}],
        now=NOW,
    )

    assert report["age_buckets"][0]["average_engagement"] is None
    assert "No citation freshness decay patterns found." in format_knowledge_citation_freshness_decay_text(report)


def test_db_loader_reads_knowledge_and_generated_content(db):
    knowledge_id = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, content, approved, published_at, ingested_at)
           VALUES ('curated_article', 'k-old', 'Old evidence', 1, ?, ?)""",
        ((NOW - timedelta(days=150)).isoformat(), (NOW - timedelta(days=10)).isoformat()),
    ).lastrowid
    db.conn.execute(
        """INSERT INTO generated_content
           (content, content_type, source_activity_ids, eval_score, published, created_at)
           VALUES ('Draft', 'x_post', ?, 5.0, 0, ?)""",
        (json.dumps([knowledge_id]), NOW.isoformat()),
    )
    db.conn.commit()

    report = build_knowledge_citation_freshness_decay_report_from_db(db, stale_usage_threshold=1, now=NOW)

    assert report["age_buckets"][0]["age_bucket"] == "stale"
    assert report["patterns"][0]["pattern"] == "stale_high_usage"


def test_cli_outputs_json_and_text(db, file_db, capsys):
    for database in (db, file_db):
        database.conn.execute(
            """INSERT INTO knowledge
               (source_type, source_id, content, approved, published_at, ingested_at)
               VALUES ('curated_article', ?, 'Evidence', 1, ?, ?)""",
            (f"k-{id(database)}", (NOW - timedelta(days=3)).isoformat(), NOW.isoformat()),
        )
        database.conn.commit()

    with patch.object(knowledge_citation_freshness_decay_script, "script_context", wraps=lambda: _script_context(db)):
        assert knowledge_citation_freshness_decay_script.main(["--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["age_buckets"][0]["age_bucket"] == "fresh"

    assert knowledge_citation_freshness_decay_script.main(["--db", str(file_db.db_path), "--format", "text"]) == 0
    assert "Knowledge Citation Freshness Decay" in capsys.readouterr().out
    assert knowledge_citation_freshness_decay_script.main(["--days", "0"]) == 2
