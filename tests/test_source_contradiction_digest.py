"""Tests for curated source contradiction digests."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from knowledge.source_contradiction_digest import (
    build_source_contradiction_digest_report,
    format_source_contradiction_digest_json,
    format_source_contradiction_digest_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "source_contradiction_digest.py"
spec = importlib.util.spec_from_file_location("source_contradiction_digest_script", SCRIPT_PATH)
source_contradiction_digest_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(source_contradiction_digest_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _add_knowledge(
    db,
    *,
    content: str,
    insight: str | None = None,
    source_type: str = "curated_article",
    source_id: str,
    days_ago: int = 1,
    metadata: dict | None = None,
) -> int:
    timestamp = (NOW - timedelta(days=days_ago)).isoformat()
    cursor = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, insight,
            approved, published_at, ingested_at, metadata)
           VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?)""",
        (
            source_type,
            source_id,
            f"https://example.test/{source_id}",
            source_id.title(),
            content,
            insight,
            timestamp,
            timestamp,
            json.dumps(metadata or {}, sort_keys=True),
        ),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def test_negation_conflict_same_topic_reports_sources_and_excerpts(db):
    left_id = _add_knowledge(
        db,
        source_id="alpha",
        content="Redis vector indexes do not support hybrid filters in serverless plans.",
        metadata={"topic": "redis vector indexes"},
    )
    right_id = _add_knowledge(
        db,
        source_id="beta",
        content="Redis vector indexes support hybrid filters in serverless plans.",
        metadata={"topic": "redis vector indexes", "trust_score": 0.9},
    )

    report = build_source_contradiction_digest_report(db, days=7, now=NOW)

    assert report.total_rows == 2
    assert report.pair_count == 1
    pair = report.pairs[0]
    assert pair.left_source_id == left_id
    assert pair.right_source_id == right_id
    assert pair.conflict_type == "negation"
    assert pair.topic == "redis vector indexes"
    assert "do not support" in pair.left_excerpt
    assert "support hybrid filters" in pair.right_excerpt
    assert pair.right_source_trust == 0.9
    assert pair.confidence_score > 0.75


def test_numeric_and_version_conflicts_are_detected_and_ranked(db):
    numeric_left = _add_knowledge(
        db,
        source_id="numeric-left",
        content="The Acme API benchmark handles 42 requests per second.",
        metadata={"topic": "acme api benchmark"},
    )
    numeric_right = _add_knowledge(
        db,
        source_id="numeric-right",
        content="The Acme API benchmark handles 24 requests per second.",
        metadata={"topic": "acme api benchmark"},
    )
    version_left = _add_knowledge(
        db,
        source_id="version-left",
        content="React 19.3 is the supported version for the compiler workflow.",
        metadata={"topic": "react compiler workflow", "source_tier": "gold"},
        days_ago=1,
    )
    version_right = _add_knowledge(
        db,
        source_id="version-right",
        content="React 19.2 is the supported version for the compiler workflow.",
        metadata={"topic": "react compiler workflow", "source_tier": "gold"},
        days_ago=1,
    )

    report = build_source_contradiction_digest_report(db, days=7, limit=10, now=NOW)

    by_type = {pair.conflict_type: pair for pair in report.pairs}
    assert by_type["numeric"].left_source_id == numeric_left
    assert by_type["numeric"].right_source_id == numeric_right
    assert by_type["version"].left_source_id == version_left
    assert by_type["version"].right_source_id == version_right
    assert report.pairs[0].conflict_type == "version"


def test_filters_by_days_source_type_and_limit(db):
    included_left = _add_knowledge(
        db,
        source_id="included-left",
        source_type="curated_newsletter",
        content="Postgres vector search supports HNSW indexes.",
        metadata={"topic": "postgres vector search"},
    )
    included_right = _add_knowledge(
        db,
        source_id="included-right",
        source_type="curated_newsletter",
        content="Postgres vector search does not support HNSW indexes.",
        metadata={"topic": "postgres vector search"},
    )
    _add_knowledge(
        db,
        source_id="wrong-type",
        source_type="curated_article",
        content="Postgres vector search does not support HNSW indexes.",
        metadata={"topic": "postgres vector search"},
    )
    _add_knowledge(
        db,
        source_id="old",
        source_type="curated_newsletter",
        content="Postgres vector search does not support HNSW indexes.",
        metadata={"topic": "postgres vector search"},
        days_ago=30,
    )

    report = build_source_contradiction_digest_report(
        db,
        days=7,
        source_type="curated_newsletter",
        limit=1,
        now=NOW,
    )

    assert report.filters["source_type"] == "curated_newsletter"
    assert report.total_rows == 2
    assert [(pair.left_source_id, pair.right_source_id) for pair in report.pairs] == [
        (included_left, included_right)
    ]


def test_missing_table_and_optional_columns_are_reported():
    missing = sqlite3.connect(":memory:")
    missing.row_factory = sqlite3.Row
    report = build_source_contradiction_digest_report(missing, now=NOW)

    assert report.total_rows == 0
    assert report.metadata["availability"]["knowledge"] is False
    assert "Missing tables: knowledge" in format_source_contradiction_digest_text(report)

    minimal = sqlite3.connect(":memory:")
    minimal.row_factory = sqlite3.Row
    minimal.executescript(
        """
        CREATE TABLE knowledge (
            id INTEGER PRIMARY KEY,
            source_type TEXT,
            content TEXT NOT NULL
        );
        INSERT INTO knowledge (id, source_type, content)
        VALUES (1, 'curated_article', 'React 19.3 is supported.');
        """
    )
    minimal_report = build_source_contradiction_digest_report(minimal, now=NOW)

    assert minimal_report.total_rows == 1
    assert "metadata" in minimal_report.metadata["missing_columns"]["knowledge"]
    assert "Missing optional knowledge columns:" in format_source_contradiction_digest_text(
        minimal_report
    )


def test_json_output_is_stable_and_cli_supports_formats(db, monkeypatch, capsys):
    left_id = _add_knowledge(
        db,
        source_id="cli-left",
        content="SQLite vector search supports metadata filters.",
        metadata={"topic": "sqlite vector search"},
    )
    _add_knowledge(
        db,
        source_id="cli-right",
        content="SQLite vector search does not support metadata filters.",
        metadata={"topic": "sqlite vector search"},
    )

    report = build_source_contradiction_digest_report(db, days=7, now=NOW)
    payload = json.loads(format_source_contradiction_digest_json(report))

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "source_contradiction_digest"
    assert payload["pairs"][0]["left_source_id"] == left_id

    monkeypatch.setattr(
        source_contradiction_digest_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        source_contradiction_digest_script,
        "build_source_contradiction_digest_report",
        lambda db, **kwargs: build_source_contradiction_digest_report(db, now=NOW, **kwargs),
    )

    exit_code = source_contradiction_digest_script.main(
        ["--format", "json", "--days", "7", "--limit", "1"]
    )
    cli_payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert cli_payload["filters"]["days"] == 7
    assert cli_payload["filters"]["limit"] == 1
    assert cli_payload["pair_count"] == 1

    text_exit = source_contradiction_digest_script.main(["--format", "text"])
    assert text_exit == 0
    assert "Source Contradiction Digest" in capsys.readouterr().out

    invalid = source_contradiction_digest_script.main(["--days", "0"])
    captured = capsys.readouterr()
    assert invalid == 2
    assert "value must be positive" in captured.err
