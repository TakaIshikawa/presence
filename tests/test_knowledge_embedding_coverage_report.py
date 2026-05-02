"""Tests for knowledge embedding coverage reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

from knowledge.embedding_coverage_report import (
    build_knowledge_embedding_coverage_report,
    format_knowledge_embedding_coverage_json,
    format_knowledge_embedding_coverage_text,
)
from knowledge.embeddings import serialize_embedding


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "report_knowledge_embedding_coverage.py"
)
spec = importlib.util.spec_from_file_location(
    "report_knowledge_embedding_coverage_script", SCRIPT_PATH
)
report_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(report_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _add_knowledge(
    db,
    *,
    source_type: str,
    source_id: str,
    content: str,
    insight: str | None = None,
    embedding: bytes | None = None,
    metadata: dict | None = None,
) -> int:
    cursor = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, insight,
            embedding, approved, metadata)
           VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)""",
        (
            source_type,
            source_id,
            f"https://example.com/{source_id}",
            "Author",
            content,
            insight,
            embedding,
            json.dumps(metadata, sort_keys=True) if metadata is not None else None,
        ),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def test_report_counts_missing_embedded_stale_and_source_type_breakdowns(db):
    embedding = serialize_embedding([0.1, 0.2, 0.3])
    missing_id = _add_knowledge(
        db,
        source_type="curated_article",
        source_id="missing",
        content="Needs an embedding",
    )
    embedded_id = _add_knowledge(
        db,
        source_type="curated_article",
        source_id="embedded",
        content="Already embedded",
        embedding=embedding,
        metadata={"embedding_text_sha256": _hash("Already embedded")},
    )
    stale_id = _add_knowledge(
        db,
        source_type="own_post",
        source_id="stale",
        content="Current text",
        embedding=embedding,
        metadata={"embedding_text_sha256": _hash("Old text")},
    )

    report = build_knowledge_embedding_coverage_report(db, limit=5, now=NOW)
    payload = json.loads(format_knowledge_embedding_coverage_json(report))

    assert payload["totals"] == {
        "embedded_count": 2,
        "missing_count": 1,
        "stale_count": 1,
        "total_knowledge_items": 3,
    }
    assert payload["samples"] == {
        "missing_item_ids": [missing_id],
        "stale_item_ids": [stale_id],
    }
    by_type = {bucket["source_type"]: bucket for bucket in payload["by_source_type"]}
    assert by_type["curated_article"]["total_count"] == 2
    assert by_type["curated_article"]["embedded_count"] == 1
    assert by_type["curated_article"]["missing_count"] == 1
    assert by_type["curated_article"]["stale_count"] == 0
    assert by_type["own_post"]["stale_count"] == 1
    by_source = {(bucket["source_type"], bucket["source_id"]): bucket for bucket in payload["by_source"]}
    assert by_source[("curated_article", "embedded")]["embedded_count"] == 1
    assert by_source[("own_post", "stale")]["stale_count"] == 1
    assert embedded_id


def test_limit_controls_representative_missing_and_stale_samples(db):
    embedding = serialize_embedding([0.1, 0.2])
    missing_ids = [
        _add_knowledge(
            db,
            source_type="curated_article",
            source_id=f"missing-{index}",
            content=f"Missing {index}",
        )
        for index in range(3)
    ]
    stale_ids = [
        _add_knowledge(
            db,
            source_type="curated_x",
            source_id=f"stale-{index}",
            content=f"Fresh {index}",
            embedding=embedding,
            metadata={"embedding": {"text_hash": _hash(f"Old {index}")}},
        )
        for index in range(3)
    ]

    report = build_knowledge_embedding_coverage_report(db, limit=2, now=NOW)

    assert report.samples["missing_item_ids"] == missing_ids[:2]
    assert report.samples["stale_item_ids"] == stale_ids[:2]


def test_insight_is_used_for_embedding_metadata_freshness(db):
    embedding = serialize_embedding([0.1, 0.2])
    _add_knowledge(
        db,
        source_type="curated_newsletter",
        source_id="fresh-insight",
        content="Long source body",
        insight="Embedded summary",
        embedding=embedding,
        metadata={"embedding_text_sha256": _hash("Embedded summary")},
    )

    report = build_knowledge_embedding_coverage_report(db, now=NOW)

    assert report.totals["stale_count"] == 0


def test_empty_database_cli_emits_valid_json(db, capsys):
    with patch.object(report_script, "script_context", lambda: _script_context(db)):
        exit_code = report_script.main([])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 0
    assert payload["artifact_type"] == "knowledge_embedding_coverage"
    assert payload["totals"]["total_knowledge_items"] == 0
    assert payload["totals"]["embedded_count"] == 0
    assert payload["totals"]["missing_count"] == 0
    assert payload["totals"]["stale_count"] == 0


def test_missing_knowledge_table_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    report = build_knowledge_embedding_coverage_report(conn, limit=1, now=NOW)
    payload = json.loads(format_knowledge_embedding_coverage_json(report))

    assert payload["missing_tables"] == ["knowledge"]
    assert payload["samples"] == {"missing_item_ids": [], "stale_item_ids": []}
    assert payload["totals"]["total_knowledge_items"] == 0


def test_text_report_is_stable(db):
    _add_knowledge(
        db,
        source_type="curated_article",
        source_id="missing",
        content="Needs an embedding",
    )

    text = format_knowledge_embedding_coverage_text(
        build_knowledge_embedding_coverage_report(db, limit=1, now=NOW)
    )

    assert "Knowledge Embedding Coverage" in text
    assert "1 knowledge rows, 0 embedded, 1 missing, 0 stale" in text
    assert "curated_article: total=1 embedded=0 missing=1 stale=0" in text
