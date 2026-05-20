"""Tests for knowledge embedding backfill priority reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.knowledge_embedding_backfill_priority import (
    build_knowledge_embedding_backfill_priority_report,
    build_knowledge_embedding_backfill_priority_report_from_db,
    format_knowledge_embedding_backfill_priority_json,
    format_knowledge_embedding_backfill_priority_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "knowledge_embedding_backfill_priority.py"
spec = importlib.util.spec_from_file_location("knowledge_embedding_backfill_priority_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _ts(days_ago: int) -> str:
    return (NOW - timedelta(days=days_ago)).strftime("%Y-%m-%d %H:%M:%S")


def _conn(path: str | Path = ":memory:") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE knowledge (
            id INTEGER PRIMARY KEY,
            source_type TEXT,
            source_id TEXT,
            source_url TEXT,
            content TEXT,
            embedding BLOB,
            embedding_model TEXT,
            updated_at TEXT,
            created_at TEXT
        )"""
    )
    return conn


def _knowledge(
    conn: sqlite3.Connection,
    *,
    knowledge_id: int,
    source_type: str = "own_post",
    embedding: bytes | None = b"vector",
    embedding_model: str | None = "text-embedding-3-small",
    days_ago: int = 1,
) -> None:
    conn.execute(
        """INSERT INTO knowledge
           (id, source_type, source_id, source_url, content, embedding, embedding_model, updated_at, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            knowledge_id,
            source_type,
            f"src-{knowledge_id}",
            f"https://e.test/{knowledge_id}",
            f"Knowledge row {knowledge_id}",
            embedding,
            embedding_model,
            _ts(days_ago),
            _ts(days_ago),
        ),
    )
    conn.commit()


def test_builder_flags_missing_embeddings_missing_model_and_stale_rows():
    report = build_knowledge_embedding_backfill_priority_report(
        [
            {
                "knowledge_id": 1,
                "source_type": "own_post",
                "source_id": "a",
                "content": "missing vector",
                "embedding": None,
                "embedding_model": "text-embedding-3-small",
                "updated_at": _ts(1),
            },
            {
                "knowledge_id": 2,
                "source_type": "own_post",
                "content": "missing model",
                "embedding": b"vector",
                "embedding_model": "",
                "updated_at": _ts(1),
            },
            {
                "knowledge_id": 3,
                "source_type": "own_post",
                "content": "stale",
                "embedding": b"vector",
                "embedding_model": "text-embedding-3-small",
                "updated_at": _ts(120),
            },
        ],
        stale_days=90,
        now=NOW,
    )

    reasons = {item["knowledge_id"]: item["reasons"] for item in report["prioritized_items"]}
    assert "missing_embedding" in reasons[1]
    assert "missing_embedding_model" in reasons[2]
    assert "stale_knowledge" in reasons[3]
    assert report["artifact_type"] == "knowledge_embedding_backfill_priority"
    assert report["totals"]["rows_scanned"] == 3


def test_source_type_priority_ordering_prefers_semantic_search_sources():
    report = build_knowledge_embedding_backfill_priority_report(
        [
            {"knowledge_id": 1, "source_type": "own_post", "embedding": b"v", "embedding_model": "m", "updated_at": _ts(1)},
            {
                "knowledge_id": 2,
                "source_type": "curated_article",
                "embedding": b"v",
                "embedding_model": "m",
                "updated_at": _ts(1),
            },
            {
                "knowledge_id": 3,
                "source_type": "curated_newsletter",
                "embedding": b"v",
                "embedding_model": "m",
                "updated_at": _ts(1),
            },
        ],
        now=NOW,
    )

    assert [item["knowledge_id"] for item in report["prioritized_items"]] == [2, 3, 1]
    assert report["prioritized_items"][0]["reasons"] == ["priority_source_type"]


def test_db_loader_handles_missing_table_and_optional_embedding_columns():
    empty = sqlite3.connect(":memory:")
    empty.row_factory = sqlite3.Row
    missing_table = build_knowledge_embedding_backfill_priority_report_from_db(empty, now=NOW)
    assert missing_table["missing_schema"]["missing_tables"] == ["knowledge"]
    assert missing_table["prioritized_items"] == []

    partial = sqlite3.connect(":memory:")
    partial.row_factory = sqlite3.Row
    partial.execute("CREATE TABLE knowledge (id INTEGER PRIMARY KEY, source_type TEXT, content TEXT, created_at TEXT)")
    partial.execute("INSERT INTO knowledge VALUES (?, ?, ?, ?)", (1, "curated_article", "copy", _ts(1)))
    partial.commit()
    report = build_knowledge_embedding_backfill_priority_report_from_db(partial, now=NOW)

    assert report["missing_schema"]["missing_columns"]["knowledge"] == ["embedding", "embedding_model", "updated_at"]
    assert report["prioritized_items"][0]["reasons"] == [
        "missing_embedding",
        "missing_embedding_model",
        "priority_source_type",
    ]


def test_formatters_are_stable_and_readable():
    report = build_knowledge_embedding_backfill_priority_report(
        [{"knowledge_id": 1, "source_type": "curated_article", "embedding": None, "created_at": _ts(100)}],
        now=NOW,
    )

    payload = json.loads(format_knowledge_embedding_backfill_priority_json(report))
    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "knowledge_embedding_backfill_priority"
    text = format_knowledge_embedding_backfill_priority_text(report)
    assert "Knowledge Embedding Backfill Priority" in text
    assert "knowledge_id=1" in text
    assert "missing_embedding" in text


def test_cli_supports_db_json_text_and_argument_validation(tmp_path, monkeypatch, capsys):
    db_path = tmp_path / "knowledge.sqlite"
    conn = _conn(db_path)
    _knowledge(conn, knowledge_id=1, embedding=None)
    conn.close()

    assert script.main(["--db", str(db_path), "--format", "json", "--limit", "5", "--stale-days", "30"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "knowledge_embedding_backfill_priority"
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Knowledge Embedding Backfill Priority" in capsys.readouterr().out

    memory = _conn()
    monkeypatch.setattr(script, "script_context", lambda: _script_context(memory))
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["prioritized_items"] == []
    assert script.main(["--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_builder_rejects_invalid_arguments():
    with pytest.raises(ValueError, match="limit must be positive"):
        build_knowledge_embedding_backfill_priority_report([], limit=0)
    with pytest.raises(ValueError, match="stale_days must be positive"):
        build_knowledge_embedding_backfill_priority_report([], stale_days=0)
