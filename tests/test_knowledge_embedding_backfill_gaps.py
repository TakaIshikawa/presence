"""Tests for knowledge embedding backfill gaps."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from evaluation.knowledge_embedding_backfill_gaps import build_knowledge_embedding_backfill_gaps_report_from_db


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "knowledge_embedding_backfill_gaps.py"
spec = importlib.util.spec_from_file_location("knowledge_embedding_backfill_gaps_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_identifies_embedding_gap_reasons(monkeypatch, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE knowledge_sources (id TEXT, source_type TEXT, title TEXT, url TEXT, metadata TEXT, updated_at TEXT)")
    conn.execute("CREATE TABLE knowledge_embeddings (source_id TEXT, embedding TEXT, embedding_model TEXT)")
    conn.executemany(
        "INSERT INTO knowledge_sources VALUES (?, ?, ?, ?, ?, ?)",
        [
            ("s1", "doc", "Missing", "", "{}", NOW.isoformat()),
            ("s2", "doc", "Empty", "", "{}", NOW.isoformat()),
            ("s3", "url", "Stale", "https://e.test", "{}", NOW.isoformat()),
            ("s4", "doc", "Failed", "", json.dumps({"embedding_failed": True}), NOW.isoformat()),
        ],
    )
    conn.executemany(
        "INSERT INTO knowledge_embeddings VALUES (?, ?, ?)",
        [("s2", "[]", "text-embedding-3-small"), ("s3", "[0.1]", "old-model"), ("s4", "[0.2]", "text-embedding-3-small")],
    )
    db = SimpleNamespace(conn=conn)

    report = build_knowledge_embedding_backfill_gaps_report_from_db(db, now=NOW, expected_model="text-embedding-3-small")

    reasons = {item["source_id"]: item["reason_code"] for item in report["findings"]}
    assert reasons == {"s1": "missing", "s2": "empty", "s3": "stale_model", "s4": "failed_embedding"}
    assert report["summary"]["counts_by_reason"]["missing"] == 1

    monkeypatch.setattr(script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        script,
        "build_knowledge_embedding_backfill_gaps_report_from_db",
        lambda db, **kwargs: build_knowledge_embedding_backfill_gaps_report_from_db(db, now=NOW, **kwargs),
    )
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "knowledge_embedding_backfill_gaps"
    assert script.main(["--table"]) == 0
    assert "reason=stale_model" in capsys.readouterr().out
