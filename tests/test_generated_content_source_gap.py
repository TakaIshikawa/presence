"""Tests for generated content source gap report."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from evaluation.generated_content_source_gap import build_generated_content_source_gap_report_from_db


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "generated_content_source_gap.py"
spec = importlib.util.spec_from_file_location("generated_content_source_gap_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_flags_rows_with_no_source_evidence(monkeypatch, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE generated_content (id INTEGER, content_type TEXT, status TEXT, created_at TEXT, content TEXT, source_commit TEXT, source_content_ids TEXT, source_activity_ids TEXT, source_urls TEXT, metadata TEXT)"
    )
    conn.executemany(
        "INSERT INTO generated_content VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (1, "post", "draft", NOW.isoformat(), "No evidence body", "", "[]", "", "", "{}"),
            (2, "blog", "draft", NOW.isoformat(), "Has metadata", "", "", "", "", json.dumps({"source_urls": ["https://e.test"]})),
        ],
    )
    db = SimpleNamespace(conn=conn)

    report = build_generated_content_source_gap_report_from_db(db, now=NOW)

    assert report["summary"]["scanned_count"] == 2
    assert report["summary"]["gap_count"] == 1
    assert report["findings"][0]["content_id"] == "1"
    assert set(report["findings"][0]["missing_evidence_kinds"]) == {
        "source_commit",
        "source_content_ids",
        "source_activity_ids",
        "source_urls",
    }

    monkeypatch.setattr(script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        script,
        "build_generated_content_source_gap_report_from_db",
        lambda db, **kwargs: build_generated_content_source_gap_report_from_db(db, now=NOW, **kwargs),
    )
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "generated_content_source_gap"
    assert script.main(["--table"]) == 0
    assert "missing=source_commit" in capsys.readouterr().out
