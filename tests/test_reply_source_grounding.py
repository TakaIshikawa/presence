"""Tests for reply source grounding reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from engagement.reply_source_grounding import build_reply_source_grounding_report


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_source_grounding.py"
spec = importlib.util.spec_from_file_location("reply_source_grounding_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY, draft_text TEXT, mention_text TEXT, context_text TEXT,
            source_url TEXT, relationship_context TEXT, status TEXT
        )"""
    )
    return SimpleNamespace(conn=conn)


def test_classifies_context_rich_context_free_and_generic_replies():
    db = _db()
    rows = [
        (1, "The migration retry window and timeout logs point to the queue worker.", "Why did the migration retry timeout?", "Queue worker timeout logs show migration retry failures.", "https://e.test/runbook", "customer reported before", "draft"),
        (2, "Thanks, this is useful. I will take a look.", "", "", "", "", "draft"),
        (3, "The billing dashboard should help your launch planning.", "How do we debug queue worker retries?", "Queue worker retry failures during migration.", "https://e.test/runbook", "", "draft"),
        (4, "Queue worker retries match the migration failure pattern.", "How do we debug queue worker retries?", "Queue worker retry failures during migration.", "", "", "draft"),
    ]
    db.conn.executemany("INSERT INTO reply_queue VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
    db.conn.commit()

    report = build_reply_source_grounding_report(db, min_overlap=0.25, now=NOW)
    statuses = {item["reply_id"]: item["grounding_status"] for item in report["findings"]}

    assert statuses["1"] == "grounded"
    assert statuses["2"] == "missing_context"
    assert statuses["3"] == "weak_overlap"
    assert statuses["4"] == "missing_evidence"
    assert report["totals"]["weak_grounding_rate"] == 0.75


def test_cli_json_and_table_output(monkeypatch, capsys):
    db = _db()
    db.conn.execute(
        "INSERT INTO reply_queue VALUES (?, ?, ?, ?, ?, ?, ?)",
        (1, "Queue worker retries match the logs.", "Queue worker retry?", "Queue worker retry logs.", "https://e.test", "", "draft"),
    )
    db.conn.commit()
    monkeypatch.setattr(script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        script,
        "build_reply_source_grounding_report",
        lambda db, **kwargs: build_reply_source_grounding_report(db, now=NOW, **kwargs),
    )
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "reply_source_grounding"
    assert script.main(["--table"]) == 0
    assert "Reply Source Grounding" in capsys.readouterr().out
