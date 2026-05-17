"""Tests for reply relationship signal drift reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from engagement.reply_relationship_signal_drift import (
    build_reply_relationship_signal_drift_report,
    build_reply_relationship_signal_drift_report_from_db,
)


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_relationship_signal_drift.py"
spec = importlib.util.spec_from_file_location("reply_relationship_signal_drift_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_classifies_ignored_stale_stance_mismatch_and_healthy():
    contexts = [
        {"relationship_id": "rel-ignored", "summary": "They care about launch metrics", "updated_at": "2026-05-01T00:00:00+00:00"},
        {"relationship_id": "rel-stale", "summary": "Old Kubernetes migration context", "updated_at": "2025-01-01T00:00:00+00:00"},
        {"relationship_id": "rel-healthy", "summary": "They are exploring evaluation reliability", "updated_at": "2026-05-10T00:00:00+00:00"},
    ]
    replies = [
        {
            "reply_id": "ignored",
            "relationship_id": "rel-ignored",
            "draft_text": "Thanks for sharing.",
            "final_reply_text": "Thanks for sharing this update.",
            "intended_stance": "supportive",
            "final_stance": "supportive",
        },
        {
            "reply_id": "stale",
            "relationship_id": "rel-stale",
            "draft_text": "The Kubernetes migration sounds relevant.",
            "final_reply_text": "That old Kubernetes migration context still seems relevant.",
            "intended_stance": "supportive",
            "final_stance": "supportive",
        },
        {
            "reply_id": "stance",
            "relationship_id": "rel-healthy",
            "draft_text": "I am curious about your evaluation reliability work.",
            "final_reply_text": "I disagree with the evaluation reliability framing and see risk.",
            "intended_stance": "curious",
            "final_stance": "challenging",
        },
        {
            "reply_id": "healthy",
            "relationship_id": "rel-healthy",
            "draft_text": "Curious how evaluation reliability is working.",
            "final_reply_text": "I am curious how your evaluation reliability work is going.",
            "intended_stance": "curious",
            "final_stance": "curious",
        },
    ]

    report = build_reply_relationship_signal_drift_report(replies, contexts, now=NOW, stale_days=90)

    by_id = {item["reply_id"]: item for item in report["replies"]}
    assert by_id["ignored"]["classification"] == "ignored_context"
    assert "stale_context_used" in by_id["stale"]["flags"]
    assert "stance_mismatch" in by_id["stance"]["flags"]
    assert by_id["healthy"]["classification"] == "healthy"
    assert report["totals"]["classification_counts"]["healthy"] == 1
    assert report["totals"]["flag_counts"]["ignored_context"] == 1


def test_empty_dataset_returns_empty_state():
    report = build_reply_relationship_signal_drift_report([], [], now=NOW)

    assert report["totals"]["reply_count"] == 0
    assert report["replies"] == []
    assert report["empty_state"]["is_empty"] is True


def test_db_loader_and_cli_emit_json_and_text(monkeypatch, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE reply_queue (
            id INTEGER, inbound_author TEXT, draft_text TEXT, posted_text TEXT,
            relationship_context TEXT, intent TEXT, detected_at TEXT
        )"""
    )
    conn.execute(
        "INSERT INTO reply_queue VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            1,
            "rel-db",
            "Curious about evaluation reliability.",
            "Curious about your evaluation reliability update.",
            json.dumps({"relationship_summary": "They discuss evaluation reliability", "updated_at": NOW.isoformat()}),
            "curious",
            NOW.isoformat(),
        ),
    )
    db = SimpleNamespace(conn=conn)

    report = build_reply_relationship_signal_drift_report_from_db(db, now=NOW)

    assert report["totals"]["reply_count"] == 1
    assert report["replies"][0]["classification"] == "healthy"

    monkeypatch.setattr(script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        script,
        "build_reply_relationship_signal_drift_report_from_db",
        lambda db, **kwargs: build_reply_relationship_signal_drift_report_from_db(db, now=NOW, **kwargs),
    )
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "reply_relationship_signal_drift"
    assert script.main(["--table"]) == 0
    assert "Reply Relationship Signal Drift" in capsys.readouterr().out
