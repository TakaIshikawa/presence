"""Tests for reply draft approval outcome reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from engagement.reply_draft_approval_outcome import build_reply_draft_approval_outcome_report, build_reply_draft_approval_outcome_report_from_db


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_draft_approval_outcome.py"
spec = importlib.util.spec_from_file_location("reply_draft_approval_outcome_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_summarizes_outcomes_context_and_score_buckets():
    report = build_reply_draft_approval_outcome_report(
        [
            {"id": "d1", "mention_id": "m1", "score": 0.9, "relationship_context": "known", "status": "approved", "created_at": "2026-04-30T00:00:00+00:00"},
            {"id": "d2", "mention_id": "m2", "score": 40, "status": "rejected", "created_at": "2026-04-29T00:00:00+00:00"},
            {"id": "d3", "score": 70, "status": "needs_revision"},
            {"id": "d4"},
        ],
        now=NOW,
    )

    assert report["totals"]["draft_count"] == 4
    assert report["totals"]["approved"] == 1
    assert report["totals"]["rejected"] == 1
    assert report["totals"]["revised"] == 1
    assert report["totals"]["pending"] == 1
    assert report["totals"]["approval_rate"] == 0.25
    assert report["drafts"][0]["context_status"] in {"has_context", "missing_context"}


def test_per_draft_record_fields_and_empty_state():
    report = build_reply_draft_approval_outcome_report([{"draft_id": "d1", "confidence": 0.5, "context_summary": "thread", "decision": "sent"}], now=NOW)
    row = report["drafts"][0]
    assert row["draft_id"] == "d1"
    assert row["score_bucket"] == "medium"
    assert row["context_status"] == "has_context"
    assert row["review_outcome"] == "approved"

    empty = build_reply_draft_approval_outcome_report([], now=NOW)
    assert empty["empty_state"]["is_empty"] is True


def test_db_adapter_tolerates_missing_optional_fields():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE reply_drafts (id TEXT, status TEXT)")
    conn.execute("INSERT INTO reply_drafts VALUES ('d1', 'approved')")
    report = build_reply_draft_approval_outcome_report_from_db(conn, now=NOW)
    assert report["drafts"][0]["draft_id"] == "d1"
    assert report["drafts"][0]["score_bucket"] == "unknown"


def test_cli_json_and_table(monkeypatch, capsys):
    monkeypatch.setattr(script, "script_context", lambda: _script_context(SimpleNamespace()))
    monkeypatch.setattr(
        script,
        "build_reply_draft_approval_outcome_report_from_db",
        lambda _db, **kwargs: build_reply_draft_approval_outcome_report([{"id": "d1", "status": "approved"}], now=NOW, **kwargs),
    )

    assert script.main(["--limit", "1", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "reply_draft_approval_outcome"
    assert script.main(["--table"]) == 0
    assert "draft_id | mention_id" in capsys.readouterr().out
