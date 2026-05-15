"""Tests for reply draft revision drift reporting."""

from __future__ import annotations

from contextlib import contextmanager
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from engagement.reply_draft_revision_drift import (
    build_reply_draft_revision_drift_report,
    build_reply_draft_revision_drift_report_from_db,
    format_reply_draft_revision_drift_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_draft_revision_drift.py"
spec = importlib.util.spec_from_file_location("reply_draft_revision_drift_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_computes_per_reply_drift_metrics():
    report = build_reply_draft_revision_drift_report(
        [
            {
                "id": "r1",
                "draft_text": "This will cut latency by 30%. Thanks!",
                "final_text": "This might reduce latency. Thanks.",
                "author_handle": "@ada",
                "topic": "latency",
            }
        ]
    )

    item = report["replies"][0]
    assert item["edit_distance"] > 0
    assert item["removed_claims"] == ["This will cut latency by 30%."]
    assert item["added_hedging"]["might"] == 1
    assert "!" in item["changed_tone_markers"]


def test_classifies_unchanged_light_substantial_and_rewrite():
    rows = [
        {"id": "unchanged", "draft_text": "Thanks for sharing.", "final_text": "Thanks for sharing."},
        {"id": "light", "draft_text": "Thanks for sharing.", "final_text": "Thanks for sharing!"},
        {"id": "substantial", "draft_text": "We will ship this today because tests pass.", "final_text": "We could ship after another check."},
        {"id": "rewrite", "draft_text": "Short answer.", "final_text": "Completely different response with new framing and details."},
    ]

    report = build_reply_draft_revision_drift_report(rows)
    buckets = {item["reply_id"]: item["drift_bucket"] for item in report["replies"]}

    assert buckets["unchanged"] == "unchanged"
    assert buckets["light"] == "light_edit"
    assert buckets["substantial"] == "substantial_edit"
    assert buckets["rewrite"] == "rewrite"
    assert "bucket" in format_reply_draft_revision_drift_text(report)


def test_aggregates_by_author_and_topic():
    report = build_reply_draft_revision_drift_report(
        [
            {"id": "1", "draft_text": "A", "final_text": "B C D", "author_handle": "@ada", "topic": "ops"},
            {"id": "2", "draft_text": "Same", "final_text": "Same", "author_handle": "@ada", "topic": "ops"},
            {"id": "3", "draft_text": "Old claim 99%.", "final_text": "New reply", "author_handle": "@ben", "topic": "data"},
        ]
    )

    assert report["aggregates"]["by_author_handle"][0]["author_handle"] in {"@ada", "@ben"}
    ops = next(item for item in report["aggregates"]["by_topic"] if item["topic"] == "ops")
    assert ops["count"] == 2


def test_db_loader_and_cli_json_output(monkeypatch, capsys, tmp_path):
    import sqlite3

    conn = sqlite3.connect(tmp_path / "replies.db")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE reply_reviews (
           id INTEGER PRIMARY KEY,
           draft_text TEXT,
           final_text TEXT,
           author_handle TEXT,
           topic TEXT
        )"""
    )
    conn.execute(
        "INSERT INTO reply_reviews (draft_text, final_text, author_handle, topic) VALUES (?, ?, ?, ?)",
        ("We will fix 2 bugs.", "We might fix the bugs.", "@ada", "bugs"),
    )
    conn.commit()

    report = build_reply_draft_revision_drift_report_from_db(conn)
    assert report["replies"][0]["reply_id"] == "1"

    monkeypatch.setattr(script, "script_context", lambda: _script_context(conn))
    monkeypatch.setattr(
        script,
        "build_reply_draft_revision_drift_report_from_db",
        lambda db, **kwargs: build_reply_draft_revision_drift_report_from_db(db, **kwargs),
    )
    assert script.main(["--limit", "5"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "reply_draft_revision_drift"

    assert script.main(["--table"]) == 0
    assert "Reply Draft Revision Drift" in capsys.readouterr().out
