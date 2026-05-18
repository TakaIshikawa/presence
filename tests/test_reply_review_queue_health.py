"""Tests for reply review queue health reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from engagement.reply_review_queue_health import (
    build_reply_review_queue_health_report,
    build_reply_review_queue_health_report_from_db,
    format_reply_review_queue_health_json,
    format_reply_review_queue_health_text,
)


NOW = datetime(2026, 5, 18, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_review_queue_health.py"
spec = importlib.util.spec_from_file_location("reply_review_queue_health_script", SCRIPT_PATH)
reply_review_queue_health_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(reply_review_queue_health_script)


@contextmanager
def _script_context(conn):
    yield SimpleNamespace(), conn


def _row(draft_id: str, hours_ago: int, score, *, relationship_context: str | None = "prior thread", status: str = "pending") -> dict:
    return {
        "draft_id": draft_id,
        "status": status,
        "drafted_at": (NOW - timedelta(hours=hours_ago)).isoformat(),
        "evaluator_score": score,
        "relationship_context": relationship_context,
        "platform": "x",
    }


def test_groups_pending_drafts_by_age_and_score_band():
    report = build_reply_review_queue_health_report(
        [_row("a", 2, 0.9), _row("b", 30, 0.65), _row("c", 200, None)],
        now=NOW,
    )

    assert report["totals"]["pending_draft_count"] == 3
    assert report["totals"]["age_buckets"]["0-4h"] == 1
    assert report["totals"]["age_buckets"]["1-3d"] == 1
    assert report["totals"]["age_buckets"]["8d+"] == 1
    assert report["totals"]["score_bands"]["high"] == 1
    assert report["totals"]["score_bands"]["medium"] == 1
    assert report["totals"]["score_bands"]["missing"] == 1


def test_flags_missing_relationship_context_when_fields_available():
    report = build_reply_review_queue_health_report(
        [_row("missing", 80, 0.4, relationship_context=""), _row("done", 10, 0.8, relationship_context="known")],
        now=NOW,
    )

    missing = next(row for row in report["risk_rows"] if row["draft_id"] == "missing")

    assert missing["missing_relationship_context"] is True
    assert missing["risk_level"] == "high"
    assert report["totals"]["missing_relationship_context_count"] == 1


def test_ignores_non_pending_and_outside_window_rows():
    report = build_reply_review_queue_health_report(
        [_row("sent", 1, 0.9, status="sent"), _row("old", 900, 0.9), _row("pending", 4, 0.9)],
        days=30,
        now=NOW,
    )

    assert [row["draft_id"] for row in report["risk_rows"]] == ["pending"]
    assert report["totals"]["not_pending"] == 1
    assert report["totals"]["outside_window"] == 1


def test_db_loader_json_text_and_cli(monkeypatch, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE reply_queue (
            id TEXT,
            status TEXT,
            drafted_at TEXT,
            evaluator_score REAL,
            relationship_context TEXT,
            platform TEXT
        )"""
    )
    conn.execute("INSERT INTO reply_queue VALUES (?, ?, ?, ?, ?, ?)", ("r1", "needs_review", (NOW - timedelta(hours=8)).isoformat(), 0.42, "", "x"))
    conn.commit()

    report = build_reply_review_queue_health_report_from_db(conn, now=NOW)

    assert json.loads(format_reply_review_queue_health_json(report))["artifact_type"] == "reply_review_queue_health"
    assert "r1 risk=high" in format_reply_review_queue_health_text(report)
    monkeypatch.setattr(reply_review_queue_health_script, "script_context", lambda: _script_context(conn))
    monkeypatch.setattr(
        reply_review_queue_health_script,
        "build_reply_review_queue_health_report_from_db",
        lambda db, **kwargs: build_reply_review_queue_health_report_from_db(db, now=NOW, **kwargs),
    )
    assert reply_review_queue_health_script.main(["--table"]) == 0
    assert "Reply Review Queue Health" in capsys.readouterr().out
