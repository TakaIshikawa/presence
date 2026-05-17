"""Tests for reply queue context staleness reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from engagement.reply_queue_context_staleness import (
    build_reply_queue_context_staleness_report,
    format_reply_queue_context_staleness_json,
    format_reply_queue_context_staleness_table,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_queue_context_staleness.py"
spec = importlib.util.spec_from_file_location("reply_queue_context_staleness_script", SCRIPT_PATH)
reply_queue_context_staleness_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(reply_queue_context_staleness_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _reply(db, *, draft_hours: int, context_hours: int | None, source_hours: int | None, status: str = "pending") -> int:
    context = {} if context_hours is None else {"updated_at": (NOW - timedelta(hours=context_hours)).isoformat()}
    metadata = {} if source_hours is None else {"mention_fetched_at": (NOW - timedelta(hours=source_hours)).isoformat()}
    reply_id = db.insert_reply_draft(
        inbound_tweet_id=f"m-{draft_hours}-{context_hours}-{source_hours}-{status}",
        inbound_author_handle="alice",
        inbound_author_id="u",
        inbound_text="?",
        our_tweet_id="our",
        our_content_id=None,
        our_post_text="post",
        draft_text="reply",
        relationship_context=json.dumps(context),
        platform_metadata=json.dumps(metadata),
        status=status,
    )
    db.conn.execute("UPDATE reply_queue SET detected_at = ? WHERE id = ?", ((NOW - timedelta(hours=draft_hours)).isoformat(), reply_id))
    db.conn.commit()
    return int(reply_id)


def test_flags_context_source_and_draft_review_staleness(db):
    fresh = _reply(db, draft_hours=2, context_hours=3, source_hours=4)
    stale = _reply(db, draft_hours=60, context_hours=200, source_hours=90)
    missing = _reply(db, draft_hours=1, context_hours=None, source_hours=None)
    _reply(db, draft_hours=100, context_hours=200, source_hours=200, status="sent")

    report = build_reply_queue_context_staleness_report(
        db,
        stale_context_hours=168,
        stale_source_hours=72,
        draft_review_hours=48,
        now=NOW,
    )
    rows = {row.reply_id: row.to_dict() for row in report.rows}

    assert rows[fresh]["staleness_status"] == "fresh"
    assert rows[stale]["draft_age_hours"] == 60.0
    assert rows[stale]["staleness_status"] == "stale_context,stale_source,stale_draft_review"
    assert rows[missing]["staleness_status"] == "stale_context,stale_source"
    assert len(rows) == 3


def test_json_table_and_cli(db, monkeypatch, capsys):
    reply_id = _reply(db, draft_hours=60, context_hours=3, source_hours=4)
    report = build_reply_queue_context_staleness_report(db, draft_review_hours=48, now=NOW)

    payload = json.loads(format_reply_queue_context_staleness_json(report))
    assert payload["rows"][0]["reply_id"] == reply_id
    assert "Reply Queue Context Staleness" in format_reply_queue_context_staleness_table(report)

    monkeypatch.setattr(reply_queue_context_staleness_script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        reply_queue_context_staleness_script,
        "build_reply_queue_context_staleness_report",
        lambda db, **kwargs: build_reply_queue_context_staleness_report(db, now=NOW, **kwargs),
    )
    assert reply_queue_context_staleness_script.main(["--format", "table", "--draft-review-hours", "24"]) == 0
    assert "reply_id | draft_age_hours" in capsys.readouterr().out
