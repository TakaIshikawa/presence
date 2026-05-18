"""Tests for reply draft context staleness reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from engagement.reply_context_staleness import (
    build_reply_context_staleness_report,
    build_reply_context_staleness_report_from_db,
    format_reply_context_staleness_table,
)


NOW = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_context_staleness.py"
spec = importlib.util.spec_from_file_location("reply_context_staleness_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_reply(db, inbound_id: str, *, created_at: datetime, status: str = "pending", context_at: datetime | None = None) -> int:
    reply_id = db.insert_reply_draft(
        inbound_tweet_id=inbound_id,
        inbound_author_handle="alice",
        inbound_author_id="user-a",
        inbound_text="Nice post",
        our_tweet_id="our-1",
        our_content_id=None,
        our_post_text="Original post",
        draft_text="Thanks.",
        status=status,
        platform="x",
    )
    _ensure_columns(db)
    db.conn.execute("UPDATE reply_queue SET created_at = ? WHERE id = ?", (created_at.isoformat(), reply_id))
    if context_at is not None:
        db.conn.execute(
            "UPDATE reply_queue SET relationship_context_updated_at = ? WHERE id = ?",
            (context_at.isoformat(), reply_id),
        )
    db.conn.commit()
    return reply_id


def _ensure_columns(db) -> None:
    cols = {row[1] for row in db.conn.execute("PRAGMA table_info(reply_queue)")}
    if "created_at" not in cols:
        db.conn.execute("ALTER TABLE reply_queue ADD COLUMN created_at TEXT")
    if "relationship_context_updated_at" not in cols:
        db.conn.execute("ALTER TABLE reply_queue ADD COLUMN relationship_context_updated_at TEXT")
    db.conn.commit()


def test_report_scores_missing_stale_old_and_fresh_context():
    rows = [
        {
            "id": 1,
            "inbound_tweet_id": "missing",
            "created_at": NOW.isoformat(),
        },
        {
            "id": 2,
            "inbound_tweet_id": "stale",
            "created_at": NOW.isoformat(),
            "relationship_context_updated_at": (NOW - timedelta(hours=80)).isoformat(),
        },
        {
            "id": 3,
            "inbound_tweet_id": "old",
            "created_at": NOW.isoformat(),
            "relationship_context_updated_at": (NOW - timedelta(hours=30)).isoformat(),
        },
        {
            "id": 4,
            "inbound_tweet_id": "fresh",
            "created_at": NOW.isoformat(),
            "relationship_context_updated_at": (NOW - timedelta(hours=3)).isoformat(),
        },
    ]

    report = build_reply_context_staleness_report(rows, old_hours=24, stale_hours=72, now=NOW)
    by_mention = {row["mention_id"]: row for row in report["rows"]}

    assert report["summary"] == {
        "draft_count": 4,
        "missing_context_count": 1,
        "stale_context_count": 1,
        "old_context_count": 1,
        "fresh_context_count": 1,
    }
    assert by_mention["missing"]["context_timestamp"] is None
    assert by_mention["missing"]["context_age_hours"] is None
    assert by_mention["missing"]["age_bucket"] == "missing"
    assert by_mention["missing"]["risk_label"] == "missing_context"
    assert by_mention["stale"]["context_age_hours"] == 80.0
    assert by_mention["stale"]["age_bucket"] == "stale"
    assert by_mention["stale"]["risk_label"] == "stale_context"
    assert by_mention["old"]["age_bucket"] == "old"
    assert by_mention["old"]["risk_label"] == "old_context"
    assert by_mention["fresh"]["age_bucket"] == "fresh"
    assert by_mention["fresh"]["risk_label"] == "fresh_context"


def test_db_loader_and_cli_support_json_and_table(db, monkeypatch, capsys):
    stale_id = _insert_reply(
        db,
        "stale",
        created_at=NOW,
        context_at=NOW - timedelta(hours=90),
    )
    _insert_reply(
        db,
        "reviewed",
        created_at=NOW,
        status="reviewed",
        context_at=NOW - timedelta(hours=90),
    )

    report = build_reply_context_staleness_report_from_db(db, old_hours=24, stale_hours=72, now=NOW)
    assert [row["draft_id"] for row in report["rows"]] == [stale_id]
    assert report["rows"][0]["risk_label"] == "stale_context"
    assert "Reply Context Staleness" in format_reply_context_staleness_table(report)

    monkeypatch.setattr(script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        script,
        "build_reply_context_staleness_report_from_db",
        lambda db, **kwargs: build_reply_context_staleness_report_from_db(db, now=NOW, **kwargs),
    )

    assert script.main(["--old-hours", "24", "--stale-hours", "72"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "reply_context_staleness"
    assert payload["rows"][0]["draft_id"] == stale_id

    assert script.main(["--table"]) == 0
    assert "stale_context" in capsys.readouterr().out
