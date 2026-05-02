"""Tests for reply draft relationship context staleness reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from engagement.reply_stale_context_report import (
    build_reply_stale_context_report,
    format_reply_stale_context_json,
    inspect_reply_stale_context,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "report_reply_stale_context.py"
spec = importlib.util.spec_from_file_location("report_reply_stale_context_script", SCRIPT_PATH)
report_reply_stale_context_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(report_reply_stale_context_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_reply(db, inbound_id: str, **kwargs) -> int:
    defaults = dict(
        inbound_tweet_id=inbound_id,
        inbound_author_handle="alice",
        inbound_author_id="user-a",
        inbound_text="Nice post",
        our_tweet_id="our-1",
        our_content_id=None,
        our_post_text="Original post",
        draft_text="Thanks for sharing this.",
        status="pending",
        platform="x",
    )
    defaults.update(kwargs)
    return db.insert_reply_draft(**defaults)


def _ensure_context_columns(db) -> None:
    cols = {row[1] for row in db.conn.execute("PRAGMA table_info(reply_queue)")}
    for column in ("context_updated_at", "relationship_context_updated_at"):
        if column not in cols:
            db.conn.execute(f"ALTER TABLE reply_queue ADD COLUMN {column} TEXT")
    db.conn.commit()


def _set_fields(db, reply_id: int, **fields) -> None:
    assignments = ", ".join(f"{field} = ?" for field in fields)
    db.conn.execute(
        f"UPDATE reply_queue SET {assignments} WHERE id = ?",
        (*fields.values(), reply_id),
    )
    db.conn.commit()


def test_classifies_fresh_stale_missing_and_malformed_context_timestamps():
    rows = [
        {
            "id": 1,
            "inbound_tweet_id": "fresh",
            "platform": "x",
            "relationship_context_updated_at": "2026-04-25T12:00:00+00:00",
        },
        {
            "id": 2,
            "inbound_tweet_id": "stale",
            "platform": "x",
            "relationship_context_updated_at": "2026-04-20T11:59:59+00:00",
        },
        {"id": 3, "inbound_tweet_id": "missing", "platform": "x"},
        {
            "id": 4,
            "inbound_tweet_id": "bad",
            "platform": "x",
            "relationship_context_updated_at": "not-a-date",
        },
    ]

    report = build_reply_stale_context_report(rows, max_age_days=10, now=NOW)
    by_mention = {item["mention_id"]: item for item in report["findings"]}

    assert report["counts"] == {
        "rows_scanned": 4,
        "fresh": 1,
        "stale": 1,
        "missing": 1,
        "malformed": 1,
    }
    assert by_mention["fresh"]["context_status"] == "fresh"
    assert by_mention["fresh"]["age_days"] == 6.0
    assert by_mention["fresh"]["severity"] == "info"
    assert by_mention["fresh"]["recommended_action"] == "review"
    assert by_mention["stale"]["context_status"] == "stale"
    assert by_mention["stale"]["age_days"] == 11.0
    assert by_mention["stale"]["severity"] == "medium"
    assert by_mention["stale"]["recommended_action"] == "refresh_context"
    assert by_mention["missing"]["context_status"] == "missing"
    assert by_mention["missing"]["age_days"] is None
    assert by_mention["missing"]["recommended_action"] == "refresh_context"
    assert by_mention["bad"]["context_status"] == "malformed"
    assert by_mention["bad"]["recommended_action"] == "repair_context_timestamp"


def test_stale_threshold_boundary_is_fresh_until_timestamp_is_older_than_max_age():
    exact = inspect_reply_stale_context(
        {
            "id": 1,
            "inbound_tweet_id": "exact",
            "context_updated_at": "2026-04-21T12:00:00+00:00",
        },
        max_age_days=10,
        now=NOW,
    )
    older = inspect_reply_stale_context(
        {
            "id": 2,
            "inbound_tweet_id": "older",
            "context_updated_at": "2026-04-21T11:59:59+00:00",
        },
        max_age_days=10,
        now=NOW,
    )

    assert exact["age_days"] == 10.0
    assert exact["context_status"] == "fresh"
    assert older["age_days"] == 10.0
    assert older["context_status"] == "stale"


def test_missing_timestamp_behavior_uses_high_severity_refresh_action():
    finding = inspect_reply_stale_context(
        {"draft_id": "12", "mention_id": "mention-12", "platform": "bluesky"},
        max_age_days=7,
        now=NOW,
    )

    assert finding["draft_id"] == 12
    assert finding["mention_id"] == "mention-12"
    assert finding["context_timestamp_field"] is None
    assert finding["context_status"] == "missing"
    assert finding["age_days"] is None
    assert finding["severity"] == "high"
    assert finding["recommended_action"] == "refresh_context"


def test_json_formatter_is_deterministic():
    report = build_reply_stale_context_report(
        [{"id": 1, "inbound_tweet_id": "one", "context_updated_at": "bad"}],
        now=NOW,
    )
    payload = json.loads(format_reply_stale_context_json(report))

    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["artifact_type"] == "reply_stale_context_report"
    assert payload["findings"][0]["context_status"] == "malformed"


def test_script_loads_pending_reply_drafts_and_emits_json(db, monkeypatch, capsys):
    _ensure_context_columns(db)
    pending_id = _insert_reply(db, "pending")
    reviewed_id = _insert_reply(db, "reviewed", status="reviewed")
    _set_fields(
        db,
        pending_id,
        relationship_context_updated_at="2026-04-20T12:00:00+00:00",
    )
    _set_fields(
        db,
        reviewed_id,
        relationship_context_updated_at="2026-04-20T12:00:00+00:00",
    )
    monkeypatch.setattr(
        report_reply_stale_context_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        report_reply_stale_context_script,
        "build_reply_stale_context_report",
        lambda rows, **kwargs: build_reply_stale_context_report(rows, now=NOW, **kwargs),
    )

    exit_code = report_reply_stale_context_script.main(["--max-age-days", "10"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["filters"]["max_age_days"] == 10
    assert payload["counts"]["rows_scanned"] == 1
    assert payload["findings"][0]["draft_id"] == pending_id
    assert payload["findings"][0]["mention_id"] == "pending"
    assert payload["findings"][0]["context_status"] == "stale"


def test_script_handles_missing_reply_queue_schema(capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    rows = report_reply_stale_context_script.list_pending_reply_drafts(conn)

    assert rows == []
