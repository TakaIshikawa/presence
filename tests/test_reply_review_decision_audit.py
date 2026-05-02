"""Tests for reply review decision trail audits."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from engagement.reply_review_decision_audit import (
    build_reply_review_decision_audit,
    format_reply_review_decision_audit_json,
    format_reply_review_decision_audit_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "reply_review_decision_audit.py"
)
spec = importlib.util.spec_from_file_location("reply_review_decision_audit_script", SCRIPT_PATH)
reply_review_decision_audit_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(reply_review_decision_audit_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_reply(db, tweet_id: str, *, status: str, handle: str = "alice") -> int:
    return db.insert_reply_draft(
        inbound_tweet_id=tweet_id,
        inbound_author_handle=handle,
        inbound_author_id=f"user-{tweet_id}",
        inbound_text="Can you help?",
        our_tweet_id="our-1",
        our_content_id=None,
        our_post_text="Original post",
        draft_text="Thanks",
        status=status,
    )


def _set_times(db, reply_id: int, **values: str) -> None:
    assignments = ", ".join(f"{column} = ?" for column in values)
    db.conn.execute(
        f"UPDATE reply_queue SET {assignments} WHERE id = ?",
        (*values.values(), reply_id),
    )
    db.conn.commit()


def test_flags_status_mismatches_missing_decision_events_and_invalid_trails(db):
    valid = _insert_reply(db, "valid", status="posted")
    mismatch = _insert_reply(db, "mismatch", status="approved")
    posted_without_event = _insert_reply(db, "posted-missing", status="posted")
    approved_without_event = _insert_reply(db, "approved-missing", status="approved")
    failed_on_posted = _insert_reply(db, "failed", status="posted")
    skipped = _insert_reply(db, "skipped", status="posted")
    broken_chain = _insert_reply(db, "broken-chain", status="posted")
    for reply_id in (
        valid,
        mismatch,
        posted_without_event,
        approved_without_event,
        failed_on_posted,
        skipped,
        broken_chain,
    ):
        _set_times(
            db,
            reply_id,
            detected_at="2026-05-01T09:00:00+00:00",
            reviewed_at="2026-05-01T10:00:00+00:00",
            posted_at="2026-05-01T11:00:00+00:00",
        )

    db.record_reply_review_event(
        valid,
        "approved",
        old_status="pending",
        new_status="approved",
        created_at="2026-05-01T10:00:00+00:00",
    )
    db.record_reply_review_event(
        valid,
        "posted",
        old_status="approved",
        new_status="posted",
        created_at="2026-05-01T11:00:00+00:00",
    )
    db.record_reply_review_event(
        mismatch,
        "rejected",
        old_status="pending",
        new_status="dismissed",
        created_at="2026-05-01T10:00:00+00:00",
    )
    db.record_reply_review_event(
        posted_without_event,
        "approved",
        old_status="pending",
        new_status="approved",
        created_at="2026-05-01T10:00:00+00:00",
    )
    db.record_reply_review_event(
        failed_on_posted,
        "approved",
        old_status="pending",
        new_status="approved",
        created_at="2026-05-01T10:00:00+00:00",
    )
    db.record_reply_review_event(
        failed_on_posted,
        "failed",
        old_status="approved",
        new_status="approved",
        created_at="2026-05-01T11:00:00+00:00",
    )
    db.record_reply_review_event(
        skipped,
        "posted",
        old_status="pending",
        new_status="posted",
        created_at="2026-05-01T11:00:00+00:00",
    )
    db.record_reply_review_event(
        broken_chain,
        "approved",
        old_status="pending",
        new_status="approved",
        created_at="2026-05-01T10:00:00+00:00",
    )
    db.record_reply_review_event(
        broken_chain,
        "posted",
        old_status="pending",
        new_status="posted",
        created_at="2026-05-01T11:00:00+00:00",
    )

    report = build_reply_review_decision_audit(db, days=7, limit=50, now=NOW)
    payload = json.loads(format_reply_review_decision_audit_json(report))

    assert valid not in {finding["reply_queue_id"] for finding in payload["findings"]}
    assert payload["totals"]["issue_totals"]["latest_event_status_mismatch"] == 3
    assert payload["totals"]["issue_totals"]["posted_without_posted_event"] == 2
    assert payload["totals"]["issue_totals"]["approved_without_approved_event"] == 3
    assert payload["totals"]["issue_totals"]["failed_event_on_non_pending_reply"] == 1
    assert payload["totals"]["issue_totals"]["skipped_approval_transition"] == 2
    assert payload["totals"]["issue_totals"]["event_old_status_chain_mismatch"] == 1
    assert any(
        finding["reply_queue_id"] == mismatch
        and finding["latest_event"]["new_status"] == "dismissed"
        and finding["current_status"] == "approved"
        for finding in payload["findings"]
    )
    assert all("suggested_action" in finding for finding in payload["findings"])


def test_limit_caps_representative_findings_but_not_issue_totals(db):
    for index in range(3):
        reply_id = _insert_reply(db, f"approved-{index}", status="approved")
        _set_times(
            db,
            reply_id,
            detected_at="2026-05-01T09:00:00+00:00",
            reviewed_at="2026-05-01T10:00:00+00:00",
        )

    report = build_reply_review_decision_audit(db, days=7, limit=2, now=NOW)

    assert report["totals"]["issue_count"] == 3
    assert report["totals"]["finding_count"] == 2
    assert report["totals"]["issue_totals"] == {"approved_without_approved_event": 3}
    assert len(report["findings"]) == 2


def test_missing_reply_review_events_is_reported_without_crashing():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            inbound_tweet_id TEXT,
            inbound_author_handle TEXT,
            status TEXT,
            detected_at TEXT,
            reviewed_at TEXT,
            posted_at TEXT
        );
        INSERT INTO reply_queue
            (id, inbound_tweet_id, inbound_author_handle, status, detected_at, reviewed_at)
        VALUES
            (1, 'inbound-1', 'alice', 'approved', '2026-05-01T09:00:00+00:00',
             '2026-05-01T10:00:00+00:00');
        """
    )

    report = build_reply_review_decision_audit(conn, days=7, limit=10, now=NOW)
    text = format_reply_review_decision_audit_text(report)

    assert report["missing_tables"] == ["reply_review_events"]
    assert report["totals"]["issue_totals"] == {"approved_without_approved_event": 1}
    assert "Missing tables: reply_review_events" in text
    assert "reply_queue:1 @alice" in text


def test_missing_reply_queue_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_reply_review_decision_audit(conn, now=NOW)

    assert report["missing_tables"] == ["reply_queue", "reply_review_events"]
    assert report["totals"]["reply_count"] == 0
    assert report["findings"] == []


def test_invalid_arguments_raise_value_error(db):
    with pytest.raises(ValueError, match="days must be at least 1"):
        build_reply_review_decision_audit(db, days=0, now=NOW)
    with pytest.raises(ValueError, match="limit must be at least 1"):
        build_reply_review_decision_audit(db, limit=0, now=NOW)


def test_cli_outputs_json(db, monkeypatch, capsys):
    reply_id = _insert_reply(db, "cli", status="approved")
    _set_times(
        db,
        reply_id,
        detected_at="2026-05-01T09:00:00+00:00",
        reviewed_at="2026-05-01T10:00:00+00:00",
    )
    monkeypatch.setattr(
        reply_review_decision_audit_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        reply_review_decision_audit_script,
        "build_reply_review_decision_audit",
        lambda db, **kwargs: build_reply_review_decision_audit(db, now=NOW, **kwargs),
    )

    exit_code = reply_review_decision_audit_script.main(
        ["--days", "7", "--limit", "5", "--format", "json"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["artifact_type"] == "reply_review_decision_audit"
    assert payload["filters"]["limit"] == 5
    assert payload["findings"][0]["reply_queue_id"] == reply_id
