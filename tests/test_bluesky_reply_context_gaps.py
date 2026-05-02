"""Tests for Bluesky reply context gap auditing."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from engagement.bluesky_reply_context_gaps import (
    audit_bluesky_reply_context_gaps,
    format_bluesky_reply_context_gaps_json,
    format_bluesky_reply_context_gaps_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "audit_bluesky_reply_context.py"
spec = importlib.util.spec_from_file_location("audit_bluesky_reply_context_script", SCRIPT_PATH)
audit_bluesky_reply_context_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(audit_bluesky_reply_context_script)

NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
ROOT_URI = "at://did:plc:me/app.bsky.feed.post/root1"
PARENT_URI = "at://did:plc:bob/app.bsky.feed.post/parent1"
INBOUND_URI = "at://did:plc:alice/app.bsky.feed.post/reply1"


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _complete_metadata(**overrides) -> dict:
    metadata = {
        "reply_root": {"uri": ROOT_URI, "cid": "root-cid"},
        "reply_parent": {"uri": PARENT_URI, "cid": "parent-cid"},
        "root_uri": ROOT_URI,
        "root_cid": "root-cid",
        "root_post_text": "Our original Bluesky post",
        "root_author_handle": "me.bsky.social",
        "parent_uri": PARENT_URI,
        "parent_cid": "parent-cid",
        "parent_post_text": "A parent reply in the branch",
        "parent_author_handle": "bob.bsky.social",
    }
    metadata.update(overrides)
    return metadata


def _insert_reply(db, inbound_id: str, metadata: dict | None = None, **kwargs) -> int:
    defaults = dict(
        inbound_tweet_id=inbound_id,
        inbound_author_handle="alice.bsky.social",
        inbound_author_id="did:plc:alice",
        inbound_text="What about this branch?",
        our_tweet_id=ROOT_URI,
        our_content_id=None,
        our_post_text="Our original Bluesky post",
        draft_text="Draft",
        platform="bluesky",
        inbound_cid="inbound-cid",
        our_platform_id=ROOT_URI,
        platform_metadata=json.dumps(metadata or {}, sort_keys=True),
    )
    defaults.update(kwargs)
    return db.insert_reply_draft(**defaults)


def _set_detected_at(db, reply_id: int, detected_at: str) -> None:
    db.conn.execute(
        "UPDATE reply_queue SET detected_at = ? WHERE id = ?",
        (detected_at, reply_id),
    )
    db.conn.commit()


def test_classifies_bluesky_rows_by_context_gap_type(db):
    ready = _insert_reply(db, INBOUND_URI, _complete_metadata())
    missing_root = _insert_reply(
        db,
        "at://did:plc:alice/app.bsky.feed.post/reply-root",
        {"parent_uri": PARENT_URI, "parent_cid": "parent-cid", "parent_post_text": "Parent"},
        our_platform_id=None,
        our_post_text="",
    )
    missing_parent = _insert_reply(
        db,
        "at://did:plc:alice/app.bsky.feed.post/reply-parent",
        {
            "reply_root": {"uri": ROOT_URI, "cid": "root-cid"},
            "root_post_text": "Our original Bluesky post",
            "root_author_handle": "me.bsky.social",
        },
    )
    missing_author = _insert_reply(
        db,
        "at://did:plc:alice/app.bsky.feed.post/reply-author",
        _complete_metadata(root_author_handle="", parent_author_handle=""),
    )
    stale = _insert_reply(
        db,
        "at://did:plc:alice/app.bsky.feed.post/reply-stale",
        _complete_metadata(reply_root={"uri": "at://did:plc:old/app.bsky.feed.post/root", "cid": "old-cid"}),
    )

    report = audit_bluesky_reply_context_gaps(db, status="all", now=NOW)

    by_id = {item.id: item.classification for item in report.items}
    assert by_id[ready] == "ready"
    assert by_id[missing_root] == "missing_root_context"
    assert by_id[missing_parent] == "missing_parent_context"
    assert by_id[missing_author] == "missing_author_context"
    assert by_id[stale] == "stale_context"
    assert report.by_classification == {
        "missing_author_context": 1,
        "missing_parent_context": 1,
        "missing_root_context": 1,
        "ready": 1,
        "stale_context": 1,
    }
    assert report.gap_count == 4
    assert report.ok is False


def test_reports_status_counts_examples_and_stable_formats(db):
    approved = _insert_reply(
        db,
        "at://did:plc:alice/app.bsky.feed.post/approved",
        {"root_cid": "root-cid"},
        status="approved",
    )
    pending = _insert_reply(
        db,
        "at://did:plc:alice/app.bsky.feed.post/pending",
        _complete_metadata(root_author_handle=""),
        status="pending",
    )

    report = audit_bluesky_reply_context_gaps(db, status="all", now=NOW)
    payload = json.loads(format_bluesky_reply_context_gaps_json(report))
    text = format_bluesky_reply_context_gaps_text(report)

    assert payload["artifact_type"] == "bluesky_reply_context_gaps"
    assert list(payload) == sorted(payload)
    assert payload["by_status"] == {
        "approved": {"missing_parent_context": 1},
        "pending": {"missing_author_context": 1},
    }
    assert payload["representative_reply_ids"] == {
        "missing_author_context": [pending],
        "missing_parent_context": [approved],
    }
    assert "Bluesky Reply Context Gap Audit" in text
    assert "By status: approved(missing_parent_context=1); pending(missing_author_context=1)" in text
    assert f"missing_author_context=[{pending}]" in text


def test_ignores_non_bluesky_reply_rows(db):
    _insert_reply(
        db,
        "x-reply-1",
        _complete_metadata(),
        platform="x",
        our_tweet_id="123",
        our_platform_id="123",
    )

    report = audit_bluesky_reply_context_gaps(db, status="all", now=NOW)

    assert report.audited_count == 0
    assert report.items == ()


def test_status_days_and_limit_filters_are_applied(db):
    old = _insert_reply(db, "at://did:plc:alice/app.bsky.feed.post/old", status="pending")
    approved = _insert_reply(db, "at://did:plc:alice/app.bsky.feed.post/approved", status="approved")
    pending = _insert_reply(db, "at://did:plc:alice/app.bsky.feed.post/new", status="pending")
    _set_detected_at(db, old, "2026-04-01 10:00:00")
    _set_detected_at(db, approved, "2026-05-02 11:00:00")
    _set_detected_at(db, pending, "2026-05-02 10:00:00")

    pending_report = audit_bluesky_reply_context_gaps(db, days=7, status="pending", now=NOW)
    approved_report = audit_bluesky_reply_context_gaps(db, days=7, status="approved", now=NOW)
    all_report = audit_bluesky_reply_context_gaps(db, days=40, status="all", limit=2, now=NOW)

    assert [item.id for item in pending_report.items] == [pending]
    assert [item.id for item in approved_report.items] == [approved]
    assert [item.id for item in all_report.items] == [approved, pending]


def test_cli_supports_json_status_days_and_limit(db, monkeypatch, capsys):
    reply_id = _insert_reply(
        db,
        "at://did:plc:alice/app.bsky.feed.post/cli",
        _complete_metadata(parent_author_handle=""),
        status="approved",
    )
    _set_detected_at(db, reply_id, "2026-05-02 10:00:00")
    monkeypatch.setattr(
        audit_bluesky_reply_context_script,
        "script_context",
        lambda: _script_context(db),
    )

    exit_code = audit_bluesky_reply_context_script.main(
        ["--json", "--status", "approved", "--days", "3", "--limit", "5"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["filters"] == {"days": 3, "limit": 5, "status": "approved"}
    assert payload["items"][0]["classification"] == "missing_author_context"


def test_cli_exits_zero_when_all_bluesky_rows_are_ready(db, monkeypatch, capsys):
    _insert_reply(db, "at://did:plc:alice/app.bsky.feed.post/safe", _complete_metadata())
    monkeypatch.setattr(
        audit_bluesky_reply_context_script,
        "script_context",
        lambda: _script_context(db),
    )

    assert audit_bluesky_reply_context_script.main(["--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    assert payload["gap_count"] == 0
