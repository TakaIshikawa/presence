"""Tests for proactive action target auditing."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from engagement.proactive_action_target_audit import (
    ISSUE_DUPLICATE_TARGET_TWEET_ID,
    ISSUE_MISSING_TARGET_METADATA,
    ISSUE_NEAR_DUPLICATE_TARGET_TEXT,
    ISSUE_POSTED_STATUS_MISMATCH,
    build_proactive_action_target_audit,
    format_proactive_action_target_audit_json,
    format_proactive_action_target_audit_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "audit_proactive_action_targets.py"
spec = importlib.util.spec_from_file_location("audit_proactive_action_targets_script", SCRIPT_PATH)
audit_proactive_action_targets_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(audit_proactive_action_targets_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_action(db, **kwargs) -> int:
    defaults = dict(
        action_type="reply",
        target_tweet_id="target-1",
        target_tweet_text="Useful point about queue retries",
        target_author_handle="alice",
        target_author_id="author-a",
        discovery_source="search",
        relevance_score=0.8,
        draft_text="Good point.",
        relationship_context=None,
        knowledge_ids=None,
        platform_metadata=None,
    )
    defaults.update(kwargs)
    return db.insert_proactive_action(**defaults)


def _set_state(
    db,
    action_id: int,
    *,
    status: str = "pending",
    created_at: str = "2026-05-02T09:00:00+00:00",
    posted_tweet_id: str | None = None,
) -> None:
    db.conn.execute(
        """UPDATE proactive_actions
           SET status = ?, created_at = ?, posted_tweet_id = ?
           WHERE id = ?""",
        (status, created_at, posted_tweet_id, action_id),
    )
    db.conn.commit()


def test_groups_duplicate_pending_actions_by_target_tweet_id(db):
    reply = _insert_action(db, target_tweet_id="same-target", action_type="reply")
    quote = _insert_action(db, target_tweet_id="same-target", action_type="quote_tweet")
    posted = _insert_action(db, target_tweet_id="same-target", action_type="like")
    _set_state(db, posted, status="posted")

    report = build_proactive_action_target_audit(db, now=NOW)
    payload = json.loads(format_proactive_action_target_audit_json(report))
    duplicates = [
        issue
        for issue in payload["issues"]
        if issue["issue_type"] == ISSUE_DUPLICATE_TARGET_TWEET_ID
    ]

    assert report.ok is False
    assert duplicates == [
        {
            "action_ids": [reply, quote],
            "action_types": ["quote_tweet", "reply"],
            "details": "multiple pending proactive actions share target_tweet_id",
            "issue_type": ISSUE_DUPLICATE_TARGET_TWEET_ID,
            "posted_tweet_id": None,
            "recommendation": "dismiss_duplicate",
            "severity": "high",
            "statuses": ["pending"],
            "target_author_handle": None,
            "target_tweet_id": "same-target",
            "target_tweet_text_preview": None,
        }
    ]
    assert posted not in duplicates[0]["action_ids"]


def test_flags_near_duplicate_pending_target_text_for_same_author(db):
    first = _insert_action(
        db,
        target_tweet_id="text-1",
        target_tweet_text="Queue retries need a budget before concurrency changes.",
        target_author_handle="@Alice",
        action_type="reply",
    )
    second = _insert_action(
        db,
        target_tweet_id="text-2",
        target_tweet_text="Queue retries need budget before concurrency changes!",
        target_author_handle="alice",
        action_type="quote_tweet",
    )
    _insert_action(
        db,
        target_tweet_id="text-3",
        target_tweet_text="Completely different release planning thought.",
        target_author_handle="alice",
        action_type="like",
    )

    report = build_proactive_action_target_audit(db, now=NOW)
    near_duplicate = next(
        issue for issue in report.issues if issue.issue_type == ISSUE_NEAR_DUPLICATE_TARGET_TEXT
    )

    assert near_duplicate.action_ids == (first, second)
    assert near_duplicate.target_author_handle == "alice"
    assert near_duplicate.recommendation == "merge"


def test_reports_missing_target_metadata_for_reply_and_quote_and_posted_status_mismatch(db):
    missing_reply = _insert_action(
        db,
        target_tweet_id="",
        target_tweet_text="",
        target_author_handle="",
        action_type="reply",
    )
    missing_quote = _insert_action(
        db,
        target_tweet_id="quote-missing",
        target_tweet_text=None,
        action_type="quote_tweet",
    )
    _insert_action(
        db,
        target_tweet_id="like-missing",
        target_tweet_text="",
        target_author_handle="",
        action_type="like",
    )
    mismatch = _insert_action(
        db,
        target_tweet_id="posted-mismatch",
        action_type="reply",
    )
    _set_state(db, mismatch, status="approved", posted_tweet_id="posted-123")

    report = build_proactive_action_target_audit(db, now=NOW)
    by_type = {}
    for issue in json.loads(format_proactive_action_target_audit_json(report))["issues"]:
        by_type.setdefault(issue["issue_type"], []).append(issue)

    assert [issue["action_ids"][0] for issue in by_type[ISSUE_MISSING_TARGET_METADATA]] == [
        missing_reply,
        missing_quote,
    ]
    assert by_type[ISSUE_MISSING_TARGET_METADATA][0]["recommendation"] == "enrich_target_metadata"
    assert by_type[ISSUE_POSTED_STATUS_MISMATCH][0]["action_ids"] == [mismatch]
    assert by_type[ISSUE_POSTED_STATUS_MISMATCH][0]["recommendation"] == "repair_status"
    assert "target_tweet_id" in by_type[ISSUE_MISSING_TARGET_METADATA][0]["details"]


def test_days_action_type_filters_text_formatter_and_schema_reports(db):
    old = _insert_action(db, target_tweet_id="old", action_type="reply")
    _set_state(db, old, created_at="2026-03-01T09:00:00+00:00", posted_tweet_id="old-post")
    recent = _insert_action(db, target_tweet_id="recent", action_type="reply")
    _set_state(db, recent, status="approved", posted_tweet_id="recent-post")
    like = _insert_action(db, target_tweet_id="like", action_type="like")
    _set_state(db, like, status="approved", posted_tweet_id="like-post")

    report = build_proactive_action_target_audit(
        db,
        days=7,
        action_types=("reply",),
        now=NOW,
    )
    payload = json.loads(format_proactive_action_target_audit_json(report))
    text = format_proactive_action_target_audit_text(report)

    assert payload["filters"] == {"action_type": ["reply"], "days": 7}
    assert payload["audited_count"] == 1
    assert payload["issues"][0]["action_ids"] == [recent]
    assert "Proactive Action Target Audit" in text
    assert "recommendation=repair_status" in text

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    missing = build_proactive_action_target_audit(conn, now=NOW)
    assert missing.ok is True
    assert missing.missing_tables == ("proactive_actions",)

    with pytest.raises(ValueError, match="invalid action_type"):
        build_proactive_action_target_audit(db, action_types=("follow",), now=NOW)


def test_cli_supports_text_json_filters_and_fail_on_issues(db, monkeypatch, capsys):
    action_id = _insert_action(db, target_tweet_id="cli", action_type="reply")
    _set_state(db, action_id, status="approved", posted_tweet_id="posted-cli")
    monkeypatch.setattr(
        audit_proactive_action_targets_script,
        "script_context",
        lambda: _script_context(db),
    )

    ok_exit = audit_proactive_action_targets_script.main(
        ["--format", "json", "--days", "7", "--action-type", "reply"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert ok_exit == 0
    assert payload["artifact_type"] == "proactive_action_target_audit"
    assert payload["issues"][0]["action_ids"] == [action_id]

    fail_exit = audit_proactive_action_targets_script.main(
        ["--format", "text", "--fail-on-issues"]
    )
    text = capsys.readouterr().out
    assert fail_exit == 1
    assert "Proactive Action Target Audit" in text

    invalid = audit_proactive_action_targets_script.main(["--action-type", "follow"])
    captured = capsys.readouterr()
    assert invalid == 2
    assert "action_type must be one of" in captured.err
