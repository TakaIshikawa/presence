"""Tests for proactive draft refresh planning."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from engagement.proactive_draft_refresh import (
    REASON_MISSING_KNOWLEDGE_IDS,
    REASON_MISSING_RELATIONSHIP_CONTEXT,
    REASON_MISSING_TARGET_TWEET_TEXT,
    REASON_STALE_DRAFT_TEXT,
    build_proactive_draft_refresh_report,
    format_proactive_draft_refresh_json,
    format_proactive_draft_refresh_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "plan_proactive_draft_refresh.py"
)
spec = importlib.util.spec_from_file_location("plan_proactive_draft_refresh_script", SCRIPT_PATH)
plan_proactive_draft_refresh_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(plan_proactive_draft_refresh_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_action(db, **kwargs) -> int:
    defaults = dict(
        action_type="reply",
        target_tweet_id="target-1",
        target_tweet_text="Useful thought",
        target_author_handle="alice",
        target_author_id="author-a",
        discovery_source="search",
        relevance_score=0.8,
        draft_text="Good point.",
        relationship_context=json.dumps({"profile_summary": "Builder"}, sort_keys=True),
        knowledge_ids=json.dumps([[123, 0.91]], sort_keys=True),
    )
    defaults.update(kwargs)
    return db.insert_proactive_action(**defaults)


def _set_action_state(
    db,
    action_id: int,
    *,
    status: str = "pending",
    created_at: str,
    reviewed_at: str | None = None,
) -> None:
    db.conn.execute(
        """UPDATE proactive_actions
           SET status = ?, created_at = ?, reviewed_at = ?
           WHERE id = ?""",
        (status, created_at, reviewed_at, action_id),
    )
    db.conn.commit()


def test_returns_pending_and_approved_stale_drafts_with_normalized_reasons(db):
    pending = _insert_action(db, target_tweet_id="pending-stale")
    _set_action_state(db, pending, created_at="2026-04-20T12:00:00+00:00")
    approved = _insert_action(db, target_tweet_id="approved-stale")
    _set_action_state(
        db,
        approved,
        status="approved",
        created_at="2026-04-15T12:00:00+00:00",
        reviewed_at="2026-04-21T12:00:00+00:00",
    )
    fresh = _insert_action(db, target_tweet_id="fresh")
    _set_action_state(db, fresh, created_at="2026-05-01T12:00:00+00:00")
    posted = _insert_action(db, target_tweet_id="posted")
    _set_action_state(db, posted, status="posted", created_at="2026-04-20T12:00:00+00:00")

    report = build_proactive_draft_refresh_report(db, stale_days=7, now=NOW, limit=10)
    payload = json.loads(format_proactive_draft_refresh_json(report))
    by_id = {item["id"]: item for item in payload["actions"]}

    assert [item["id"] for item in payload["actions"]] == [pending, approved]
    assert by_id[pending]["refresh_reasons"] == [REASON_STALE_DRAFT_TEXT]
    assert by_id[pending]["recommendation"] == "refresh_draft"
    assert by_id[approved]["age_anchor"] == "2026-04-21T12:00:00+00:00"
    assert payload["summary"]["by_recommendation"] == {"refresh_draft": 2}
    assert fresh not in by_id
    assert posted not in by_id


def test_no_draft_rows_are_included_only_when_context_gaps_are_actionable(db):
    no_draft_with_gaps = _insert_action(
        db,
        target_tweet_id="no-draft-gap",
        target_tweet_text="",
        draft_text=None,
        relationship_context=None,
        knowledge_ids=None,
    )
    _set_action_state(db, no_draft_with_gaps, created_at="2026-05-02T08:00:00+00:00")
    no_draft_complete = _insert_action(
        db,
        target_tweet_id="no-draft-complete",
        draft_text=None,
    )
    _set_action_state(db, no_draft_complete, created_at="2026-04-20T12:00:00+00:00")
    draft_with_context_gaps = _insert_action(
        db,
        target_tweet_id="draft-gap",
        relationship_context="",
        knowledge_ids="[]",
    )
    _set_action_state(db, draft_with_context_gaps, created_at="2026-05-02T08:00:00+00:00")

    report = build_proactive_draft_refresh_report(db, stale_days=7, now=NOW)
    by_id = {item.id: item for item in report.actions}

    assert set(by_id) == {no_draft_with_gaps, draft_with_context_gaps}
    assert by_id[no_draft_with_gaps].refresh_reasons == (
        REASON_MISSING_TARGET_TWEET_TEXT,
        REASON_MISSING_RELATIONSHIP_CONTEXT,
        REASON_MISSING_KNOWLEDGE_IDS,
    )
    assert by_id[no_draft_with_gaps].recommendation == "enrich_context_before_review"
    assert by_id[draft_with_context_gaps].refresh_reasons == (
        REASON_MISSING_RELATIONSHIP_CONTEXT,
        REASON_MISSING_KNOWLEDGE_IDS,
    )
    assert no_draft_complete not in by_id


def test_status_action_type_limit_and_invalid_filters(db):
    reply = _insert_action(db, target_tweet_id="reply")
    _set_action_state(db, reply, created_at="2026-04-20T12:00:00+00:00")
    quote = _insert_action(db, target_tweet_id="quote", action_type="quote_tweet")
    _set_action_state(db, quote, status="approved", created_at="2026-04-19T12:00:00+00:00")
    like = _insert_action(db, target_tweet_id="like", action_type="like")
    _set_action_state(db, like, created_at="2026-04-18T12:00:00+00:00")

    report = build_proactive_draft_refresh_report(
        db,
        stale_days=7,
        statuses=("approved",),
        action_types=("quote_tweet", "reply"),
        limit=1,
        now=NOW,
    )
    payload = json.loads(format_proactive_draft_refresh_json(report))

    assert [item["id"] for item in payload["actions"]] == [quote]
    assert payload["filters"] == {
        "action_type": ["quote_tweet", "reply"],
        "limit": 1,
        "stale_days": 7,
        "status": ["approved"],
    }
    with pytest.raises(ValueError, match="invalid status"):
        build_proactive_draft_refresh_report(db, statuses=("posted",), now=NOW)
    with pytest.raises(ValueError, match="invalid action_type"):
        build_proactive_draft_refresh_report(db, action_types=("follow",), now=NOW)


def test_text_formatter_and_missing_schema_reports_are_stable(db):
    action_id = _insert_action(db, target_tweet_id="text")
    _set_action_state(db, action_id, created_at="2026-04-20T12:00:00+00:00")

    report = build_proactive_draft_refresh_report(db, now=NOW)
    text = format_proactive_draft_refresh_text(report)

    assert "Proactive Draft Refresh Plan" in text
    assert f"#{action_id} reply @alice" in text
    assert "recommendation=refresh_draft" in text
    assert "Recommendations:" in text

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    missing = build_proactive_draft_refresh_report(conn, now=NOW)
    assert missing.total_actions == 0
    assert missing.missing_tables == ("proactive_actions",)


def test_cli_supports_text_json_filters_and_validation(db, monkeypatch, capsys):
    action_id = _insert_action(db, target_tweet_id="cli", action_type="reply")
    _set_action_state(db, action_id, created_at="2026-04-20T12:00:00+00:00")
    monkeypatch.setattr(
        plan_proactive_draft_refresh_script,
        "script_context",
        lambda: _script_context(db),
    )

    exit_code = plan_proactive_draft_refresh_script.main(
        [
            "--format",
            "json",
            "--status",
            "pending",
            "--action-type",
            "reply",
            "--stale-days",
            "7",
            "--limit",
            "1",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["actions"][0]["id"] == action_id
    assert payload["summary"]["by_recommendation"] == {"refresh_draft": 1}

    text_exit = plan_proactive_draft_refresh_script.main(["--format", "text"])
    text = capsys.readouterr().out
    assert text_exit == 0
    assert "Proactive Draft Refresh Plan" in text

    invalid = plan_proactive_draft_refresh_script.main(["--status", "posted"])
    captured = capsys.readouterr()
    assert invalid == 2
    assert "status must be one of" in captured.err
