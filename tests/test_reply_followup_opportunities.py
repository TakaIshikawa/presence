"""Tests for reply follow-up opportunities reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from engagement.reply_followup_opportunities import (
    build_reply_followup_opportunities_report,
    format_reply_followup_opportunities_json,
    format_reply_followup_opportunities_text,
)


NOW = datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_followup_opportunities.py"
spec = importlib.util.spec_from_file_location("reply_followup_opportunities_script", SCRIPT_PATH)
reply_followup_opportunities_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(reply_followup_opportunities_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _reply(db, inbound_id: str, *, hours_ago: int, intent: str = "question", context=None) -> int:
    reply_id = db.insert_reply_draft(
        inbound_tweet_id=inbound_id,
        inbound_author_handle="ada",
        inbound_author_id="1",
        inbound_text="Can you say more?",
        our_tweet_id="our-1",
        our_content_id=None,
        our_post_text="post",
        draft_text="reply",
        intent=intent,
        priority="normal",
        relationship_context=json.dumps(context or {}),
        status="posted",
    )
    stamp = (NOW - timedelta(hours=hours_ago)).isoformat()
    db.conn.execute(
        "UPDATE reply_queue SET detected_at = ?, posted_at = ? WHERE id = ?",
        (stamp, stamp, reply_id),
    )
    db.conn.commit()
    return reply_id


def test_stale_replies_are_ranked_with_reason_codes(db):
    reply_id = _reply(db, "m1", hours_ago=80)

    report = build_reply_followup_opportunities_report(db, now=NOW)

    assert report.opportunities[0].reply_id == reply_id
    assert "stale_inbound_response" in report.opportunities[0].reason_codes
    assert "unresolved_intent" in report.opportunities[0].reason_codes


def test_recently_handled_replies_do_not_pass_priority_filter(db):
    _reply(db, "m1", hours_ago=2, intent="appreciation")

    report = build_reply_followup_opportunities_report(db, min_priority=1, now=NOW)

    assert report.opportunities == ()
    assert "No reply follow-up opportunities" in format_reply_followup_opportunities_text(report)


def test_duplicate_intents_keep_highest_priority_candidate(db):
    old = _reply(db, "m1", hours_ago=50, intent="question")
    newer = _reply(db, "m2", hours_ago=100, intent="question")

    report = build_reply_followup_opportunities_report(db, now=NOW)

    assert [item.reply_id for item in report.opportunities] == [newer]
    assert old != newer


def test_relationship_context_scoring_and_cli_json(db, monkeypatch, capsys):
    _reply(db, "m1", hours_ago=10, intent="other", context={"stage": "champion", "strength": 0.9})
    monkeypatch.setattr(reply_followup_opportunities_script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        reply_followup_opportunities_script,
        "build_reply_followup_opportunities_report",
        lambda db, **kwargs: build_reply_followup_opportunities_report(db, now=NOW, **kwargs),
    )

    report = build_reply_followup_opportunities_report(db, now=NOW)
    payload = json.loads(format_reply_followup_opportunities_json(report))
    exit_code = reply_followup_opportunities_script.main(["--format", "json", "--min-priority", "1"])
    cli_payload = json.loads(capsys.readouterr().out)

    assert "high_value_relationship" in payload["opportunities"][0]["reason_codes"]
    assert cli_payload["opportunity_count"] == 1
    assert exit_code == 0
