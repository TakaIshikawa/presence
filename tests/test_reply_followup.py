"""Tests for reply follow-up reminder candidate generation and CLI."""

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_followup import (  # noqa: E402
    ReplyFollowupPolicy,
    create_reply_followup_reminders,
    select_reply_followup_candidates,
)
from reply_followups import main  # noqa: E402


NOW = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)


def _insert_reply(db, tweet_id: str, handle: str, **kwargs) -> int:
    defaults = dict(
        inbound_tweet_id=tweet_id,
        inbound_author_handle=handle,
        inbound_author_id=f"{handle}_id",
        inbound_text="This was thoughtful",
        our_tweet_id="our-1",
        our_content_id=None,
        our_post_text="Original post",
        draft_text="Thanks, this is useful context.",
        status="posted",
        quality_score=8.2,
        quality_flags=json.dumps(["clean"]),
        priority="normal",
        relationship_context=json.dumps(
            {
                "is_known": True,
                "engagement_stage": 3,
                "stage_name": "Active",
                "dunbar_tier": 2,
                "tier_name": "Warm",
                "relationship_strength": 0.7,
            }
        ),
    )
    defaults.update(kwargs)
    reply_id = db.insert_reply_draft(**defaults)
    db.conn.execute(
        "UPDATE reply_queue SET posted_at = ?, reviewed_at = ? WHERE id = ?",
        ("2026-04-22T12:00:00+00:00", "2026-04-22T12:00:00+00:00", reply_id),
    )
    db.conn.commit()
    return reply_id


def _mock_script_context(config, db):
    @contextmanager
    def _ctx():
        yield (config, db)

    return _ctx


def test_select_candidates_requires_quality_relationship_and_cooldown(db):
    good_id = _insert_reply(db, "good", "alice")
    _insert_reply(db, "low-quality", "bob", quality_score=5.0)
    _insert_reply(db, "dismissed", "carol", status="dismissed")
    _insert_reply(
        db,
        "no-relationship",
        "dave",
        relationship_context=json.dumps({"is_known": False, "engagement_stage": 0}),
    )
    _insert_reply(db, "recent", "erin")
    db.insert_reply_followup_reminder(
        target_handle="erin",
        source_type="reply_queue",
        source_id=99,
        due_at="2026-04-28T12:00:00+00:00",
        reason="Already queued",
    )

    candidates = select_reply_followup_candidates(
        db,
        policy=ReplyFollowupPolicy(limit=10),
        now=NOW,
    )

    assert [(c.target_handle, c.source_type, c.source_id) for c in candidates] == [
        ("alice", "reply_queue", good_id)
    ]
    assert candidates[0].due_at == "2026-04-30T12:00:00+00:00"
    assert "High-value posted reply" in candidates[0].reason


def test_create_reminders_suppresses_duplicate_source(db):
    reply_id = _insert_reply(db, "good", "alice")
    db.insert_reply_followup_reminder(
        target_handle="alice",
        source_type="reply_queue",
        source_id=reply_id,
        due_at="2026-04-30T12:00:00+00:00",
        reason="Existing",
    )

    rows = create_reply_followup_reminders(db, now=NOW)

    assert rows == []


def test_proactive_reply_candidate_uses_relevance_score(db):
    action_id = db.insert_proactive_action(
        action_type="reply",
        target_tweet_id="tw-1",
        target_tweet_text="Interesting thread",
        target_author_handle="maya",
        relevance_score=0.82,
        draft_text="Sharp point.",
        relationship_context=json.dumps(
            {"is_known": True, "engagement_stage": 2, "dunbar_tier": 3}
        ),
    )
    db.conn.execute(
        """UPDATE proactive_actions
           SET status = 'approved', reviewed_at = ?
           WHERE id = ?""",
        ("2026-04-22T12:00:00+00:00", action_id),
    )
    db.conn.commit()

    candidates = select_reply_followup_candidates(db, now=NOW)

    assert len(candidates) == 1
    assert candidates[0].source_type == "proactive_actions"
    assert candidates[0].source_id == action_id


def test_script_dry_run_json_emits_candidates_without_writes(capsys):
    config = SimpleNamespace()
    db = MagicMock()
    candidate = MagicMock()
    candidate.to_dict.return_value = {
        "target_handle": "alice",
        "source_type": "reply_queue",
        "source_id": 1,
        "due_at": "2026-04-30T12:00:00+00:00",
        "reason": "High-value reply",
        "notes": None,
    }

    with (
        patch("reply_followups.script_context", _mock_script_context(config, db)),
        patch("reply_followups.select_reply_followup_candidates", return_value=[candidate]),
    ):
        assert main(["--dry-run", "--json"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["action"] == "dry_run"
    assert payload["reminders"][0]["target_handle"] == "alice"
    db.insert_reply_followup_reminder.assert_not_called()


def test_script_mark_done_and_dismiss_modes():
    config = SimpleNamespace()
    db = MagicMock()
    db.mark_reply_followup_done.return_value = True
    db.dismiss_reply_followup.return_value = True

    with patch("reply_followups.script_context", _mock_script_context(config, db)):
        assert main(["--mark-done", "10", "--notes", "sent a note"]) == 0
        assert main(["--dismiss", "11"]) == 0

    db.mark_reply_followup_done.assert_called_once_with(10, notes="sent a note")
    db.dismiss_reply_followup.assert_called_once_with(11, notes=None)
