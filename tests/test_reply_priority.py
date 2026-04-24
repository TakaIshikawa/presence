"""Tests for deterministic reply priority scoring."""

import json
from datetime import datetime, timezone

from engagement.reply_priority import prioritize_replies, score_reply_priority


NOW = datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc)


def _reply(**overrides):
    row = {
        "id": 1,
        "intent": "other",
        "priority": "normal",
        "inbound_text": "Thanks!",
        "relationship_context": None,
        "platform_metadata": None,
        "quality_score": 6.0,
        "quality_flags": None,
        "detected_at": "2026-04-25T08:00:00+00:00",
    }
    row.update(overrides)
    return row


def test_urgent_known_bug_report_scores_highest():
    score = score_reply_priority(
        _reply(
            intent="bug_report",
            priority="high",
            inbound_text="This crashes with an auth error, can you help?",
            relationship_context=json.dumps(
                {
                    "engagement_stage": 3,
                    "dunbar_tier": 2,
                    "relationship_strength": 0.8,
                    "is_known": True,
                }
            ),
            platform_metadata=json.dumps(
                {
                    "conversation_depth": 2,
                    "reply_root": {"uri": "at://did:plc:me/root"},
                    "mentions_our_handle": True,
                }
            ),
            quality_score=8.5,
        ),
        now=NOW,
    )

    assert score.label == "urgent"
    assert score.score == 100
    assert "intent:bug_report:+35" in score.reasons
    assert "stored_priority:high:+12" in score.reasons


def test_low_value_appreciation_with_quality_flags_scores_low():
    score = score_reply_priority(
        _reply(
            intent="appreciation",
            priority="low",
            inbound_text="Thanks",
            quality_score=3.0,
            quality_flags=json.dumps(["generic", "sycophantic"]),
            detected_at="2026-04-22T08:00:00+00:00",
        ),
        now=NOW,
    )

    assert score.label == "low"
    assert score.score < 38
    assert "quality:-22" in score.reasons


def test_malformed_json_inputs_are_ignored_deterministically():
    score = score_reply_priority(
        _reply(
            inbound_text="ok",
            quality_score=None,
            relationship_context="not json",
            platform_metadata="[1, 2]",
            quality_flags='{"not": "a list"}',
        ),
        now=NOW,
    )

    assert score.label == "normal"
    assert score.score == score_reply_priority(
        _reply(
            relationship_context=None,
            platform_metadata=None,
            quality_flags=None,
            inbound_text="ok",
            quality_score=None,
        ),
        now=NOW,
    ).score


def test_prioritize_replies_sorts_by_score_age_then_id():
    tied_higher_id = _reply(id=2, inbound_text="Can you explain this?", intent="question")
    tied_newer = _reply(
        id=1,
        inbound_text="Can you explain this?",
        intent="question",
    )
    urgent = _reply(
        id=3,
        intent="bug_report",
        priority="high",
        inbound_text="Urgent regression: this fails with an exception?",
    )

    ordered = prioritize_replies([tied_higher_id, urgent, tied_newer], now=NOW)

    assert [row["id"] for row in ordered] == [3, 1, 2]
    assert all("computed_priority" in row for row in ordered)
