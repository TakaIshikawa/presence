"""Tests for deterministic reply triage scoring."""

import json
from datetime import datetime, timezone

from engagement.reply_triage import (
    score_pending_replies,
    score_reply_triage,
    sort_by_triage,
)


NOW = datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc)


def _reply(**overrides):
    row = {
        "id": 1,
        "priority": "normal",
        "intent": "other",
        "detected_at": "2026-04-25T10:00:00+00:00",
        "relationship_context": None,
        "quality_score": None,
        "platform_metadata": None,
    }
    row.update(overrides)
    return row


class TestReplyTriage:
    def test_high_value_question_with_relationship_scores_higher_than_low_appreciation(self):
        high_value = _reply(
            priority="high",
            intent="question",
            detected_at="2026-04-24T12:00:00+00:00",
            relationship_context=json.dumps(
                {
                    "engagement_stage": 3,
                    "stage_name": "Active",
                    "dunbar_tier": 2,
                    "tier_name": "Key Network",
                    "relationship_strength": 0.8,
                }
            ),
            quality_score=8.0,
            platform_metadata=json.dumps({"parent_post_text": "Thread context"}),
        )
        low_value = _reply(
            priority="low",
            intent="appreciation",
            detected_at="2026-04-25T11:00:00+00:00",
            relationship_context=json.dumps({"relationship_strength": 0.1}),
            quality_score=4.0,
        )

        high_score = score_reply_triage(high_value, now=NOW)
        low_score = score_reply_triage(low_value, now=NOW)

        assert high_score.score > low_score.score
        assert "high priority" in high_score.reason
        assert "question" in high_score.reason

    def test_age_increases_score_but_is_capped(self):
        young = score_reply_triage(
            _reply(detected_at="2026-04-25T10:00:00+00:00"),
            now=NOW,
        )
        old = score_reply_triage(
            _reply(detected_at="2026-04-22T12:00:00+00:00"),
            now=NOW,
        )
        very_old = score_reply_triage(
            _reply(detected_at="2026-04-01T12:00:00+00:00"),
            now=NOW,
        )

        assert old.score > young.score
        assert very_old.score == old.score

    def test_quality_score_can_penalize_or_boost(self):
        weak = score_reply_triage(_reply(quality_score=2.0), now=NOW)
        strong = score_reply_triage(_reply(quality_score=9.0), now=NOW)

        assert strong.score > weak.score
        assert "quality 9.0/10" in strong.reason

    def test_platform_metadata_adds_context_signal(self):
        plain = score_reply_triage(_reply(platform_metadata=None), now=NOW)
        contextual = score_reply_triage(
            _reply(
                platform_metadata=json.dumps(
                    {
                        "quoted_tweet_id": "quote-1",
                        "parent_post_text": "Parent context",
                        "reply_refs": ["root", "parent"],
                    }
                )
            ),
            now=NOW,
        )

        assert contextual.score > plain.score
        assert "quote context" in contextual.reason

    def test_stable_triage_sort_uses_score_detected_at_then_id(self):
        rows = score_pending_replies(
            [
                _reply(id=3, priority="normal", detected_at="2026-04-25T10:00:00+00:00"),
                _reply(id=1, priority="high", detected_at="2026-04-25T11:00:00+00:00"),
                _reply(id=2, priority="normal", detected_at="2026-04-25T09:00:00+00:00"),
            ],
            now=NOW,
        )

        ordered = sort_by_triage(rows)

        assert [row["id"] for row in ordered] == [1, 2, 3]
