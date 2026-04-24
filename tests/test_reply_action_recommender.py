"""Tests for inbound mention reply action recommendations."""

from __future__ import annotations

import json

from engagement.reply_action_recommender import ReplyActionRecommender


def _row(**kwargs):
    defaults = {
        "id": 1,
        "platform": "x",
        "status": "pending",
        "inbound_author_handle": "alice",
        "inbound_tweet_id": "mention-1",
        "inbound_text": "Nice post",
        "our_post_text": "Original post",
        "draft_text": "Thanks for reading.",
        "intent": "other",
        "priority": "normal",
        "quality_score": None,
        "quality_flags": None,
        "detected_at": "2026-04-23T09:00:00+00:00",
    }
    defaults.update(kwargs)
    return defaults


def test_direct_question_recommends_reply_now_when_draft_quality_is_safe():
    recommendation = ReplyActionRecommender().recommend(
        _row(
            inbound_text="How does this handle retries?",
            intent="question",
            quality_score=7.5,
            quality_flags="[]",
            draft_text="It retries idempotent operations with backoff.",
        )
    )

    assert recommendation.action == "reply_now"
    assert recommendation.reason == "direct question or support issue"


def test_direct_question_with_high_evaluator_risk_needs_manual_review():
    recommendation = ReplyActionRecommender().recommend(
        _row(
            inbound_text="How does this handle retries?",
            intent="question",
            quality_score=3.0,
            quality_flags=json.dumps(["generic"]),
        )
    )

    assert recommendation.action == "needs_manual_review"
    assert recommendation.reason == "evaluator flagged draft risk"


def test_quote_worthy_claim_recommends_quote_candidate():
    recommendation = ReplyActionRecommender().recommend(
        _row(
            inbound_text=(
                "Hot take: teams underestimate regression tests because the real "
                "problem is tool-call drift, not model creativity."
            ),
            intent="other",
            draft_text="",
        )
    )

    assert recommendation.action == "quote_candidate"
    assert "quote-worthy" in recommendation.reason


def test_spam_recommends_no_response_with_reason():
    recommendation = ReplyActionRecommender().recommend(
        _row(
            inbound_text="Free crypto giveaway, click https://example.com",
            intent="spam",
        )
    )

    assert recommendation.action == "no_response"
    assert recommendation.reason == "spam pattern"


def test_low_signal_praise_recommends_no_response_with_reason():
    recommendation = ReplyActionRecommender().recommend(
        _row(inbound_text="Thanks, great post", intent="appreciation")
    )

    assert recommendation.action == "no_response"
    assert recommendation.reason == "low-signal mention"


def test_support_issue_recommends_reply_now():
    recommendation = ReplyActionRecommender().recommend(
        _row(
            inbound_text="This crashes with a traceback on startup",
            intent="bug_report",
            priority="high",
            draft_text="Can you share the version and startup command?",
        )
    )

    assert recommendation.action == "reply_now"


def test_high_context_question_needs_manual_review():
    recommendation = ReplyActionRecommender().recommend(
        _row(
            inbound_text="Can you review my repo and tell me what to change before I launch?",
            intent="question",
            draft_text="",
        )
    )

    assert recommendation.action == "needs_manual_review"
    assert recommendation.reason == "high-context ask"


def test_recommend_many_marks_duplicate_mentions_from_same_author():
    rows = [
        _row(id=1, inbound_tweet_id="one", inbound_text="How does this handle retries?", intent="question"),
        _row(id=2, inbound_tweet_id="two", inbound_text="How does this handle retries?", intent="question"),
    ]

    recommendations = ReplyActionRecommender().recommend_many(rows)
    duplicate = next(item for item in recommendations if item.reply_id == 2)

    assert duplicate.action == "no_response"
    assert duplicate.reason == "duplicate mention from same author"
