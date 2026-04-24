"""Tests for review_replies.py display formatting functions."""

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

# Add scripts/ to path so we can import the module under test
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from review_helpers import format_relationship_context
from review_replies import (
    _format_quality_line,
    _format_triage_line,
    _publish_reply,
    _record_publish_result,
)


# --- format_relationship_context ---


class TestFormatContextLine:
    def test_full_context(self):
        ctx = {
            "engagement_stage": 3,
            "stage_name": "Active",
            "dunbar_tier": 2,
            "tier_name": "Key Network",
            "relationship_strength": 0.42,
        }
        result = format_relationship_context(json.dumps(ctx))
        assert result == "Active (stage 3) | Key Network (tier 2) | strength: 0.42"

    def test_partial_context_stage_only(self):
        ctx = {"engagement_stage": 1, "stage_name": "Ambient"}
        result = format_relationship_context(json.dumps(ctx))
        assert result == "Ambient (stage 1)"

    def test_missing_stage_name_shows_question_mark(self):
        ctx = {"engagement_stage": 3}
        result = format_relationship_context(json.dumps(ctx))
        assert result == "? (stage 3)"

    def test_none_input_returns_none(self):
        assert format_relationship_context(None) is None

    def test_empty_string_returns_none(self):
        assert format_relationship_context("") is None

    def test_malformed_json_returns_none(self):
        assert format_relationship_context("not json{") is None

    def test_empty_object_returns_none(self):
        assert format_relationship_context("{}") is None


# --- _format_quality_line ---


class TestFormatQualityLine:
    def test_passing_score_no_flags(self):
        result = _format_quality_line(7.5, None)
        assert result == "Quality: 7.5/10"

    def test_flagged_score(self):
        result = _format_quality_line(3.0, '["sycophantic"]')
        assert result == "Quality: 3.0/10 ⚠ sycophantic"

    def test_multiple_flags(self):
        result = _format_quality_line(2.0, '["sycophantic", "generic"]')
        assert "sycophantic" in result
        assert "generic" in result

    def test_none_score_returns_none(self):
        assert _format_quality_line(None, None) is None

    def test_score_with_empty_flags(self):
        result = _format_quality_line(8.0, "[]")
        assert result == "Quality: 8.0/10"

    def test_score_with_malformed_flags(self):
        result = _format_quality_line(6.0, "not json")
        assert result == "Quality: 6.0/10"


class TestFormatTriageLine:
    def test_score_and_reason(self):
        result = _format_triage_line(72.25, "high priority; question")
        assert result == "Triage: 72.2 - high priority; question"

    def test_score_without_reason(self):
        result = _format_triage_line(31.0, None)
        assert result == "Triage: 31.0"

    def test_none_score_returns_none_for_backwards_compatibility(self):
        assert _format_triage_line(None, "high priority") is None


class TestPublishReply:
    def test_publish_reply_uses_x_client_by_default(self):
        config = SimpleNamespace()
        x_client = MagicMock()
        x_client.reply.return_value = SimpleNamespace(
            success=True,
            tweet_id="posted-x-1",
            url="https://x.com/me/status/posted-x-1",
        )
        reply = {
            "platform": "x",
            "inbound_tweet_id": "inbound-x-1",
        }

        result = _publish_reply(reply, "Thanks!", config, x_client, None)

        assert result["publish_result"].tweet_id == "posted-x-1"
        x_client.reply.assert_called_once_with("Thanks!", "inbound-x-1")

    def test_publish_reply_uses_bluesky_metadata(self):
        bluesky_client = MagicMock()
        bluesky_client.reply_from_queue_metadata.return_value = SimpleNamespace(
            success=True,
            uri="at://did:plc:me/app.bsky.feed.post/reply1",
            url="https://bsky.app/profile/me.bsky.social/post/reply1",
        )
        config = SimpleNamespace(
            bluesky=SimpleNamespace(
                enabled=True,
                handle="me.bsky.social",
                app_password="app-password",
            )
        )
        reply = {
            "platform": "bluesky",
            "inbound_tweet_id": "at://did:plc:alice/app.bsky.feed.post/inbound1",
            "inbound_cid": "inbound-cid",
            "our_platform_id": "at://did:plc:me/app.bsky.feed.post/root1",
            "our_tweet_id": "at://did:plc:me/app.bsky.feed.post/root1",
            "platform_metadata": json.dumps(
                {
                    "reply_root": {
                        "uri": "at://did:plc:me/app.bsky.feed.post/root1",
                        "cid": "root-cid",
                    }
                }
            ),
        }

        result = _publish_reply(reply, "Thanks!", config, None, bluesky_client)

        assert result["publish_result"].uri == "at://did:plc:me/app.bsky.feed.post/reply1"
        bluesky_client.reply_from_queue_metadata.assert_called_once_with(
            "Thanks!",
            inbound_uri="at://did:plc:alice/app.bsky.feed.post/inbound1",
            inbound_cid="inbound-cid",
            platform_metadata=reply["platform_metadata"],
            our_platform_id="at://did:plc:me/app.bsky.feed.post/root1",
        )

    def test_record_bluesky_success_stores_platform_id_without_tweet_id(self):
        db = MagicMock()
        reply = {"id": 123}
        publish_result = SimpleNamespace(
            success=True,
            uri="at://did:plc:me/app.bsky.feed.post/reply1",
            url="https://bsky.app/profile/me.bsky.social/post/reply1",
        )

        posted = _record_publish_result(db, reply, publish_result)

        assert posted is True
        db.update_reply_status.assert_called_once_with(
            123,
            "posted",
            posted_tweet_id=None,
            posted_platform_id="at://did:plc:me/app.bsky.feed.post/reply1",
        )

    def test_record_failure_does_not_update_status(self):
        db = MagicMock()
        reply = {"id": 123}
        publish_result = SimpleNamespace(success=False, error="Missing refs")

        posted = _record_publish_result(db, reply, publish_result)

        assert posted is False
        db.update_reply_status.assert_not_called()
