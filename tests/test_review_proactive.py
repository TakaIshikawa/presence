"""Tests for review_proactive.py normalization and helper functions."""

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

# Add scripts/ and src/ to path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.cultivate_bridge import PersonContext, ProactiveAction
from review_proactive import (
    _normalize_presence_action,
    _normalize_cultivate_action,
    _mark_completed,
    _mark_dismissed,
    _format_action_header,
    _open_action_url,
)


def _make_person_context(**overrides):
    defaults = dict(
        x_handle="dev_jane",
        display_name="Jane Dev",
        bio="Building AI tools",
        relationship_strength=0.42,
        engagement_stage=3,
        dunbar_tier=2,
        authenticity_score=0.85,
        content_quality_score=0.7,
        content_relevance_score=0.6,
        is_known=True,
    )
    defaults.update(overrides)
    return PersonContext(**defaults)


def _make_presence_row(**overrides):
    """Build a dict mimicking a proactive_actions row."""
    defaults = {
        "id": 1,
        "action_type": "reply",
        "target_tweet_id": "t_123",
        "target_tweet_text": "AI agents are underrated",
        "target_author_handle": "karpathy",
        "target_author_id": "99",
        "discovery_source": "curated_timeline",
        "relevance_score": 0.78,
        "draft_text": "We've seen similar patterns in our work.",
        "status": "pending",
        "relationship_context": None,
        "knowledge_ids": None,
    }
    defaults.update(overrides)
    return defaults


def _make_cultivate_action(**overrides):
    """Build a CultivateBridge ProactiveAction."""
    defaults = dict(
        action_id="cult_1",
        action_type="engagement",
        target_handle="swyx",
        target_person_id="p_42",
        description="[reply] Good conversation starter",
        payload={
            "tweet_id": "t_456",
            "tweet_content": "Building in public is hard",
            "draft": "Totally agree - the feedback loop is invaluable.",
            "execution_type": "reply",
        },
        person_context=None,
    )
    defaults.update(overrides)
    return ProactiveAction(**defaults)


# --- Normalization ---


class TestNormalizePresenceAction:
    def test_maps_fields_correctly(self):
        row = _make_presence_row()
        norm = _normalize_presence_action(row)

        assert norm["source"] == "presence"
        assert norm["id"] == 1
        assert norm["action_type"] == "reply"
        assert norm["target_handle"] == "karpathy"
        assert norm["target_tweet_id"] == "t_123"
        assert norm["target_tweet_text"] == "AI agents are underrated"
        assert norm["draft_text"] == "We've seen similar patterns in our work."
        assert norm["relevance_score"] == 0.78
        assert norm["discovery_source"] == "curated_timeline"

    def test_handles_missing_optional_fields(self):
        row = _make_presence_row(
            relationship_context=None,
            relevance_score=None,
            draft_text=None,
        )
        norm = _normalize_presence_action(row)
        assert norm["relationship_context"] is None
        assert norm["relevance_score"] is None
        assert norm["draft_text"] is None


class TestNormalizeCultivateAction:
    def test_maps_fields_correctly(self):
        action = _make_cultivate_action()
        norm = _normalize_cultivate_action(action)

        assert norm["source"] == "cultivate"
        assert norm["id"] == "cult_1"
        assert norm["action_type"] == "reply"
        assert norm["target_handle"] == "swyx"
        assert norm["target_tweet_id"] == "t_456"
        assert norm["target_tweet_text"] == "Building in public is hard"
        assert norm["draft_text"] == "Totally agree - the feedback loop is invaluable."
        assert norm["discovery_source"] == "cultivate"

    def test_extracts_exec_type_from_description_tag(self):
        action = _make_cultivate_action(
            payload={"tweet_id": "t_1"},
            description="[like] Good post to engage with",
        )
        norm = _normalize_cultivate_action(action)
        assert norm["action_type"] == "like"

    def test_no_payload_yields_none_fields(self):
        action = _make_cultivate_action(payload=None)
        norm = _normalize_cultivate_action(action)
        assert norm["target_tweet_id"] is None
        assert norm["target_tweet_text"] is None
        assert norm["draft_text"] is None

    def test_preserves_original_cultivate_action(self):
        action = _make_cultivate_action()
        norm = _normalize_cultivate_action(action)
        assert norm["_cultivate_action"] is action


# --- mark_completed / mark_dismissed ---


class TestMarkCompleted:
    def test_presence_action_calls_db(self, db):
        aid = db.insert_proactive_action(
            action_type="reply",
            target_tweet_id="t_1",
            target_tweet_text="test",
            target_author_handle="user",
        )
        action = {"source": "presence", "id": aid}

        _mark_completed(action, db, bridge=None, posted_tweet_id="posted_1")

        row = db.conn.execute(
            "SELECT status, posted_tweet_id FROM proactive_actions WHERE id = ?",
            (aid,),
        ).fetchone()
        assert row[0] == "posted"
        assert row[1] == "posted_1"

    def test_cultivate_action_calls_bridge(self):
        bridge = MagicMock()
        action = {"source": "cultivate", "id": "cult_1"}

        _mark_completed(action, db=MagicMock(), bridge=bridge, posted_tweet_id="p_1")
        bridge.mark_action_completed.assert_called_once_with("cult_1")


class TestMarkDismissed:
    def test_presence_action_calls_db(self, db):
        aid = db.insert_proactive_action(
            action_type="reply",
            target_tweet_id="t_1",
            target_tweet_text="test",
            target_author_handle="user",
        )
        action = {"source": "presence", "id": aid}

        _mark_dismissed(action, db, bridge=None)

        row = db.conn.execute(
            "SELECT status FROM proactive_actions WHERE id = ?", (aid,),
        ).fetchone()
        assert row[0] == "dismissed"

    def test_cultivate_action_calls_bridge(self):
        bridge = MagicMock()
        action = {"source": "cultivate", "id": "cult_2"}

        _mark_dismissed(action, db=MagicMock(), bridge=bridge)
        bridge.mark_action_dismissed.assert_called_once_with("cult_2")


# --- Format action header ---


class TestFormatActionHeader:
    def test_includes_action_type_and_handle(self):
        action = _normalize_presence_action(_make_presence_row())
        header = _format_action_header(action)
        assert "REPLY -> @karpathy" in header

    def test_includes_relevance_score(self):
        action = _normalize_presence_action(_make_presence_row(relevance_score=0.82))
        header = _format_action_header(action)
        assert "0.82" in header

    def test_includes_discovery_source(self):
        action = _normalize_presence_action(_make_presence_row())
        header = _format_action_header(action)
        assert "curated_timeline" in header

    def test_includes_relationship_context(self):
        ctx_json = json.dumps({
            "engagement_stage": 3,
            "stage_name": "Active",
            "dunbar_tier": 2,
            "tier_name": "Key Network",
            "relationship_strength": 0.55,
        })
        action = _normalize_presence_action(
            _make_presence_row(relationship_context=ctx_json)
        )
        header = _format_action_header(action)
        assert "Active (stage 3)" in header
        assert "Key Network (tier 2)" in header

    def test_no_relevance_score_omits_it(self):
        action = _normalize_presence_action(_make_presence_row(relevance_score=None))
        header = _format_action_header(action)
        assert "relevance" not in header

    def test_cultivate_action_format(self):
        action = _normalize_cultivate_action(_make_cultivate_action())
        header = _format_action_header(action)
        assert "REPLY -> @swyx" in header
        assert "cultivate" in header


# --- Daily cap integration ---


class TestDailyCapIntegration:
    def test_count_daily_proactive_posts_starts_zero(self, db):
        assert db.count_daily_proactive_posts("reply") == 0

    def test_count_increments_after_posting(self, db):
        for i in range(3):
            aid = db.insert_proactive_action(
                action_type="reply",
                target_tweet_id=f"t_{i}",
                target_tweet_text=f"Tweet {i}",
                target_author_handle="user",
            )
            db.mark_proactive_posted(aid, f"posted_{i}")

        assert db.count_daily_proactive_posts("reply") == 3

    def test_presence_and_cultivate_merged(self):
        """Both sources should appear in the unified action list."""
        presence_row = _make_presence_row(id=1)
        cultivate_action = _make_cultivate_action(action_id="c_1")

        actions = [
            _normalize_presence_action(presence_row),
            _normalize_cultivate_action(cultivate_action),
        ]

        assert len(actions) == 2
        assert actions[0]["source"] == "presence"
        assert actions[1]["source"] == "cultivate"


# --- Open action URL ---


class TestOpenActionUrl:
    @patch("review_proactive.webbrowser.open")
    def test_opens_tweet_url(self, mock_open):
        action = _normalize_presence_action(_make_presence_row())
        _open_action_url(action)
        mock_open.assert_called_once_with("https://x.com/karpathy/status/t_123")

    @patch("review_proactive.webbrowser.open")
    def test_falls_back_to_profile_url(self, mock_open):
        action = _normalize_presence_action(_make_presence_row(target_tweet_id=None))
        _open_action_url(action)
        mock_open.assert_called_once_with("https://x.com/karpathy")

    @patch("review_proactive.webbrowser.open")
    def test_no_handle_prints_message(self, mock_open):
        action = {"target_handle": "", "target_tweet_id": None}
        _open_action_url(action)
        mock_open.assert_not_called()
