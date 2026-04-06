"""Tests for the Claude-powered reply drafter."""

from unittest.mock import MagicMock, patch

import pytest

from engagement.reply_drafter import ReplyDrafter, SYSTEM_PROMPT


# --- SYSTEM_PROMPT content ---


class TestSystemPrompt:
    def test_emphasizes_authenticity(self):
        assert "authentically" in SYSTEM_PROMPT

    def test_prohibits_hashtags(self):
        assert "hashtags" in SYSTEM_PROMPT.lower()

    def test_prohibits_sycophancy(self):
        assert "sycophantic" in SYSTEM_PROMPT.lower()

    def test_prohibits_em_dashes(self):
        assert "em-dash" in SYSTEM_PROMPT.lower()

    def test_character_limit(self):
        assert "280 characters" in SYSTEM_PROMPT


# --- ReplyDrafter construction ---


class TestReplyDrafterInit:
    def test_creates_anthropic_client(self):
        with patch("engagement.reply_drafter.anthropic.Anthropic") as mock_cls:
            drafter = ReplyDrafter(api_key="sk-test", model="claude-sonnet-4-5-20250929")
            mock_cls.assert_called_once_with(api_key="sk-test")

    def test_stores_model(self):
        with patch("engagement.reply_drafter.anthropic.Anthropic"):
            drafter = ReplyDrafter(api_key="sk-test", model="claude-sonnet-4-5-20250929")
            assert drafter.model == "claude-sonnet-4-5-20250929"


# --- draft() ---


class TestDraft:
    @pytest.fixture
    def drafter(self):
        with patch("engagement.reply_drafter.anthropic.Anthropic") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            d = ReplyDrafter(api_key="sk-test", model="claude-sonnet-4-5-20250929")
            d._mock_client = mock_client  # expose for assertions
            yield d

    def _set_reply(self, drafter, text):
        mock_response = MagicMock()
        mock_response.content = [MagicMock(text=text)]
        drafter._mock_client.messages.create.return_value = mock_response

    # --- API call parameters ---

    def test_calls_api_with_correct_model(self, drafter):
        self._set_reply(drafter, "Nice insight!")
        drafter.draft("my post", "their reply", "them", "me")

        call_kwargs = drafter._mock_client.messages.create.call_args[1]
        assert call_kwargs["model"] == "claude-sonnet-4-5-20250929"

    def test_calls_api_with_max_tokens(self, drafter):
        self._set_reply(drafter, "Nice insight!")
        drafter.draft("my post", "their reply", "them", "me")

        call_kwargs = drafter._mock_client.messages.create.call_args[1]
        assert call_kwargs["max_tokens"] == 150

    def test_passes_system_prompt(self, drafter):
        self._set_reply(drafter, "Nice insight!")
        drafter.draft("my post", "their reply", "them", "me")

        call_kwargs = drafter._mock_client.messages.create.call_args[1]
        assert call_kwargs["system"] == SYSTEM_PROMPT

    def test_message_role_is_user(self, drafter):
        self._set_reply(drafter, "Nice insight!")
        drafter.draft("my post", "their reply", "them", "me")

        call_kwargs = drafter._mock_client.messages.create.call_args[1]
        messages = call_kwargs["messages"]
        assert len(messages) == 1
        assert messages[0]["role"] == "user"

    # --- Context passed in prompt ---

    def test_prompt_includes_original_post(self, drafter):
        self._set_reply(drafter, "Sure thing")
        drafter.draft("Building in public is underrated", "their reply", "them", "me")

        prompt = drafter._mock_client.messages.create.call_args[1]["messages"][0]["content"]
        assert "Building in public is underrated" in prompt

    def test_prompt_includes_their_reply(self, drafter):
        self._set_reply(drafter, "Sure thing")
        drafter.draft("my post", "Totally agree, I had the same experience", "them", "me")

        prompt = drafter._mock_client.messages.create.call_args[1]["messages"][0]["content"]
        assert "Totally agree, I had the same experience" in prompt

    def test_prompt_includes_their_handle(self, drafter):
        self._set_reply(drafter, "Sure thing")
        drafter.draft("my post", "their reply", "dev_jane", "me")

        prompt = drafter._mock_client.messages.create.call_args[1]["messages"][0]["content"]
        assert "@dev_jane" in prompt

    def test_prompt_includes_self_handle(self, drafter):
        self._set_reply(drafter, "Sure thing")
        drafter.draft("my post", "their reply", "them", "taka_dev")

        prompt = drafter._mock_client.messages.create.call_args[1]["messages"][0]["content"]
        assert "@taka_dev" in prompt

    # --- Response extraction ---

    def test_returns_stripped_text(self, drafter):
        self._set_reply(drafter, "  Nice insight!  ")
        result = drafter.draft("my post", "their reply", "them", "me")
        assert result == "Nice insight!"

    def test_strips_surrounding_quotes(self, drafter):
        self._set_reply(drafter, '"Nice insight!"')
        result = drafter.draft("my post", "their reply", "them", "me")
        assert result == "Nice insight!"

    def test_strips_whitespace_then_quotes(self, drafter):
        self._set_reply(drafter, '  "That tracks with what I saw too"  ')
        result = drafter.draft("my post", "their reply", "them", "me")
        assert result == "That tracks with what I saw too"

    def test_preserves_internal_quotes(self, drafter):
        self._set_reply(drafter, 'The "real" question is why')
        result = drafter.draft("my post", "their reply", "them", "me")
        assert result == 'The "real" question is why'

    # --- Error handling ---

    def test_api_error_propagates(self, drafter):
        drafter._mock_client.messages.create.side_effect = Exception("API connection failed")
        with pytest.raises(Exception, match="API connection failed"):
            drafter.draft("my post", "their reply", "them", "me")


# --- person_context enrichment ---


from engagement.cultivate_bridge import PersonContext


def _make_person_context(**overrides):
    """Build a PersonContext with sensible defaults."""
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
        recent_interactions=[],
        is_known=True,
    )
    defaults.update(overrides)
    return PersonContext(**defaults)


class TestContextSection:
    def test_build_context_section_full(self):
        ctx = _make_person_context(
            recent_interactions=[
                {"type": "reply", "direction": "them → me", "date": "2026-03-28", "snippet": "interesting take"},
                {"type": "like", "direction": "me → them", "date": "2026-03-20", "snippet": ""},
            ]
        )
        section = ReplyDrafter._build_context_section(ctx)
        assert "## Relationship Context for @dev_jane" in section
        assert "Bio: Building AI tools" in section
        assert "Active (stage 3)" in section
        assert "Key Network (tier 2)" in section
        assert "Relationship strength: 0.42" in section
        assert "[2026-03-28] reply (them → me): interesting take" in section
        assert "[2026-03-20] like (me → them)" in section

    def test_build_context_section_minimal(self):
        ctx = _make_person_context(
            bio=None,
            relationship_strength=None,
            engagement_stage=None,
            dunbar_tier=None,
            recent_interactions=[],
        )
        section = ReplyDrafter._build_context_section(ctx)
        assert "## Relationship Context for @dev_jane" in section
        assert "Bio:" not in section
        assert "stage" not in section
        assert "tier" not in section
        assert "strength" not in section

    def test_build_context_section_limits_interactions(self):
        interactions = [
            {"type": "reply", "direction": "them → me", "date": f"2026-03-{i:02d}", "snippet": f"msg {i}"}
            for i in range(1, 11)
        ]
        ctx = _make_person_context(recent_interactions=interactions)
        section = ReplyDrafter._build_context_section(ctx)
        # Only first 5 should be included
        assert "msg 5" in section
        assert "msg 6" not in section

    def test_draft_with_person_context_injects_section(self):
        with patch("engagement.reply_drafter.anthropic.Anthropic") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            mock_response = MagicMock()
            mock_response.content = [MagicMock(text="Nice insight")]
            mock_client.messages.create.return_value = mock_response

            drafter = ReplyDrafter(api_key="sk-test", model="test-model")
            ctx = _make_person_context()
            drafter.draft("my post", "their reply", "them", "me", person_context=ctx)

            prompt = mock_client.messages.create.call_args[1]["messages"][0]["content"]
            assert "Relationship Context for @dev_jane" in prompt
            assert "Active (stage 3)" in prompt

    def test_draft_with_unknown_person_skips_context(self):
        with patch("engagement.reply_drafter.anthropic.Anthropic") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            mock_response = MagicMock()
            mock_response.content = [MagicMock(text="Nice insight")]
            mock_client.messages.create.return_value = mock_response

            drafter = ReplyDrafter(api_key="sk-test", model="test-model")
            ctx = _make_person_context(is_known=False)
            drafter.draft("my post", "their reply", "them", "me", person_context=ctx)

            prompt = mock_client.messages.create.call_args[1]["messages"][0]["content"]
            assert "Relationship Context" not in prompt

    def test_draft_without_person_context_no_section(self):
        with patch("engagement.reply_drafter.anthropic.Anthropic") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            mock_response = MagicMock()
            mock_response.content = [MagicMock(text="Nice insight")]
            mock_client.messages.create.return_value = mock_response

            drafter = ReplyDrafter(api_key="sk-test", model="test-model")
            drafter.draft("my post", "their reply", "them", "me")

            prompt = mock_client.messages.create.call_args[1]["messages"][0]["content"]
            assert "Relationship Context" not in prompt
