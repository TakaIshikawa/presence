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
