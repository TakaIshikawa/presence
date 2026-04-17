"""Unit tests for scripts/poll_replies.py mention processing logic."""

import json
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import anthropic
import pytest
import tweepy

# Add scripts/ and src/ to path so we can import the module under test
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


# ---------------------------------------------------------------------------
# Helpers — fake config objects and mention builders
# ---------------------------------------------------------------------------


def _make_config(
    replies_enabled=True,
    max_daily_replies=10,
    cultivate_enabled=False,
    cultivate_db_path="~/.cultivate/cultivate.db",
    forward_mentions=False,
    enrich_replies=False,
    reply_quality_threshold=6.0,
    embeddings_enabled=False,
):
    """Build a minimal Config-like namespace matching load_config() shape."""
    x = SimpleNamespace(
        api_key="k", api_secret="s", access_token="at", access_token_secret="ats"
    )
    anthropic = SimpleNamespace(api_key="ak")
    synthesis = SimpleNamespace(model="claude-test")
    paths = SimpleNamespace(database=":memory:")
    replies = SimpleNamespace(
        enabled=replies_enabled, max_daily_replies=max_daily_replies
    )

    cultivate = None
    if cultivate_enabled:
        cultivate = SimpleNamespace(
            enabled=True,
            db_path=cultivate_db_path,
            forward_mentions=forward_mentions,
            enrich_replies=enrich_replies,
            proactive_review=False,
            reply_quality_threshold=reply_quality_threshold,
        )

    embeddings = None
    if embeddings_enabled:
        embeddings = SimpleNamespace(
            api_key="voyage-key",
            model="voyage-3-large",
        )

    timeouts = SimpleNamespace(
        anthropic_seconds=300,
        github_seconds=30,
        http_seconds=30,
    )

    return SimpleNamespace(
        x=x,
        anthropic=anthropic,
        synthesis=synthesis,
        paths=paths,
        replies=replies,
        cultivate=cultivate,
        embeddings=embeddings,
        timeouts=timeouts,
    )


def _make_mention(
    tweet_id="100",
    text="Nice post!",
    author_id="user_A",
    in_reply_to_user_id="my_id",
    conversation_id="conv_1",
    created_at="2026-04-06T12:00:00Z",
):
    """Build a mention dict matching XClient.get_mentions() output."""
    return {
        "id": tweet_id,
        "text": text,
        "author_id": author_id,
        "in_reply_to_user_id": in_reply_to_user_id,
        "conversation_id": conversation_id,
        "created_at": created_at,
    }


def _make_me(user_id="my_id", username="my_handle"):
    """Build a fake tweepy get_me() response."""
    data = SimpleNamespace(id=user_id, username=username)
    return SimpleNamespace(data=data)


USERS_BY_ID = {
    "user_A": {"id": "user_A", "username": "alice", "name": "Alice"},
    "user_B": {"id": "user_B", "username": "bob", "name": "Bob"},
}

OUR_CONTENT = {"id": 1, "content": "Here is our original post text."}


# ---------------------------------------------------------------------------
# Patch targets — all imports happen inside scripts/poll_replies.py
# ---------------------------------------------------------------------------

_PATCH_BASE = "poll_replies"


@pytest.fixture
def _patches():
    """Yield a namespace of mocks for every external dependency of main()."""
    with (
        patch(f"{_PATCH_BASE}.script_context") as mock_script_context,
        patch(f"{_PATCH_BASE}.update_monitoring") as mock_update_monitoring,
        patch(f"{_PATCH_BASE}.XClient") as MockXClient,
        patch(f"{_PATCH_BASE}.ReplyDrafter") as MockDrafter,
        patch(f"{_PATCH_BASE}.signal") as mock_signal,
    ):
        # Mock database
        db = MagicMock()
        db.count_replies_today.return_value = 0
        db.is_reply_processed.return_value = False
        db.get_last_mention_id.return_value = None
        db.get_content_by_tweet_id.return_value = OUR_CONTENT
        db.insert_reply_draft.return_value = 1

        # Mock config
        config = _make_config()

        # script_context is a context manager that yields (config, db)
        mock_script_context.return_value.__enter__.return_value = (config, db)
        mock_script_context.return_value.__exit__.return_value = None

        x_client = MockXClient.return_value
        x_client.client.get_me.return_value = _make_me()
        x_client.get_mentions.return_value = ([], USERS_BY_ID)

        drafter = MockDrafter.return_value
        draft_result = SimpleNamespace(
            reply_text="Great point, thanks for sharing!",
            knowledge_ids=[],
        )
        drafter.draft_with_lineage_with_lineage.return_value = draft_result

        yield SimpleNamespace(
            script_context=mock_script_context,
            update_monitoring=mock_update_monitoring,
            config=config,
            db=db,
            MockXClient=MockXClient,
            x_client=x_client,
            MockDrafter=MockDrafter,
            drafter=drafter,
            signal=mock_signal,
        )


# Import main after patches are possible (module-level side effects are
# handled by the sys.path.insert above).
from poll_replies import main, _timeout_handler


# ---------------------------------------------------------------------------
# 1. Daily reply cap enforcement
# ---------------------------------------------------------------------------


class TestDailyReplyCap:
    def test_skips_when_cap_already_reached(self, _patches):
        """Processing stops immediately when replies_today >= max_daily."""
        _patches.db.count_replies_today.return_value = 10
        _patches.config.replies.max_daily_replies = 10

        main()

        _patches.x_client.get_mentions.assert_not_called()
        _patches.update_monitoring.assert_called_with("poll-replies")

    def test_skips_when_cap_exceeded(self, _patches):
        """Processing stops when replies_today is over max_daily."""
        _patches.db.count_replies_today.return_value = 15
        _patches.config.replies.max_daily_replies = 10

        main()

        _patches.x_client.get_mentions.assert_not_called()

    def test_stops_mid_processing_when_cap_hit(self, _patches):
        """Only processes remaining_cap mentions, then breaks."""
        _patches.db.count_replies_today.return_value = 8
        _patches.config.replies.max_daily_replies = 10

        # 5 valid mentions but only 2 slots remaining (10 - 8 = 2)
        mentions = [
            _make_mention(tweet_id=str(100 + i), author_id="user_A")
            for i in range(5)
        ]
        _patches.x_client.get_mentions.return_value = (mentions, USERS_BY_ID)

        main()

        assert _patches.drafter.draft_with_lineage.call_count == 2
        assert _patches.db.insert_reply_draft.call_count == 2


# ---------------------------------------------------------------------------
# 2. Mention filtering logic
# ---------------------------------------------------------------------------


class TestMentionFiltering:
    def test_skips_self_mentions(self, _patches):
        """Mentions where author_id == my_user_id are skipped."""
        mention = _make_mention(author_id="my_id")
        _patches.x_client.get_mentions.return_value = ([mention], USERS_BY_ID)

        main()

        _patches.drafter.draft_with_lineage.assert_not_called()

    def test_skips_non_reply_mentions(self, _patches):
        """Mentions not replying to us (in_reply_to_user_id != my_id) are skipped."""
        mention = _make_mention(in_reply_to_user_id="someone_else")
        _patches.x_client.get_mentions.return_value = ([mention], USERS_BY_ID)

        main()

        _patches.drafter.draft_with_lineage.assert_not_called()

    def test_skips_already_processed_mentions(self, _patches):
        """Mentions already in DB (is_reply_processed=True) are skipped."""
        mention = _make_mention()
        _patches.x_client.get_mentions.return_value = ([mention], USERS_BY_ID)
        _patches.db.is_reply_processed.return_value = True

        main()

        _patches.drafter.draft_with_lineage.assert_not_called()

    def test_skips_mention_without_tracked_conversation(self, _patches):
        """Mentions replying to a tweet we don't track are skipped."""
        mention = _make_mention()
        _patches.x_client.get_mentions.return_value = ([mention], USERS_BY_ID)
        _patches.db.get_content_by_tweet_id.return_value = None

        main()

        _patches.drafter.draft_with_lineage.assert_not_called()

    def test_processes_valid_mention(self, _patches):
        """A mention that passes all filters gets drafted and stored."""
        mention = _make_mention()
        _patches.x_client.get_mentions.return_value = ([mention], USERS_BY_ID)

        main()

        _patches.drafter.draft_with_lineage.assert_called_once()
        _patches.db.insert_reply_draft.assert_called_once()

    def test_mixed_mentions_filters_correctly(self, _patches):
        """Of multiple mentions, only the valid ones are processed."""
        mentions = [
            _make_mention(tweet_id="101", author_id="my_id"),       # self-mention
            _make_mention(tweet_id="102", author_id="user_A"),       # valid
            _make_mention(
                tweet_id="103", author_id="user_B",
                in_reply_to_user_id="other",
            ),                                                        # not reply to us
            _make_mention(tweet_id="104", author_id="user_B"),       # valid
        ]
        _patches.x_client.get_mentions.return_value = (mentions, USERS_BY_ID)

        main()

        assert _patches.drafter.draft_with_lineage.call_count == 2
        assert _patches.db.insert_reply_draft.call_count == 2


# ---------------------------------------------------------------------------
# 3. Cursor management
# ---------------------------------------------------------------------------


class TestCursorManagement:
    def test_updates_cursor_to_highest_mention_id(self, _patches):
        """set_last_mention_id is called with the highest tweet_id."""
        mentions = [
            _make_mention(tweet_id="200", author_id="user_A"),
            _make_mention(tweet_id="300", author_id="user_A"),
            _make_mention(tweet_id="250", author_id="user_A"),
        ]
        _patches.x_client.get_mentions.return_value = (mentions, USERS_BY_ID)

        main()

        _patches.db.set_last_mention_id.assert_called_once_with("300")

    def test_cursor_not_updated_when_no_new_mentions(self, _patches):
        """set_last_mention_id is not called when no mentions returned."""
        _patches.x_client.get_mentions.return_value = ([], USERS_BY_ID)

        main()

        _patches.db.set_last_mention_id.assert_not_called()

    def test_cursor_not_updated_when_same_as_since_id(self, _patches):
        """When max_mention_id == since_id, cursor is not re-written."""
        _patches.db.get_last_mention_id.return_value = "200"
        # All mentions are skipped (self-mentions), but cursor updates
        # only if max_mention_id != since_id
        mention = _make_mention(tweet_id="200", author_id="my_id")
        _patches.x_client.get_mentions.return_value = ([mention], USERS_BY_ID)

        main()

        _patches.db.set_last_mention_id.assert_not_called()

    def test_cursor_advances_past_since_id(self, _patches):
        """When since_id is set and new mentions have higher IDs, cursor advances."""
        _patches.db.get_last_mention_id.return_value = "100"
        mention = _make_mention(tweet_id="200", author_id="user_A")
        _patches.x_client.get_mentions.return_value = ([mention], USERS_BY_ID)

        main()

        _patches.db.set_last_mention_id.assert_called_once_with("200")

    def test_cursor_tracks_highest_even_for_skipped_mentions(self, _patches):
        """Cursor advances even when all mentions are skipped (filtered out)."""
        mentions = [
            _make_mention(tweet_id="500", author_id="my_id"),  # self — skipped
            _make_mention(tweet_id="600", author_id="my_id"),  # self — skipped
        ]
        _patches.x_client.get_mentions.return_value = (mentions, USERS_BY_ID)

        main()

        _patches.db.set_last_mention_id.assert_called_once_with("600")


# ---------------------------------------------------------------------------
# 4. Cultivate integration path
# ---------------------------------------------------------------------------


class TestCultivateIntegration:
    def test_bridge_connected_calls_get_person_context(self, _patches):
        """When cultivate enabled + bridge connects, get_person_context is called."""
        _patches.config.cultivate = SimpleNamespace(enabled=True, db_path="~/.cultivate/cultivate.db", forward_mentions=False, enrich_replies=True, proactive_review=False, reply_quality_threshold=6.0)

        mock_bridge = MagicMock()
        mock_bridge.get_person_context.return_value = None

        mention = _make_mention(author_id="user_A")
        _patches.x_client.get_mentions.return_value = ([mention], USERS_BY_ID)

        with (
            patch("engagement.cultivate_bridge.CultivateBridge") as MockBridge,
            patch("engagement.reply_evaluator.ReplyEvaluator") as MockEval,
        ):
            MockBridge.try_connect.return_value = mock_bridge
            mock_evaluator = MockEval.return_value
            mock_evaluator.evaluate.return_value = SimpleNamespace(
                score=8.0, passes=True, feedback="good", flags=[]
            )
            main()

        mock_bridge.get_person_context.assert_called_once_with("alice")

    def test_forward_mentions_calls_record_mention_event(self, _patches):
        """When forward_mentions=True, record_mention_event is called."""
        _patches.config.cultivate = SimpleNamespace(enabled=True, db_path="~/.cultivate/cultivate.db", forward_mentions=True, enrich_replies=False, proactive_review=False, reply_quality_threshold=6.0)

        mock_bridge = MagicMock()
        mock_bridge.get_person_context.return_value = None

        mention = _make_mention(
            tweet_id="42", author_id="user_A", text="Nice post!"
        )
        _patches.x_client.get_mentions.return_value = ([mention], USERS_BY_ID)

        with (
            patch("engagement.cultivate_bridge.CultivateBridge") as MockBridge,
            patch("engagement.reply_evaluator.ReplyEvaluator") as MockEval,
        ):
            MockBridge.try_connect.return_value = mock_bridge
            mock_evaluator = MockEval.return_value
            mock_evaluator.evaluate.return_value = SimpleNamespace(
                score=8.0, passes=True, feedback="good", flags=[]
            )
            main()

        mock_bridge.record_mention_event.assert_called_once_with(
            tweet_id="42",
            author_x_id="user_A",
            author_handle="alice",
            text="Nice post!",
            created_at="2026-04-06T12:00:00Z",
        )

    def test_no_bridge_when_try_connect_returns_none(self, _patches):
        """When cultivate enabled but bridge fails to connect, enrichment skipped."""
        _patches.config.cultivate = SimpleNamespace(enabled=True, db_path="~/.cultivate/cultivate.db", forward_mentions=False, enrich_replies=True, proactive_review=False, reply_quality_threshold=6.0)

        mention = _make_mention(author_id="user_A")
        _patches.x_client.get_mentions.return_value = ([mention], USERS_BY_ID)

        with (
            patch("engagement.cultivate_bridge.CultivateBridge") as MockBridge,
            patch("engagement.reply_evaluator.ReplyEvaluator") as MockEval,
        ):
            MockBridge.try_connect.return_value = None
            mock_evaluator = MockEval.return_value
            mock_evaluator.evaluate.return_value = SimpleNamespace(
                score=8.0, passes=True, feedback="good", flags=[]
            )
            main()

        # Draft still called — bridge absence doesn't block processing
        _patches.drafter.draft_with_lineage.assert_called_once()

    def test_bridge_closed_at_end(self, _patches):
        """When bridge is connected, it is closed at the end of main()."""
        _patches.config.cultivate = SimpleNamespace(enabled=True, db_path="~/.cultivate/cultivate.db", forward_mentions=False, enrich_replies=False, proactive_review=False, reply_quality_threshold=6.0)

        mock_bridge = MagicMock()
        mock_bridge.get_person_context.return_value = None

        # Need at least one mention so main() reaches the cleanup block
        # (empty mentions causes early return before bridge.close)
        mention = _make_mention(author_id="user_A")
        _patches.x_client.get_mentions.return_value = ([mention], USERS_BY_ID)

        with (
            patch("engagement.cultivate_bridge.CultivateBridge") as MockBridge,
            patch("engagement.reply_evaluator.ReplyEvaluator") as MockEval,
        ):
            MockBridge.try_connect.return_value = mock_bridge
            mock_evaluator = MockEval.return_value
            mock_evaluator.evaluate.return_value = SimpleNamespace(
                score=8.0, passes=True, feedback="good", flags=[]
            )
            main()

        mock_bridge.close.assert_called_once()


# ---------------------------------------------------------------------------
# 5. Quality evaluation path
# ---------------------------------------------------------------------------


class TestQualityEvaluation:
    def test_evaluator_called_when_cultivate_enabled(self, _patches):
        """When cultivate is enabled, evaluator.evaluate() is called for each draft."""
        _patches.config.cultivate = SimpleNamespace(
            enabled=True, db_path="~/.cultivate/cultivate.db",
            forward_mentions=False, enrich_replies=False,
            proactive_review=False, reply_quality_threshold=7.0
        )

        mock_bridge = MagicMock()
        mock_bridge.get_person_context.return_value = None

        eval_result = SimpleNamespace(
            score=8.5, passes=True, feedback="good", flags=[]
        )
        mock_evaluator = MagicMock()
        mock_evaluator.evaluate.return_value = eval_result

        mention = _make_mention(author_id="user_A")
        _patches.x_client.get_mentions.return_value = ([mention], USERS_BY_ID)

        with (
            patch("engagement.cultivate_bridge.CultivateBridge") as MockBridge,
            patch("engagement.reply_evaluator.ReplyEvaluator") as MockEval,
        ):
            MockBridge.try_connect.return_value = mock_bridge
            MockEval.return_value = mock_evaluator
            main()

        mock_evaluator.evaluate.assert_called_once()
        call_kwargs = mock_evaluator.evaluate.call_args
        assert call_kwargs.kwargs["threshold"] == 7.0

    def test_quality_score_passed_to_insert_reply_draft(self, _patches):
        """quality_score and quality_flags from evaluator are stored in the DB."""
        _patches.config.cultivate = SimpleNamespace(enabled=True, db_path="~/.cultivate/cultivate.db", forward_mentions=False, enrich_replies=False, proactive_review=False, reply_quality_threshold=6.0)

        mock_bridge = MagicMock()
        mock_bridge.get_person_context.return_value = None

        eval_result = SimpleNamespace(
            score=5.0, passes=False, feedback="meh", flags=["generic", "sycophantic"]
        )
        mock_evaluator = MagicMock()
        mock_evaluator.evaluate.return_value = eval_result

        mention = _make_mention(author_id="user_A")
        _patches.x_client.get_mentions.return_value = ([mention], USERS_BY_ID)

        with (
            patch("engagement.cultivate_bridge.CultivateBridge") as MockBridge,
            patch("engagement.reply_evaluator.ReplyEvaluator") as MockEval,
        ):
            MockBridge.try_connect.return_value = mock_bridge
            MockEval.return_value = mock_evaluator
            main()

        call_kwargs = _patches.db.insert_reply_draft.call_args
        assert call_kwargs.kwargs["quality_score"] == 5.0
        assert json.loads(call_kwargs.kwargs["quality_flags"]) == [
            "generic",
            "sycophantic",
        ]

    def test_no_evaluator_when_cultivate_disabled(self, _patches):
        """Without cultivate, quality_score and quality_flags are None."""
        mention = _make_mention(author_id="user_A")
        _patches.x_client.get_mentions.return_value = ([mention], USERS_BY_ID)

        main()

        call_kwargs = _patches.db.insert_reply_draft.call_args
        assert call_kwargs.kwargs["quality_score"] is None
        assert call_kwargs.kwargs["quality_flags"] is None


# ---------------------------------------------------------------------------
# 6. Graceful error handling
# ---------------------------------------------------------------------------


class TestErrorHandling:
    def test_get_mentions_error_causes_early_return(self, _patches):
        """An exception from get_mentions() causes clean early return."""
        _patches.x_client.get_mentions.side_effect = tweepy.TweepyException("API rate limit")

        main()

        _patches.drafter.draft_with_lineage.assert_not_called()

    def test_draft_error_skips_mention_continues_processing(self, _patches):
        """An error in drafter.draft_with_lineage() skips that mention but continues."""
        mentions = [
            _make_mention(tweet_id="100", author_id="user_A"),
            _make_mention(tweet_id="200", author_id="user_B"),
            _make_mention(tweet_id="300", author_id="user_A"),
        ]
        _patches.x_client.get_mentions.return_value = (mentions, USERS_BY_ID)

        # First call raises, second and third succeed
        _patches.drafter.draft_with_lineage.side_effect = [
            anthropic.APITimeoutError(request=MagicMock()),
            SimpleNamespace(reply_text="Reply to mention 200", knowledge_ids=[]),
            SimpleNamespace(reply_text="Reply to mention 300", knowledge_ids=[]),
        ]

        main()

        assert _patches.drafter.draft_with_lineage.call_count == 3
        # Only 2 successful drafts get inserted
        assert _patches.db.insert_reply_draft.call_count == 2

    def test_replies_disabled_returns_early(self, _patches):
        """When replies.enabled is False, main() returns immediately."""
        _patches.config.replies.enabled = False

        main()

        # With script_context, DB is always initialized, but X operations should be skipped
        _patches.MockXClient.assert_not_called()
        _patches.update_monitoring.assert_called_with("poll-replies")


# ---------------------------------------------------------------------------
# 7. Watchdog timeout
# ---------------------------------------------------------------------------


class TestWatchdogTimeout:
    def test_timeout_handler_exits(self):
        """_timeout_handler calls sys.exit(1)."""
        with pytest.raises(SystemExit) as exc_info:
            _timeout_handler(None, None)
        assert exc_info.value.code == 1

    def test_signal_alarm_set_in_main(self, _patches):
        """main() sets SIGALRM with WATCHDOG_TIMEOUT."""
        main()

        _patches.signal.signal.assert_called()
        _patches.signal.alarm.assert_called_once_with(600)


# ---------------------------------------------------------------------------
# 8. Insert call correctness
# ---------------------------------------------------------------------------


class TestInsertReplyDraft:
    def test_insert_called_with_correct_fields(self, _patches):
        """Verify all fields passed to db.insert_reply_draft are correct."""
        mention = _make_mention(
            tweet_id="42",
            text="Interesting take!",
            author_id="user_A",
            conversation_id="conv_1",
        )
        _patches.x_client.get_mentions.return_value = ([mention], USERS_BY_ID)
        _patches.drafter.draft_with_lineage.return_value = SimpleNamespace(
            reply_text="Thanks for the feedback!",
            knowledge_ids=[],
        )

        main()

        _patches.db.insert_reply_draft.assert_called_once_with(
            inbound_tweet_id="42",
            inbound_author_handle="alice",
            inbound_author_id="user_A",
            inbound_text="Interesting take!",
            our_tweet_id="conv_1",
            our_content_id=1,
            our_post_text="Here is our original post text.",
            draft_text="Thanks for the feedback!",
            relationship_context=None,
            quality_score=None,
            quality_flags=None,
        )

    def test_drafter_receives_correct_arguments(self, _patches):
        """Verify drafter.draft_with_lineage() is called with the right context."""
        mention = _make_mention(
            text="What about edge cases?", author_id="user_A"
        )
        _patches.x_client.get_mentions.return_value = ([mention], USERS_BY_ID)

        main()

        _patches.drafter.draft_with_lineage.assert_called_once_with(
            our_post="Here is our original post text.",
            their_reply="What about edge cases?",
            their_handle="alice",
            self_handle="my_handle",
            person_context=None,
        )


# ---------------------------------------------------------------------------
# 9. Knowledge store integration
# ---------------------------------------------------------------------------


class TestKnowledgeStoreIntegration:
    def test_knowledge_store_initialized_when_embeddings_configured(self, _patches):
        """When config.embeddings is set, knowledge store is initialized."""
        _patches.config.embeddings = SimpleNamespace(
            api_key="voyage-key",
            model="voyage-3-large",
        )

        mention = _make_mention(author_id="user_A")
        _patches.x_client.get_mentions.return_value = ([mention], USERS_BY_ID)

        with (
            patch("poll_replies.VoyageEmbeddings") as MockEmbeddings,
            patch("poll_replies.KnowledgeStore") as MockStore,
        ):
            mock_embedder = MockEmbeddings.return_value
            mock_store = MockStore.return_value
            main()

            MockEmbeddings.assert_called_once_with(
                api_key="voyage-key",
                model="voyage-3-large",
            )
            MockStore.assert_called_once_with(_patches.db.conn, mock_embedder)

    def test_knowledge_ids_stored_when_returned(self, _patches):
        """When draft_with_lineage returns knowledge_ids, they are stored."""
        mention = _make_mention(author_id="user_A")
        _patches.x_client.get_mentions.return_value = ([mention], USERS_BY_ID)
        _patches.db.insert_reply_draft.return_value = 42

        draft_result = SimpleNamespace(
            reply_text="Reply with knowledge",
            knowledge_ids=["k1", "k2", "k3"],
        )
        _patches.drafter.draft_with_lineage.return_value = draft_result

        main()

        _patches.db.insert_reply_knowledge_links.assert_called_once_with(
            42, ["k1", "k2", "k3"]
        )

    def test_knowledge_link_storage_failure_is_non_fatal(self, _patches):
        """When insert_reply_knowledge_links fails, processing continues."""
        mentions = [
            _make_mention(tweet_id="100", author_id="user_A"),
            _make_mention(tweet_id="200", author_id="user_B"),
        ]
        _patches.x_client.get_mentions.return_value = (mentions, USERS_BY_ID)
        _patches.db.insert_reply_draft.side_effect = [42, 43]

        draft_result = SimpleNamespace(
            reply_text="Reply with knowledge",
            knowledge_ids=["k1"],
        )
        _patches.drafter.draft_with_lineage.return_value = draft_result

        # First insert succeeds, second fails
        _patches.db.insert_reply_knowledge_links.side_effect = [
            None,
            sqlite3.Error("DB constraint violation"),
        ]

        main()

        # Both mentions still get drafted and inserted
        assert _patches.drafter.draft_with_lineage.call_count == 2
        assert _patches.db.insert_reply_draft.call_count == 2
        assert _patches.db.insert_reply_knowledge_links.call_count == 2

    def test_no_knowledge_links_stored_when_empty(self, _patches):
        """When knowledge_ids is empty, insert_reply_knowledge_links not called."""
        mention = _make_mention(author_id="user_A")
        _patches.x_client.get_mentions.return_value = ([mention], USERS_BY_ID)

        draft_result = SimpleNamespace(
            reply_text="Reply without knowledge",
            knowledge_ids=[],
        )
        _patches.drafter.draft_with_lineage.return_value = draft_result

        main()

        _patches.db.insert_reply_knowledge_links.assert_not_called()


# ---------------------------------------------------------------------------
# 10. Early exit conditions
# ---------------------------------------------------------------------------


class TestEarlyExitConditions:
    def test_exits_early_when_no_mentions_returned(self, _patches):
        """When get_mentions returns empty list, processing stops early."""
        _patches.x_client.get_mentions.return_value = ([], USERS_BY_ID)

        main()

        _patches.drafter.draft_with_lineage.assert_not_called()
        _patches.db.set_last_mention_id.assert_not_called()
        _patches.update_monitoring.assert_called_with("poll-replies")

    def test_bridge_closed_on_early_exit_no_mentions(self, _patches):
        """When exiting early due to no mentions, bridge is still closed."""
        _patches.config.cultivate = SimpleNamespace(
            enabled=True, db_path="~/.cultivate/cultivate.db",
            forward_mentions=False, enrich_replies=False,
            proactive_review=False, reply_quality_threshold=6.0
        )

        mock_bridge = MagicMock()
        _patches.x_client.get_mentions.return_value = ([], USERS_BY_ID)

        with patch("engagement.cultivate_bridge.CultivateBridge") as MockBridge:
            MockBridge.try_connect.return_value = mock_bridge
            main()

        mock_bridge.close.assert_called_once()

    def test_bridge_closed_on_early_exit_cap_reached(self, _patches):
        """When exiting early due to cap, bridge is closed."""
        _patches.config.cultivate = SimpleNamespace(
            enabled=True, db_path="~/.cultivate/cultivate.db",
            forward_mentions=False, enrich_replies=False,
            proactive_review=False, reply_quality_threshold=6.0
        )
        _patches.db.count_replies_today.return_value = 10
        _patches.config.replies.max_daily_replies = 10

        mock_bridge = MagicMock()

        with patch("engagement.cultivate_bridge.CultivateBridge") as MockBridge:
            MockBridge.try_connect.return_value = mock_bridge
            main()

        mock_bridge.close.assert_called_once()

    def test_bridge_closed_on_mention_fetch_error(self, _patches):
        """When get_mentions raises, bridge is still closed."""
        _patches.config.cultivate = SimpleNamespace(
            enabled=True, db_path="~/.cultivate/cultivate.db",
            forward_mentions=False, enrich_replies=False,
            proactive_review=False, reply_quality_threshold=6.0
        )
        _patches.x_client.get_mentions.side_effect = tweepy.TweepyException("API error")

        mock_bridge = MagicMock()

        with patch("engagement.cultivate_bridge.CultivateBridge") as MockBridge:
            MockBridge.try_connect.return_value = mock_bridge
            main()

        mock_bridge.close.assert_called_once()
