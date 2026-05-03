"""Comprehensive tests for custom assertion helpers.

Tests cover:
- All custom assertions
- Error messages and context
- Edge cases and boundary conditions
- Assertion composition
- pytest integration
"""

import pytest
import sqlite3
from unittest.mock import Mock

from tests.helpers.assertions import (
    assert_valid_post,
    assert_valid_thread,
    assert_valid_candidate,
    assert_engagement_above_threshold,
    assert_dedup_detected,
    assert_evaluation_scores_valid,
    assert_database_state,
    assert_no_data_leakage,
    compose_assertions,
)


# --- assert_valid_post tests ---


class TestAssertValidPost:
    """Test assert_valid_post custom assertion."""

    def test_valid_post_passes(self):
        """Test that a valid post passes validation."""
        content = "This is a valid post with good content."
        assert_valid_post(content)  # Should not raise

    def test_empty_post_fails(self):
        """Test that empty post fails with helpful message."""
        with pytest.raises(AssertionError) as exc_info:
            assert_valid_post("")

        error_msg = str(exc_info.value)
        assert "Post content is empty" in error_msg
        assert "Expected: Non-empty content" in error_msg
        assert "Debug hint" in error_msg

    def test_post_over_char_limit_fails(self):
        """Test that post exceeding char limit fails with details."""
        content = "x" * 300  # Over 280 default limit
        with pytest.raises(AssertionError) as exc_info:
            assert_valid_post(content)

        error_msg = str(exc_info.value)
        assert "exceeds character limit" in error_msg
        assert "300 characters" in error_msg
        assert "over by 20" in error_msg

    def test_custom_char_limit(self):
        """Test custom character limit validation."""
        content = "x" * 100
        # Should pass with limit 100
        assert_valid_post(content, char_limit=100)

        # Should fail with limit 50
        with pytest.raises(AssertionError) as exc_info:
            assert_valid_post(content, char_limit=50)

        assert "50 characters" in str(exc_info.value)

    def test_banned_words_detected(self):
        """Test that banned words are detected and reported."""
        content = "This post contains spam and inappropriate content"
        banned = ["spam", "inappropriate"]

        with pytest.raises(AssertionError) as exc_info:
            assert_valid_post(content, banned_words=banned)

        error_msg = str(exc_info.value)
        assert "banned words" in error_msg.lower()
        assert "spam" in error_msg
        assert "inappropriate" in error_msg

    def test_banned_words_case_insensitive(self):
        """Test that banned word detection is case-insensitive."""
        content = "This has SPAM in it"
        with pytest.raises(AssertionError):
            assert_valid_post(content, banned_words=["spam"])

    def test_tweet_markers_detected(self):
        """Test that TWEET markers are detected as formatting errors."""
        content = "TWEET 1:\nThis should not have markers"

        with pytest.raises(AssertionError) as exc_info:
            assert_valid_post(content)

        error_msg = str(exc_info.value)
        assert "TWEET markers" in error_msg
        assert "not properly formatted" in error_msg

    def test_whitespace_issues_detected(self):
        """Test that leading/trailing whitespace is detected."""
        with pytest.raises(AssertionError) as exc_info:
            assert_valid_post("  content with spaces  ")

        assert "whitespace" in str(exc_info.value).lower()

    def test_formatting_check_can_be_disabled(self):
        """Test that formatting checks can be disabled."""
        # Should pass even with markers when formatting check is off
        content = "TWEET 1:\nContent"
        assert_valid_post(content, require_proper_formatting=False)


# --- assert_valid_thread tests ---


class TestAssertValidThread:
    """Test assert_valid_thread custom assertion."""

    def test_valid_thread_passes(self):
        """Test that a valid thread passes validation."""
        tweets = [
            "First tweet in the thread",
            "Second tweet continuing the thought",
            "Third tweet wrapping up",
        ]
        assert_valid_thread(tweets)

    def test_empty_thread_fails(self):
        """Test that empty thread fails with helpful message."""
        with pytest.raises(AssertionError) as exc_info:
            assert_valid_thread([])

        error_msg = str(exc_info.value)
        assert "Thread is empty" in error_msg
        assert "Expected: At least one tweet" in error_msg

    def test_single_tweet_thread_fails(self):
        """Test that single tweet thread fails (below minimum)."""
        with pytest.raises(AssertionError) as exc_info:
            assert_valid_thread(["Only one tweet"], min_tweets=2)

        error_msg = str(exc_info.value)
        assert "Thread too short" in error_msg
        assert "At least 2 tweets" in error_msg

    def test_thread_too_long_fails(self):
        """Test that excessively long thread fails."""
        tweets = [f"Tweet {i}" for i in range(15)]
        with pytest.raises(AssertionError) as exc_info:
            assert_valid_thread(tweets, max_tweets=10)

        error_msg = str(exc_info.value)
        assert "Thread too long" in error_msg
        assert "15 tweets" in error_msg

    def test_individual_tweet_over_limit_fails(self):
        """Test that individual tweet exceeding limit fails."""
        tweets = [
            "Short tweet",
            "x" * 300,  # Over limit
            "Another short tweet",
        ]

        with pytest.raises(AssertionError) as exc_info:
            assert_valid_thread(tweets)

        error_msg = str(exc_info.value)
        assert "Tweet 2 exceeds character limit" in error_msg
        assert "300 characters" in error_msg

    def test_empty_tweet_in_thread_fails(self):
        """Test that empty tweet in thread fails."""
        tweets = ["First tweet", "", "Third tweet"]

        with pytest.raises(AssertionError) as exc_info:
            assert_valid_thread(tweets)

        error_msg = str(exc_info.value)
        assert "Tweet 2 is empty" in error_msg

    def test_total_thread_length_check(self):
        """Test total thread length validation."""
        # Create tweets that individually fit but exceed total limit
        # Use fewer tweets to avoid max_tweets check (15 > 10 default)
        tweets = ["x" * 250 for _ in range(8)]  # 8 * 250 = 2000 chars

        with pytest.raises(AssertionError) as exc_info:
            assert_valid_thread(tweets, total_char_limit=1500, max_tweets=10)

        error_msg = str(exc_info.value)
        assert "total length exceeds limit" in error_msg.lower()
        assert "2000 characters" in error_msg

    def test_duplicate_tweet_detection(self):
        """Test that nearly identical tweets are detected."""
        tweets = [
            "This is a tweet",
            "This is a tweet with minor change",  # Very similar
        ]

        # This should pass - they're different enough
        assert_valid_thread(tweets)

        # But exact duplicates should fail
        duplicate_tweets = [
            "This is exactly the same tweet",
            "This is exactly the same tweet",
        ]

        with pytest.raises(AssertionError) as exc_info:
            assert_valid_thread(duplicate_tweets)

        error_msg = str(exc_info.value)
        assert "nearly identical" in error_msg.lower()

    def test_continuity_check_can_be_disabled(self):
        """Test that continuity checks can be disabled."""
        duplicate_tweets = ["Same content", "Same content"]
        # Should pass when continuity check is off
        assert_valid_thread(duplicate_tweets, check_continuity=False)


# --- assert_valid_candidate tests ---


class TestAssertValidCandidate:
    """Test assert_valid_candidate custom assertion."""

    def test_valid_candidate_passes(self):
        """Test that valid candidate passes validation."""
        candidate = {"content": "Some content", "score": 8.5}
        assert_valid_candidate(candidate)

    def test_none_candidate_fails(self):
        """Test that None candidate fails with helpful message."""
        with pytest.raises(AssertionError) as exc_info:
            assert_valid_candidate(None)

        error_msg = str(exc_info.value)
        assert "Candidate is None" in error_msg

    def test_missing_required_fields_fails(self):
        """Test that missing required fields are reported."""
        candidate = {"content": "Some content"}  # Missing score

        with pytest.raises(AssertionError) as exc_info:
            assert_valid_candidate(candidate)

        error_msg = str(exc_info.value)
        assert "missing required fields" in error_msg.lower()
        assert "score" in error_msg

    def test_custom_required_fields(self):
        """Test custom required fields validation."""
        candidate = {"content": "Text", "score": 8.0}

        # Should fail when checking for additional fields
        with pytest.raises(AssertionError) as exc_info:
            assert_valid_candidate(
                candidate, required_fields=["content", "score", "author"]
            )

        assert "author" in str(exc_info.value)

    def test_none_values_in_required_fields_fails(self):
        """Test that None values in required fields fail."""
        candidate = {"content": None, "score": 8.0}

        with pytest.raises(AssertionError) as exc_info:
            assert_valid_candidate(candidate)

        error_msg = str(exc_info.value)
        assert "None values" in error_msg
        assert "content" in error_msg

    def test_score_not_numeric_fails(self):
        """Test that non-numeric score fails."""
        candidate = {"content": "Text", "score": "8.5"}  # String instead of float

        with pytest.raises(AssertionError) as exc_info:
            assert_valid_candidate(candidate)

        error_msg = str(exc_info.value)
        assert "Score is not numeric" in error_msg

    def test_score_out_of_range_fails(self):
        """Test that score outside valid range fails."""
        # Score too high
        candidate = {"content": "Text", "score": 15.0}
        with pytest.raises(AssertionError) as exc_info:
            assert_valid_candidate(candidate)

        error_msg = str(exc_info.value)
        assert "Score out of valid range" in error_msg
        assert "15.0" in error_msg

        # Score too low
        candidate = {"content": "Text", "score": -1.0}
        with pytest.raises(AssertionError):
            assert_valid_candidate(candidate)

    def test_custom_score_range(self):
        """Test custom score range validation."""
        candidate = {"content": "Text", "rating": 75}

        # Should pass with custom range
        assert_valid_candidate(
            candidate, score_field="rating", min_score=0, max_score=100
        )

        # Should fail with default range
        with pytest.raises(AssertionError):
            assert_valid_candidate(candidate, score_field="rating")


# --- assert_engagement_above_threshold tests ---


class TestAssertEngagementAboveThreshold:
    """Test assert_engagement_above_threshold custom assertion."""

    def test_valid_metrics_pass(self):
        """Test that metrics above threshold pass."""
        metrics = {"likes": 100, "retweets": 50, "replies": 25}
        assert_engagement_above_threshold(metrics, threshold=10.0)

    def test_empty_metrics_fail(self):
        """Test that empty metrics fail."""
        with pytest.raises(AssertionError) as exc_info:
            assert_engagement_above_threshold({}, threshold=10.0)

        assert "empty" in str(exc_info.value).lower()

    def test_specific_metric_below_threshold_fails(self):
        """Test that specific metric below threshold fails."""
        metrics = {"engagement_score": 5.0}

        with pytest.raises(AssertionError) as exc_info:
            assert_engagement_above_threshold(
                metrics, threshold=10.0, metric_name="engagement_score"
            )

        error_msg = str(exc_info.value)
        assert "below threshold" in error_msg.lower()
        assert "engagement_score" in error_msg
        assert "shortfall: 5.00" in error_msg

    def test_missing_metric_fails(self):
        """Test that missing specific metric fails."""
        metrics = {"likes": 100}

        with pytest.raises(AssertionError) as exc_info:
            assert_engagement_above_threshold(
                metrics, threshold=10.0, metric_name="retweets"
            )

        error_msg = str(exc_info.value)
        assert "not found" in error_msg.lower()
        assert "retweets" in error_msg

    def test_all_metrics_checked_when_no_name_specified(self):
        """Test that all metrics are checked when no specific name given."""
        metrics = {"likes": 100, "retweets": 5, "replies": 50}

        with pytest.raises(AssertionError) as exc_info:
            assert_engagement_above_threshold(metrics, threshold=10.0)

        error_msg = str(exc_info.value)
        assert "Some metrics below threshold" in error_msg
        assert "retweets=5" in error_msg
        # likes and replies should not be mentioned as failing


# --- assert_dedup_detected tests ---


class TestAssertDedupDetected:
    """Test assert_dedup_detected custom assertion."""

    def test_exact_match_passes(self):
        """Test that exact duplicates pass exact method."""
        content = "This is duplicate content"
        assert_dedup_detected(content, content, method="exact")

    def test_exact_match_fails_on_difference(self):
        """Test that different content fails exact method."""
        with pytest.raises(AssertionError) as exc_info:
            assert_dedup_detected(
                "Content A", "Content B", method="exact"
            )

        error_msg = str(exc_info.value)
        assert "Exact match deduplication failed" in error_msg
        assert "Content A" in error_msg
        assert "Content B" in error_msg

    def test_sequence_matcher_high_similarity_passes(self):
        """Test that similar content passes sequence matcher."""
        content1 = "This is a post about Python programming"
        content2 = "This is a post about Python development"

        # Should pass with 0.8 threshold (very similar)
        assert_dedup_detected(
            content1, content2, method="sequence_matcher", similarity_threshold=0.7
        )

    def test_sequence_matcher_low_similarity_fails(self):
        """Test that dissimilar content fails sequence matcher."""
        content1 = "Python programming is great"
        content2 = "I love eating pizza"

        with pytest.raises(AssertionError) as exc_info:
            assert_dedup_detected(
                content1, content2, method="sequence_matcher", similarity_threshold=0.8
            )

        error_msg = str(exc_info.value)
        assert "Sequence similarity deduplication failed" in error_msg
        assert "similarity" in error_msg.lower()

    def test_embedding_method_not_implemented(self):
        """Test that embedding method raises NotImplementedError."""
        with pytest.raises(NotImplementedError) as exc_info:
            assert_dedup_detected(
                "Content A", "Content B", method="embedding"
            )

        assert "embedding" in str(exc_info.value).lower()

    def test_invalid_method_raises_valueerror(self):
        """Test that invalid method raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            assert_dedup_detected(
                "Content A", "Content B", method="unknown_method"
            )

        assert "Unknown deduplication method" in str(exc_info.value)


# --- assert_evaluation_scores_valid tests ---


class TestAssertEvaluationScoresValid:
    """Test assert_evaluation_scores_valid custom assertion."""

    def test_valid_scores_pass(self):
        """Test that valid scores pass validation."""
        scores = {"opus": 9.0, "sonnet": 8.0}
        assert_evaluation_scores_valid(scores)

    def test_empty_scores_fail(self):
        """Test that empty scores fail."""
        with pytest.raises(AssertionError) as exc_info:
            assert_evaluation_scores_valid({})

        assert "empty" in str(exc_info.value).lower()

    def test_score_out_of_range_fails(self):
        """Test that scores outside valid range fail."""
        scores = {"opus": 15.0, "sonnet": 8.0}

        with pytest.raises(AssertionError) as exc_info:
            assert_evaluation_scores_valid(scores)

        error_msg = str(exc_info.value)
        assert "out of valid range" in error_msg.lower()
        assert "opus=15.0" in error_msg

    def test_opus_not_higher_than_sonnet_fails(self):
        """Test that Opus score lower than Sonnet fails."""
        scores = {"opus": 7.0, "sonnet": 8.0}

        with pytest.raises(AssertionError) as exc_info:
            assert_evaluation_scores_valid(scores)

        error_msg = str(exc_info.value)
        assert "Opus score not sufficiently higher" in error_msg
        assert "Opus=7.00, Sonnet=8.00" in error_msg

    def test_opus_slightly_higher_fails_without_threshold(self):
        """Test that Opus barely higher than Sonnet fails threshold check."""
        scores = {"opus": 8.2, "sonnet": 8.0}

        with pytest.raises(AssertionError) as exc_info:
            assert_evaluation_scores_valid(scores, min_difference=0.5)

        assert "not sufficiently higher" in str(exc_info.value).lower()

    def test_custom_model_keys(self):
        """Test custom model key names."""
        scores = {"gpt4": 9.0, "gpt35": 7.0}

        assert_evaluation_scores_valid(
            scores,
            opus_key="gpt4",
            sonnet_key="gpt35",
            min_difference=1.0,
        )

    def test_missing_opus_score_fails(self):
        """Test that missing Opus score fails."""
        scores = {"sonnet": 8.0}

        with pytest.raises(AssertionError) as exc_info:
            assert_evaluation_scores_valid(scores)

        error_msg = str(exc_info.value)
        assert "Opus score missing" in error_msg

    def test_can_disable_opus_higher_requirement(self):
        """Test that Opus > Sonnet check can be disabled."""
        scores = {"opus": 7.0, "sonnet": 8.0}
        # Should pass when not requiring Opus higher
        assert_evaluation_scores_valid(scores, require_opus_higher=False)


# --- assert_database_state tests ---


class TestAssertDatabaseState:
    """Test assert_database_state custom assertion."""

    @pytest.fixture
    def db_connection(self):
        """Create in-memory SQLite database for testing."""
        conn = sqlite3.connect(":memory:")
        cursor = conn.cursor()

        # Create test tables
        cursor.execute("CREATE TABLE users (id INTEGER, name TEXT)")
        cursor.execute("CREATE TABLE posts (id INTEGER, content TEXT)")

        # Insert test data
        cursor.execute("INSERT INTO users VALUES (1, 'Alice')")
        cursor.execute("INSERT INTO users VALUES (2, 'Bob')")
        cursor.execute("INSERT INTO posts VALUES (1, 'Post 1')")

        conn.commit()
        return conn

    def test_valid_database_state_passes(self, db_connection):
        """Test that correct database state passes."""
        assert_database_state(
            db_connection,
            expected_tables=["users", "posts"],
            expected_row_counts={"users": 2, "posts": 1},
        )

    def test_missing_table_fails(self, db_connection):
        """Test that missing table is detected."""
        with pytest.raises(AssertionError) as exc_info:
            assert_database_state(
                db_connection,
                expected_tables=["users", "posts", "comments"],
            )

        error_msg = str(exc_info.value)
        assert "Expected tables missing" in error_msg
        assert "comments" in error_msg

    def test_incorrect_row_count_fails(self, db_connection):
        """Test that incorrect row count is detected."""
        with pytest.raises(AssertionError) as exc_info:
            assert_database_state(
                db_connection,
                expected_row_counts={"users": 5},
            )

        error_msg = str(exc_info.value)
        assert "unexpected row count" in error_msg.lower()
        assert "Expected: 5 rows" in error_msg
        assert "Actual: 2 rows" in error_msg
        assert "difference: -3" in error_msg

    def test_works_with_cursor_object(self, db_connection):
        """Test that function works with cursor object."""
        cursor = db_connection.cursor()
        assert_database_state(
            cursor,
            expected_tables=["users"],
            expected_row_counts={"users": 2},
        )

    def test_invalid_connection_fails(self):
        """Test that invalid connection object fails gracefully."""
        with pytest.raises(AssertionError) as exc_info:
            assert_database_state(
                "not a connection",
                expected_tables=["users"],
            )

        assert "doesn't support queries" in str(exc_info.value).lower()


# --- assert_no_data_leakage tests ---


class TestAssertNoDataLeakage:
    """Test assert_no_data_leakage custom assertion."""

    def test_disjoint_sets_pass(self):
        """Test that disjoint train/test sets pass."""
        train = [1, 2, 3, 4, 5]
        test = [6, 7, 8, 9, 10]
        assert_no_data_leakage(train, test)

    def test_empty_train_set_fails(self):
        """Test that empty training set fails."""
        with pytest.raises(AssertionError) as exc_info:
            assert_no_data_leakage([], [1, 2, 3])

        assert "Training data is empty" in str(exc_info.value)

    def test_empty_test_set_fails(self):
        """Test that empty test set fails."""
        with pytest.raises(AssertionError) as exc_info:
            assert_no_data_leakage([1, 2, 3], [])

        assert "Test data is empty" in str(exc_info.value)

    def test_overlap_detected(self):
        """Test that data leakage is detected."""
        train = [1, 2, 3, 4, 5]
        test = [4, 5, 6, 7, 8]

        with pytest.raises(AssertionError) as exc_info:
            assert_no_data_leakage(train, test)

        error_msg = str(exc_info.value)
        assert "Data leakage detected" in error_msg
        assert "2 items appear in both sets" in error_msg
        assert "Overlap: 2" in error_msg

    def test_dict_items_with_identity_key(self):
        """Test leakage detection with dict items using identity key."""
        train = [{"id": 1, "text": "A"}, {"id": 2, "text": "B"}]
        test = [{"id": 2, "text": "B"}, {"id": 3, "text": "C"}]

        with pytest.raises(AssertionError) as exc_info:
            assert_no_data_leakage(train, test, identity_key="id")

        error_msg = str(exc_info.value)
        assert "Data leakage detected" in error_msg
        assert "1 items appear in both sets" in error_msg

    def test_unhashable_items_converted_to_strings(self):
        """Test that unhashable items are handled via string conversion."""
        train = [{"name": "Alice"}, {"name": "Bob"}]
        test = [{"name": "Charlie"}, {"name": "David"}]

        # Should pass - different items
        assert_no_data_leakage(train, test)

    def test_missing_identity_key_fails(self):
        """Test that missing identity key raises error."""
        train = [{"id": 1}, {"id": 2}]
        test = [{"no_id": 3}]

        with pytest.raises(AssertionError) as exc_info:
            assert_no_data_leakage(train, test, identity_key="id")

        assert "Failed to extract identity key" in str(exc_info.value)


# --- compose_assertions tests ---


class TestComposeAssertions:
    """Test compose_assertions helper."""

    def test_all_passing_assertions_succeed(self):
        """Test that all passing assertions succeed."""

        def assertion1():
            assert 1 + 1 == 2

        def assertion2():
            assert "hello".upper() == "HELLO"

        def assertion3():
            assert len([1, 2, 3]) == 3

        # Should not raise
        compose_assertions(assertion1, assertion2, assertion3)

    def test_single_failing_assertion_reports_failure(self):
        """Test that single failing assertion is reported."""

        def passing():
            assert True

        def failing():
            raise AssertionError("This assertion failed")

        with pytest.raises(AssertionError) as exc_info:
            compose_assertions(passing, failing)

        error_msg = str(exc_info.value)
        assert "1/2 failed" in error_msg
        assert "Assertion 2 failed" in error_msg
        assert "This assertion failed" in error_msg

    def test_multiple_failing_assertions_all_reported(self):
        """Test that all failing assertions are collected and reported."""

        def fail1():
            raise AssertionError("First failure")

        def fail2():
            raise AssertionError("Second failure")

        def success():
            assert True

        with pytest.raises(AssertionError) as exc_info:
            compose_assertions(fail1, success, fail2)

        error_msg = str(exc_info.value)
        assert "2/3 failed" in error_msg
        assert "First failure" in error_msg
        assert "Second failure" in error_msg

    def test_unexpected_errors_caught_and_reported(self):
        """Test that unexpected errors are caught and reported."""

        def raise_runtime_error():
            raise RuntimeError("Unexpected error")

        def normal_assertion():
            assert True

        with pytest.raises(AssertionError) as exc_info:
            compose_assertions(normal_assertion, raise_runtime_error)

        error_msg = str(exc_info.value)
        assert "unexpected error" in error_msg.lower()
        assert "RuntimeError" in error_msg

    def test_compose_with_custom_assertions(self):
        """Test composition with custom domain assertions."""

        def check_post():
            assert_valid_post("Valid post content")

        def check_scores():
            assert_evaluation_scores_valid({"opus": 9.0, "sonnet": 7.5})

        # Should pass
        compose_assertions(check_post, check_scores)

        # Now with a failing check
        def check_bad_post():
            assert_valid_post("x" * 500)  # Too long

        with pytest.raises(AssertionError) as exc_info:
            compose_assertions(check_post, check_bad_post, check_scores)

        error_msg = str(exc_info.value)
        assert "1/3 failed" in error_msg
        assert "exceeds character limit" in error_msg


# --- Integration with pytest tests ---


class TestPytestIntegration:
    """Test integration with pytest features."""

    def test_works_with_pytest_raises(self):
        """Test that assertions work within pytest.raises context."""
        with pytest.raises(AssertionError):
            assert_valid_post("x" * 500)

        with pytest.raises(AssertionError):
            assert_valid_thread([])

    def test_works_with_pytest_parametrize(self):
        """Test that assertions work with parametrized tests."""

        @pytest.mark.parametrize(
            "content,should_pass",
            [
                ("Valid post", True),
                ("x" * 500, False),
                ("", False),
            ],
        )
        def check_post(content, should_pass):
            if should_pass:
                assert_valid_post(content)
            else:
                with pytest.raises(AssertionError):
                    assert_valid_post(content)

        # Run the parametrized test cases
        check_post("Valid post", True)
        check_post("x" * 500, False)
        check_post("", False)

    def test_assertion_error_messages_helpful_for_debugging(self):
        """Test that error messages provide debugging context."""
        with pytest.raises(AssertionError) as exc_info:
            assert_valid_candidate({"content": "text", "score": 15.0})

        error_msg = str(exc_info.value)

        # Should have all key components
        assert "Expected:" in error_msg
        assert "Actual:" in error_msg
        assert "Debug hint:" in error_msg

        # Should have specific details
        assert "15.0" in error_msg  # The actual value
        assert "score" in error_msg.lower()


# --- Edge case tests ---


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_unicode_content_handling(self):
        """Test that unicode content is handled correctly."""
        content = "Hello 世界 🌍 émojis and spëcial chars"
        assert_valid_post(content)

    def test_exact_char_limit_boundary(self):
        """Test behavior at exact character limit boundary."""
        # Exactly at limit should pass
        content = "x" * 280
        assert_valid_post(content, char_limit=280)

        # One over should fail
        content = "x" * 281
        with pytest.raises(AssertionError):
            assert_valid_post(content, char_limit=280)

    def test_score_boundary_values(self):
        """Test score validation at boundaries."""
        # Minimum valid score
        assert_valid_candidate({"content": "text", "score": 0.0})

        # Maximum valid score
        assert_valid_candidate({"content": "text", "score": 10.0})

        # Just below minimum
        with pytest.raises(AssertionError):
            assert_valid_candidate({"content": "text", "score": -0.001})

        # Just above maximum
        with pytest.raises(AssertionError):
            assert_valid_candidate({"content": "text", "score": 10.001})

    def test_thread_with_single_very_long_tweet(self):
        """Test thread with one tweet taking most of the budget."""
        tweets = [
            "x" * 279,  # Nearly max for single tweet
            "Short",
        ]
        assert_valid_thread(tweets)

    def test_empty_banned_words_list(self):
        """Test that empty banned words list doesn't cause issues."""
        assert_valid_post("Any content", banned_words=[])

    def test_none_banned_words(self):
        """Test that None banned words is handled."""
        assert_valid_post("Any content", banned_words=None)
