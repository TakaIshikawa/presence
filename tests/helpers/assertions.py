"""Custom assertion helpers for domain-specific test validations.

This module provides domain-specific assertions that make tests more readable
and maintainable by encapsulating complex validation logic with detailed error
messages.
"""

import re
from difflib import SequenceMatcher
from typing import Any, Callable, Optional


# Constants for validation
DEFAULT_CHAR_LIMIT = 280  # Twitter/X character limit
THREAD_CHAR_LIMIT = 2800  # Approximate thread limit (10 tweets)
MIN_SCORE = 0.0
MAX_SCORE = 10.0
OPUS_SONNET_THRESHOLD = 0.5  # Opus should score at least 0.5 points higher than Sonnet


def assert_valid_post(
    content: str,
    char_limit: int = DEFAULT_CHAR_LIMIT,
    banned_words: Optional[list[str]] = None,
    require_proper_formatting: bool = True,
) -> None:
    """Assert that a post is valid for publication.

    Checks:
    - Character limit compliance
    - No banned words present
    - Proper formatting (no TWEET markers, no trailing whitespace)

    Args:
        content: The post content to validate
        char_limit: Maximum character limit (default: 280)
        banned_words: List of words that shouldn't appear in content
        require_proper_formatting: Check for formatting issues

    Raises:
        AssertionError: If validation fails, with detailed context
    """
    if not content:
        raise AssertionError(
            "Post content is empty.\n"
            "Expected: Non-empty content\n"
            "Actual: Empty string or None\n"
            "Debug hint: Check if content generation completed successfully"
        )

    # Check character limit
    content_length = len(content)
    if content_length > char_limit:
        raise AssertionError(
            f"Post exceeds character limit.\n"
            f"Expected: ≤{char_limit} characters\n"
            f"Actual: {content_length} characters (over by {content_length - char_limit})\n"
            f"Content preview: {content[:100]}...\n"
            f"Debug hint: Consider splitting into a thread or using content shortening"
        )

    # Check for banned words
    if banned_words:
        found_banned = [word for word in banned_words if word.lower() in content.lower()]
        if found_banned:
            raise AssertionError(
                f"Post contains banned words.\n"
                f"Expected: No banned words\n"
                f"Actual: Found {len(found_banned)} banned word(s): {', '.join(found_banned)}\n"
                f"Content: {content}\n"
                f"Debug hint: Review content generation filters and policies"
            )

    # Check proper formatting
    if require_proper_formatting:
        # Check for TWEET markers (should be removed in final output)
        if re.search(r'\bTWEET\s+\d+\s*:', content, re.IGNORECASE):
            raise AssertionError(
                f"Post contains TWEET markers (not properly formatted).\n"
                f"Expected: Clean post without markers\n"
                f"Actual: Contains 'TWEET N:' markers\n"
                f"Content: {content}\n"
                f"Debug hint: TWEET markers should be stripped before final output"
            )

        # Check for trailing/leading whitespace
        if content != content.strip():
            raise AssertionError(
                f"Post has leading or trailing whitespace.\n"
                f"Expected: Trimmed content\n"
                f"Actual: {repr(content)}\n"
                f"Debug hint: Strip whitespace before validation"
            )


def assert_valid_thread(
    tweets: list[str],
    min_tweets: int = 2,
    max_tweets: int = 10,
    char_limit_per_tweet: int = DEFAULT_CHAR_LIMIT,
    total_char_limit: int = THREAD_CHAR_LIMIT,
    check_continuity: bool = True,
) -> None:
    """Assert that a thread is valid.

    Checks:
    - Thread length within bounds
    - Each tweet within character limit
    - Total thread length compliance
    - Content continuity (optional)

    Args:
        tweets: List of tweet strings
        min_tweets: Minimum number of tweets required
        max_tweets: Maximum number of tweets allowed
        char_limit_per_tweet: Character limit per individual tweet
        total_char_limit: Total character limit for entire thread
        check_continuity: Whether to check for narrative continuity

    Raises:
        AssertionError: If validation fails, with detailed context
    """
    if not tweets:
        raise AssertionError(
            "Thread is empty.\n"
            "Expected: At least one tweet\n"
            "Actual: Empty list\n"
            "Debug hint: Check thread generation logic"
        )

    # Check thread length bounds
    tweet_count = len(tweets)
    if tweet_count < min_tweets:
        raise AssertionError(
            f"Thread too short.\n"
            f"Expected: At least {min_tweets} tweets\n"
            f"Actual: {tweet_count} tweet(s)\n"
            f"Debug hint: Thread should have multiple connected ideas"
        )

    if tweet_count > max_tweets:
        raise AssertionError(
            f"Thread too long.\n"
            f"Expected: At most {max_tweets} tweets\n"
            f"Actual: {tweet_count} tweets\n"
            f"Debug hint: Consider condensing content or splitting into multiple threads"
        )

    # Check individual tweet lengths
    for i, tweet in enumerate(tweets, 1):
        tweet_length = len(tweet)
        if tweet_length > char_limit_per_tweet:
            raise AssertionError(
                f"Tweet {i} exceeds character limit.\n"
                f"Expected: ≤{char_limit_per_tweet} characters\n"
                f"Actual: {tweet_length} characters (over by {tweet_length - char_limit_per_tweet})\n"
                f"Tweet content: {tweet[:100]}...\n"
                f"Debug hint: Split long tweet or rephrase to fit limit"
            )

        if not tweet.strip():
            raise AssertionError(
                f"Tweet {i} is empty or whitespace-only.\n"
                f"Expected: Non-empty content\n"
                f"Actual: '{tweet}'\n"
                f"Debug hint: Remove empty tweets or fill with content"
            )

    # Check total thread length
    total_length = sum(len(tweet) for tweet in tweets)
    if total_length > total_char_limit:
        raise AssertionError(
            f"Thread total length exceeds limit.\n"
            f"Expected: ≤{total_char_limit} characters total\n"
            f"Actual: {total_length} characters (over by {total_length - total_char_limit})\n"
            f"Thread: {tweet_count} tweets\n"
            f"Debug hint: Reduce tweet count or shorten individual tweets"
        )

    # Check continuity
    if check_continuity and tweet_count > 1:
        # Simple continuity check: look for transition indicators or topic consistency
        # This is a basic heuristic - more sophisticated checks could use embeddings
        for i in range(len(tweets) - 1):
            current = tweets[i].lower()
            next_tweet = tweets[i + 1].lower()

            # Check if tweets are too similar (copy-paste errors)
            similarity = SequenceMatcher(None, current, next_tweet).ratio()
            if similarity > 0.9:
                raise AssertionError(
                    f"Tweets {i + 1} and {i + 2} are nearly identical.\n"
                    f"Expected: Distinct content in each tweet\n"
                    f"Actual: {similarity * 100:.1f}% similarity\n"
                    f"Tweet {i + 1}: {tweets[i][:80]}...\n"
                    f"Tweet {i + 2}: {tweets[i + 1][:80]}...\n"
                    f"Debug hint: Check for content duplication bugs"
                )


def assert_valid_candidate(
    candidate: dict[str, Any],
    required_fields: Optional[list[str]] = None,
    score_field: str = "score",
    min_score: float = MIN_SCORE,
    max_score: float = MAX_SCORE,
) -> None:
    """Assert that a candidate object is valid.

    Checks:
    - Has all required fields
    - Score is within valid range
    - No None values for required fields

    Args:
        candidate: The candidate dictionary to validate
        required_fields: List of required field names
        score_field: Name of the score field
        min_score: Minimum valid score
        max_score: Maximum valid score

    Raises:
        AssertionError: If validation fails, with detailed context
    """
    if required_fields is None:
        required_fields = ["content", score_field]

    # Check for None candidate
    if candidate is None:
        raise AssertionError(
            "Candidate is None.\n"
            "Expected: Valid candidate object\n"
            "Actual: None\n"
            "Debug hint: Check candidate generation returned a value"
        )

    # Check required fields exist
    missing_fields = [field for field in required_fields if field not in candidate]
    if missing_fields:
        raise AssertionError(
            f"Candidate missing required fields.\n"
            f"Expected: {', '.join(required_fields)}\n"
            f"Actual: Missing {', '.join(missing_fields)}\n"
            f"Available fields: {', '.join(candidate.keys())}\n"
            f"Debug hint: Ensure all required fields are populated during candidate creation"
        )

    # Check for None values in required fields
    none_fields = [field for field in required_fields if candidate.get(field) is None]
    if none_fields:
        raise AssertionError(
            f"Candidate has None values in required fields.\n"
            f"Expected: Non-None values for all required fields\n"
            f"Actual: None values in {', '.join(none_fields)}\n"
            f"Debug hint: Check field population logic"
        )

    # Check score range
    if score_field in candidate:
        score = candidate[score_field]
        if not isinstance(score, (int, float)):
            raise AssertionError(
                f"Score is not numeric.\n"
                f"Expected: int or float\n"
                f"Actual: {type(score).__name__} = {score}\n"
                f"Debug hint: Ensure score is properly parsed as a number"
            )

        if not (min_score <= score <= max_score):
            raise AssertionError(
                f"Score out of valid range.\n"
                f"Expected: {min_score} ≤ score ≤ {max_score}\n"
                f"Actual: {score}\n"
                f"Debug hint: Review scoring logic or adjust valid range"
            )


def assert_engagement_above_threshold(
    metrics: dict[str, float],
    threshold: float,
    metric_name: Optional[str] = None,
) -> None:
    """Assert that engagement metrics meet or exceed threshold.

    Args:
        metrics: Dictionary of metric names to values
        threshold: Minimum acceptable value
        metric_name: Specific metric to check (if None, checks all)

    Raises:
        AssertionError: If validation fails, with detailed context
    """
    if not metrics:
        raise AssertionError(
            "Engagement metrics are empty.\n"
            "Expected: At least one metric\n"
            "Actual: Empty dict\n"
            "Debug hint: Check metrics collection/calculation"
        )

    if metric_name:
        # Check specific metric
        if metric_name not in metrics:
            raise AssertionError(
                f"Metric '{metric_name}' not found.\n"
                f"Expected: Metric exists in {list(metrics.keys())}\n"
                f"Actual: Missing\n"
                f"Debug hint: Verify metric name or ensure it's being tracked"
            )

        value = metrics[metric_name]
        if value < threshold:
            raise AssertionError(
                f"Metric '{metric_name}' below threshold.\n"
                f"Expected: ≥{threshold}\n"
                f"Actual: {value} (shortfall: {threshold - value:.2f})\n"
                f"Debug hint: Content may need optimization to improve engagement"
            )
    else:
        # Check all metrics
        below_threshold = {k: v for k, v in metrics.items() if v < threshold}
        if below_threshold:
            failing_metrics = ', '.join(f"{k}={v:.2f}" for k, v in below_threshold.items())
            raise AssertionError(
                f"Some metrics below threshold.\n"
                f"Expected: All metrics ≥{threshold}\n"
                f"Actual: {len(below_threshold)} metric(s) below: {failing_metrics}\n"
                f"All metrics: {metrics}\n"
                f"Debug hint: Review content performance across all engagement dimensions"
            )


def assert_dedup_detected(
    content1: str,
    content2: str,
    method: str = "sequence_matcher",
    similarity_threshold: float = 0.8,
) -> None:
    """Assert that deduplication correctly detects similar content.

    Args:
        content1: First piece of content
        content2: Second piece of content
        method: Deduplication method ('sequence_matcher', 'embedding', 'exact')
        similarity_threshold: Minimum similarity to consider duplicate

    Raises:
        AssertionError: If content should be duplicates but isn't detected as such
    """
    if method == "exact":
        if content1 != content2:
            raise AssertionError(
                f"Exact match deduplication failed.\n"
                f"Expected: Identical content\n"
                f"Actual: Content differs\n"
                f"Content 1: {content1[:100]}...\n"
                f"Content 2: {content2[:100]}...\n"
                f"Debug hint: Contents should be exactly the same"
            )

    elif method == "sequence_matcher":
        similarity = SequenceMatcher(None, content1, content2).ratio()
        if similarity < similarity_threshold:
            raise AssertionError(
                f"Sequence similarity deduplication failed.\n"
                f"Expected: Similarity ≥{similarity_threshold * 100:.1f}%\n"
                f"Actual: {similarity * 100:.1f}% similarity\n"
                f"Content 1: {content1[:100]}...\n"
                f"Content 2: {content2[:100]}...\n"
                f"Debug hint: Contents not similar enough to be considered duplicates"
            )

    elif method == "embedding":
        # For embedding-based dedup, we expect the calling code to pass similarity
        # This is a placeholder that assumes semantic similarity
        raise NotImplementedError(
            "Embedding-based dedup requires similarity score to be passed.\n"
            "Use assert_dedup_detected with method='sequence_matcher' or implement\n"
            "custom validation with embedding similarity scores."
        )

    else:
        raise ValueError(
            f"Unknown deduplication method: {method}\n"
            f"Supported methods: 'exact', 'sequence_matcher', 'embedding'"
        )


def assert_evaluation_scores_valid(
    scores: dict[str, float],
    require_opus_higher: bool = True,
    opus_key: str = "opus",
    sonnet_key: str = "sonnet",
    min_difference: float = OPUS_SONNET_THRESHOLD,
) -> None:
    """Assert that evaluation scores are valid.

    Checks:
    - All scores in valid range
    - Opus score higher than Sonnet (if required)

    Args:
        scores: Dictionary mapping model name to score
        require_opus_higher: Whether Opus must score higher than Sonnet
        opus_key: Key for Opus score in dict
        sonnet_key: Key for Sonnet score in dict
        min_difference: Minimum required difference between Opus and Sonnet

    Raises:
        AssertionError: If validation fails, with detailed context
    """
    if not scores:
        raise AssertionError(
            "Evaluation scores are empty.\n"
            "Expected: At least one model score\n"
            "Actual: Empty dict\n"
            "Debug hint: Check evaluation completed successfully"
        )

    # Check all scores in valid range
    invalid_scores = {
        k: v for k, v in scores.items()
        if not isinstance(v, (int, float)) or not (MIN_SCORE <= v <= MAX_SCORE)
    }
    if invalid_scores:
        invalid_list = ', '.join(f"{k}={v}" for k, v in invalid_scores.items())
        raise AssertionError(
            f"Some scores out of valid range.\n"
            f"Expected: {MIN_SCORE} ≤ score ≤ {MAX_SCORE}\n"
            f"Actual: Invalid scores: {invalid_list}\n"
            f"All scores: {scores}\n"
            f"Debug hint: Check scoring logic produces values in correct range"
        )

    # Check Opus vs Sonnet ordering
    if require_opus_higher:
        if opus_key not in scores:
            raise AssertionError(
                f"Opus score missing.\n"
                f"Expected: '{opus_key}' in scores\n"
                f"Actual: Available keys: {list(scores.keys())}\n"
                f"Debug hint: Ensure Opus evaluation ran successfully"
            )

        if sonnet_key not in scores:
            raise AssertionError(
                f"Sonnet score missing.\n"
                f"Expected: '{sonnet_key}' in scores\n"
                f"Actual: Available keys: {list(scores.keys())}\n"
                f"Debug hint: Ensure Sonnet evaluation ran successfully"
            )

        opus_score = scores[opus_key]
        sonnet_score = scores[sonnet_key]

        if opus_score < sonnet_score + min_difference:
            difference = opus_score - sonnet_score
            raise AssertionError(
                f"Opus score not sufficiently higher than Sonnet.\n"
                f"Expected: Opus ≥ Sonnet + {min_difference} (Opus ≥ {sonnet_score + min_difference:.2f})\n"
                f"Actual: Opus={opus_score:.2f}, Sonnet={sonnet_score:.2f}, diff={difference:.2f}\n"
                f"Debug hint: Opus should consistently score higher; check evaluation calibration"
            )


def assert_database_state(
    db_connection: Any,
    expected_tables: Optional[list[str]] = None,
    expected_row_counts: Optional[dict[str, int]] = None,
) -> None:
    """Assert that database is in expected state.

    Args:
        db_connection: Database connection object (should have execute method)
        expected_tables: List of table names that should exist
        expected_row_counts: Dict mapping table names to expected row counts

    Raises:
        AssertionError: If validation fails, with detailed context
    """
    # Check for execute method
    if not hasattr(db_connection, 'execute'):
        # Try to get a cursor
        if hasattr(db_connection, 'cursor'):
            cursor = db_connection.cursor()
        else:
            raise AssertionError(
                "Database connection doesn't support queries.\n"
                "Expected: Object with .execute() or .cursor() method\n"
                f"Actual: {type(db_connection).__name__}\n"
                f"Debug hint: Pass a valid database connection or cursor"
            )
    else:
        cursor = db_connection

    # Check expected tables exist
    if expected_tables:
        # Get list of existing tables (SQLite-specific query)
        result = cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        existing_tables = {row[0] for row in result.fetchall()}

        missing_tables = set(expected_tables) - existing_tables
        if missing_tables:
            raise AssertionError(
                f"Expected tables missing from database.\n"
                f"Expected: {', '.join(sorted(expected_tables))}\n"
                f"Missing: {', '.join(sorted(missing_tables))}\n"
                f"Existing: {', '.join(sorted(existing_tables))}\n"
                f"Debug hint: Run migrations or check table creation logic"
            )

    # Check row counts
    if expected_row_counts:
        for table, expected_count in expected_row_counts.items():
            result = cursor.execute(f"SELECT COUNT(*) FROM {table}")
            actual_count = result.fetchone()[0]

            if actual_count != expected_count:
                raise AssertionError(
                    f"Table '{table}' has unexpected row count.\n"
                    f"Expected: {expected_count} rows\n"
                    f"Actual: {actual_count} rows (difference: {actual_count - expected_count:+d})\n"
                    f"Debug hint: Check data insertion/deletion logic or adjust expected count"
                )


def assert_no_data_leakage(
    train_data: list[Any],
    test_data: list[Any],
    identity_key: Optional[str] = None,
) -> None:
    """Assert that there's no data leakage between train and test sets.

    Args:
        train_data: Training dataset
        test_data: Test dataset
        identity_key: Key to extract identity from dict items (if None, uses items directly)

    Raises:
        AssertionError: If any test data appears in training data
    """
    if not train_data:
        raise AssertionError(
            "Training data is empty.\n"
            "Expected: Non-empty training set\n"
            "Actual: Empty list\n"
            "Debug hint: Check data loading for training set"
        )

    if not test_data:
        raise AssertionError(
            "Test data is empty.\n"
            "Expected: Non-empty test set\n"
            "Actual: Empty list\n"
            "Debug hint: Check data loading for test set"
        )

    # Extract identities
    if identity_key:
        try:
            train_ids = {item[identity_key] for item in train_data}
            test_ids = {item[identity_key] for item in test_data}
        except (KeyError, TypeError) as e:
            raise AssertionError(
                f"Failed to extract identity key '{identity_key}'.\n"
                f"Error: {e}\n"
                f"Debug hint: Ensure all items have the specified key and are dict-like"
            )
    else:
        # Use items directly (they should be hashable)
        try:
            train_ids = set(train_data)
            test_ids = set(test_data)
        except TypeError:
            # Items not hashable, convert to strings
            train_ids = {str(item) for item in train_data}
            test_ids = {str(item) for item in test_data}

    # Find overlap
    overlap = train_ids & test_ids
    if overlap:
        sample_overlap = list(overlap)[:5]  # Show up to 5 examples
        raise AssertionError(
            f"Data leakage detected between train and test sets.\n"
            f"Expected: Disjoint train and test sets\n"
            f"Actual: {len(overlap)} items appear in both sets\n"
            f"Sample overlapping items: {sample_overlap}\n"
            f"Train size: {len(train_ids)}, Test size: {len(test_ids)}, Overlap: {len(overlap)}\n"
            f"Debug hint: Ensure proper train/test split with no shared items"
        )


def compose_assertions(*assertions: Callable[[], None]) -> None:
    """Compose multiple assertions into a single check.

    All assertions are executed, and all failures are collected and reported together.

    Args:
        *assertions: Callable assertions that raise AssertionError on failure

    Raises:
        AssertionError: If any assertion fails, with combined error messages
    """
    failures = []

    for i, assertion in enumerate(assertions, 1):
        try:
            assertion()
        except AssertionError as e:
            failures.append(f"Assertion {i} failed: {e}")
        except Exception as e:
            failures.append(f"Assertion {i} raised unexpected error: {type(e).__name__}: {e}")

    if failures:
        combined_message = "\n\n".join(failures)
        raise AssertionError(
            f"Composed assertion failures ({len(failures)}/{len(assertions)} failed):\n\n"
            f"{combined_message}\n\n"
            f"Debug hint: Fix each failing assertion in sequence"
        )
