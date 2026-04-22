"""Tests for deterministic X thread validation."""

from synthesis.thread_validator import parse_thread_posts, validate_thread


def _codes(result):
    return [issue.code for issue in result.issues]


def test_valid_thread_passes():
    content = (
        "TWEET 1:\nFirst concrete observation.\n\n"
        "TWEET 2:\nSecond point with a useful detail.\n\n"
        "TWEET 3:\nFinal principle."
    )

    result = validate_thread(content)

    assert result.valid is True
    assert [post.text for post in result.posts] == [
        "First concrete observation.",
        "Second point with a useful detail.",
        "Final principle.",
    ]


def test_inline_tweet_marker_content_is_parsed():
    posts, unnumbered = parse_thread_posts(
        "TWEET 1: First point\nTWEET 2: Second point"
    )

    assert unnumbered == []
    assert [post.text for post in posts] == ["First point", "Second point"]


def test_empty_post_is_rejected():
    result = validate_thread("TWEET 1:\nFirst\nTWEET 2:\n\nTWEET 3:\nThird")

    assert result.valid is False
    assert "empty_post" in _codes(result)
    assert "Tweet 2 is empty" in result.failure_reasons


def test_duplicate_tweets_are_rejected():
    result = validate_thread(
        "TWEET 1:\nSame point\nTWEET 2:\nDifferent point\nTWEET 3:\nSame point"
    )

    assert result.valid is False
    assert "duplicate_tweet" in _codes(result)


def test_invalid_numbering_is_rejected():
    result = validate_thread("TWEET 1:\nFirst\nTWEET 3:\nSkipped")

    assert result.valid is False
    assert "invalid_numbering" in _codes(result)


def test_unnumbered_thread_is_rejected():
    result = validate_thread("First point\nSecond point")

    assert result.valid is False
    assert "invalid_numbering" in _codes(result)


def test_overlong_tweet_is_rejected():
    result = validate_thread(f"TWEET 1:\n{'x' * 281}\nTWEET 2:\nSecond")

    assert result.valid is False
    assert "overlong_tweet" in _codes(result)


def test_missing_continuation_marker_is_rejected_when_marker_style_is_used():
    result = validate_thread(
        "TWEET 1:\n1/ First point\nTWEET 2:\nSecond point\nTWEET 3:\n3/ Final"
    )

    assert result.valid is False
    assert "missing_continuation_marker" in _codes(result)


def test_continuation_markers_are_not_required_when_style_is_unused():
    result = validate_thread("TWEET 1:\nFirst\nTWEET 2:\nSecond")

    assert result.valid is True


def test_invalid_continuation_marker_total_is_rejected():
    result = validate_thread(
        "TWEET 1:\n1/2 First point\nTWEET 2:\n2/2 Second\nTWEET 3:\n3/2 Third"
    )

    assert result.valid is False
    assert "invalid_continuation_marker" in _codes(result)


def test_broken_url_only_tweet_is_rejected():
    result = validate_thread("TWEET 1:\nhttps://\nTWEET 2:\nSecond point")

    assert result.valid is False
    assert "broken_url_only_tweet" in _codes(result)


def test_valid_url_only_tweet_passes_url_check():
    result = validate_thread("TWEET 1:\nhttps://example.com/path\nTWEET 2:\nSecond")

    assert "broken_url_only_tweet" not in _codes(result)
