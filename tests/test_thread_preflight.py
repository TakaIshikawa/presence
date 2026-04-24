"""Tests for queued thread publication preflight validation."""

from output.thread_preflight import (
    split_thread_content_for_preflight,
    summarize_thread_preflight_failures,
    validate_platform_threads,
    validate_thread_preflight,
)


def _codes(result):
    return [issue.code for issue in result.issues]


def test_valid_x_thread_payload_passes_unchanged():
    posts = ["First post", "Second post"]

    result = validate_thread_preflight("x", posts)

    assert result.passed is True
    assert result.post_count == 2
    assert posts == ["First post", "Second post"]


def test_valid_bluesky_thread_payload_passes_unchanged():
    posts = ["First post", "Second post"]

    result = validate_thread_preflight("bluesky", posts)

    assert result.passed is True
    assert result.post_count == 2
    assert posts == ["First post", "Second post"]


def test_empty_thread_payload_fails():
    result = validate_thread_preflight("x", [])

    assert result.passed is False
    assert _codes(result) == ["empty_thread"]


def test_empty_marked_thread_post_is_preserved_and_fails():
    posts = split_thread_content_for_preflight("TWEET 1:\nFirst\nTWEET 2:\n\n")

    result = validate_thread_preflight("x", posts)

    assert posts == ["First", ""]
    assert "empty_post" in _codes(result)


def test_over_limit_posts_fail_per_platform():
    x_result = validate_thread_preflight("x", ["x" * 281])
    bsky_result = validate_thread_preflight("bluesky", ["b" * 301])

    assert _codes(x_result) == ["over_limit_post"]
    assert _codes(bsky_result) == ["over_limit_post"]


def test_malformed_order_is_reported_for_indexed_payloads():
    result = validate_thread_preflight(
        "x",
        [
            {"index": 1, "text": "First"},
            {"index": 3, "text": "Third"},
        ],
    )

    assert "out_of_order_post" in _codes(result)
    assert "malformed_order" in _codes(result)


def test_x_reply_payload_requires_parent_metadata():
    result = validate_thread_preflight(
        "x",
        [
            {"index": 1, "text": "First"},
            {"index": 2, "text": "Second"},
        ],
    )

    assert "missing_parent_metadata" in _codes(result)


def test_bluesky_reply_payload_requires_root_and_parent_metadata():
    result = validate_thread_preflight(
        "bluesky",
        [
            {"index": 1, "text": "First"},
            {"index": 2, "text": "Second", "reply_to": {"root": {"uri": "u"}}},
        ],
    )

    assert "missing_reply_metadata" in _codes(result)


def test_valid_indexed_reply_payloads_pass():
    x_result = validate_thread_preflight(
        "x",
        [
            {"index": 1, "text": "First"},
            {"index": 2, "text": "Second", "in_reply_to_tweet_id": "tw-1"},
        ],
    )
    bsky_result = validate_thread_preflight(
        "bluesky",
        [
            {"index": 1, "text": "First"},
            {
                "index": 2,
                "text": "Second",
                "reply_to": {
                    "root": {"uri": "at://root", "cid": "root-cid"},
                    "parent": {"uri": "at://parent", "cid": "parent-cid"},
                },
            },
        ],
    )

    assert x_result.passed is True
    assert bsky_result.passed is True


def test_multi_platform_summary_includes_failed_platform():
    results = validate_platform_threads(
        {
            "x": ["First", ""],
            "bluesky": ["First", "Second"],
        }
    )

    summary = summarize_thread_preflight_failures(results)

    assert results["x"].passed is False
    assert results["bluesky"].passed is True
    assert "x post 2: empty_post" in summary
