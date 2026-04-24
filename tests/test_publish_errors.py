"""Tests for stable publish error helpers."""

from __future__ import annotations

from output.publish_errors import classify_publish_error, normalize_error_category


def test_normalize_error_category_accepts_known_values():
    assert normalize_error_category("auth") == "auth"
    assert normalize_error_category("rate_limit") == "rate_limit"
    assert normalize_error_category("not-real") == "unknown"
    assert normalize_error_category(None) == "unknown"


def test_classify_publish_error_categories():
    assert classify_publish_error("429 too many requests") == "rate_limit"
    assert classify_publish_error("status is a duplicate") == "duplicate"
    assert classify_publish_error("unsupported file size for media") == "media"
    assert classify_publish_error("invalid token") == "auth"
    assert classify_publish_error("gateway timeout") == "network"
    assert classify_publish_error("something surprising") == "unknown"
