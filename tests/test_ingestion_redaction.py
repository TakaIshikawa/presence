"""Tests for ingestion redaction helpers."""

from ingestion.redaction import Redactor, redact_text


def test_redacts_common_secret_shapes_while_preserving_context():
    text = (
        "Rotate token=ghp_abcdefghijklmnopqrstuvwxyz123456 and "
        "call Authorization: Bearer abcdefghijklmnopqrstuvwxyz. "
        "Contact dev@example.com after cleanup."
    )

    redacted = redact_text(text)

    assert "Rotate token=[REDACTED_SECRET]" in redacted
    assert "Bearer abcdefghijklmnopqrstuvwxyz" not in redacted
    assert "[REDACTED_BEARER]" in redacted
    assert "dev@example.com" not in redacted
    assert "[REDACTED_EMAIL]" in redacted


def test_redacts_private_keys_and_local_machine_paths():
    text = (
        "Read /Users/taka/Project/app/.env and "
        "-----BEGIN PRIVATE KEY-----\nabc123\n-----END PRIVATE KEY-----"
    )

    redacted = redact_text(text)

    assert "/Users/taka" not in redacted
    assert "[REDACTED_PATH]" in redacted
    assert "BEGIN PRIVATE KEY" not in redacted
    assert "[REDACTED_PRIVATE_KEY]" in redacted


def test_custom_pattern_can_preserve_surrounding_text():
    redactor = Redactor(
        [
            {
                "name": "ticket",
                "pattern": r"(ticket-)(\d+)",
                "replacement": r"\1[REDACTED_TICKET]",
            }
        ]
    )

    assert redactor.redact("fixed ticket-1234 today") == "fixed ticket-[REDACTED_TICKET] today"


def test_empty_text_is_returned_unchanged():
    assert redact_text("") == ""
