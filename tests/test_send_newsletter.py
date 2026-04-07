"""Tests for scripts/send_newsletter.py — weekly newsletter delivery orchestration."""

import sys
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Add scripts/ and src/ to path so we can import send_newsletter and its deps
_project_root = Path(__file__).parent.parent
sys.path.insert(0, str(_project_root / "scripts"))
sys.path.insert(0, str(_project_root / "src"))

from send_newsletter import main
from output.newsletter import NewsletterContent, NewsletterResult


def _make_config(enabled=True, api_key="test-key"):
    """Build a mock Config with newsletter settings."""
    config = MagicMock()
    config.newsletter.enabled = enabled
    config.newsletter.api_key = api_key
    config.paths.database = ":memory:"
    return config


class TestEarlyExits:
    """Tests for conditions that cause main() to return before sending."""

    @patch("send_newsletter.script_context")
    def test_exits_when_newsletter_disabled(self, mock_script_context, caplog):
        caplog.set_level(logging.INFO)
        config = _make_config(enabled=False)
        db = MagicMock()
        mock_script_context.return_value.__enter__.return_value = (config, db)

        main()

        assert "not enabled" in caplog.text

    @patch("send_newsletter.script_context")
    def test_exits_when_newsletter_is_none(self, mock_script_context, caplog):
        caplog.set_level(logging.INFO)
        config = MagicMock()
        config.newsletter = None
        db = MagicMock()
        mock_script_context.return_value.__enter__.return_value = (config, db)

        main()

        assert "not enabled" in caplog.text

    @patch("send_newsletter.script_context")
    def test_exits_when_api_key_empty(self, mock_script_context, caplog):
        caplog.set_level(logging.INFO)
        config = _make_config(api_key="")
        db = MagicMock()
        mock_script_context.return_value.__enter__.return_value = (config, db)

        main()

        assert "API key not configured" in caplog.text

    @patch("send_newsletter.script_context")
    def test_exits_when_api_key_none(self, mock_script_context, caplog):
        caplog.set_level(logging.INFO)
        config = _make_config(api_key=None)
        db = MagicMock()
        mock_script_context.return_value.__enter__.return_value = (config, db)

        main()

        assert "API key not configured" in caplog.text


class TestIdempotency:
    """Newsletter must not send more than once per 6-day window."""

    @patch("send_newsletter.ButtondownClient")
    @patch("send_newsletter.NewsletterAssembler")
    @patch("send_newsletter.script_context")
    def test_skips_when_sent_recently(
        self, mock_script_context, MockAssembler, MockClient, caplog
    ):
        caplog.set_level(logging.INFO)
        config = _make_config()
        db = MagicMock()
        db.get_last_newsletter_send.return_value = datetime.now(timezone.utc) - timedelta(days=3)
        mock_script_context.return_value.__enter__.return_value = (config, db)

        main()

        assert "already sent" in caplog.text
        assert "skipping" in caplog.text
        MockAssembler.assert_not_called()
        MockClient.assert_not_called()

    @patch("send_newsletter.update_monitoring")
    @patch("send_newsletter.ButtondownClient")
    @patch("send_newsletter.NewsletterAssembler")
    @patch("send_newsletter.script_context")
    def test_proceeds_when_last_send_over_6_days(
        self, mock_script_context, MockAssembler, MockClient, mock_monitoring
    ):
        config = _make_config()
        db = MagicMock()
        db.get_last_newsletter_send.return_value = datetime.now(timezone.utc) - timedelta(days=7)
        mock_script_context.return_value.__enter__.return_value = (config, db)

        content = NewsletterContent(
            subject="Weekly Update",
            body_markdown="# Hello\nSome content",
            source_content_ids=[1, 2],
        )
        MockAssembler.return_value.assemble.return_value = content

        result = NewsletterResult(success=True, issue_id="issue-1", url="https://example.com/1")
        MockClient.return_value.send.return_value = result
        MockClient.return_value.get_subscriber_count.return_value = 10

        main()

        MockClient.return_value.send.assert_called_once_with("Weekly Update", "# Hello\nSome content")

    @patch("send_newsletter.update_monitoring")
    @patch("send_newsletter.ButtondownClient")
    @patch("send_newsletter.NewsletterAssembler")
    @patch("send_newsletter.script_context")
    def test_proceeds_when_no_previous_send(
        self, mock_script_context, MockAssembler, MockClient, mock_monitoring
    ):
        config = _make_config()
        db = MagicMock()
        db.get_last_newsletter_send.return_value = None
        mock_script_context.return_value.__enter__.return_value = (config, db)

        content = NewsletterContent(
            subject="First Issue",
            body_markdown="Content here",
            source_content_ids=[1],
        )
        MockAssembler.return_value.assemble.return_value = content

        result = NewsletterResult(success=True, issue_id="issue-1", url="https://example.com/1")
        MockClient.return_value.send.return_value = result
        MockClient.return_value.get_subscriber_count.return_value = 5

        main()

        MockClient.return_value.send.assert_called_once()


class TestEmptyContent:
    """No newsletter should be sent when there's nothing to report."""

    @patch("send_newsletter.NewsletterAssembler")
    @patch("send_newsletter.script_context")
    def test_exits_when_body_empty(self, mock_script_context, MockAssembler, caplog):
        caplog.set_level(logging.INFO)
        config = _make_config()
        db = MagicMock()
        db.get_last_newsletter_send.return_value = None
        mock_script_context.return_value.__enter__.return_value = (config, db)

        content = NewsletterContent(subject="", body_markdown="", source_content_ids=[])
        MockAssembler.return_value.assemble.return_value = content

        main()

        assert "No content published this week" in caplog.text

    @patch("send_newsletter.NewsletterAssembler")
    @patch("send_newsletter.script_context")
    def test_exits_when_body_whitespace_only(self, mock_script_context, MockAssembler, caplog):
        caplog.set_level(logging.INFO)
        config = _make_config()
        db = MagicMock()
        db.get_last_newsletter_send.return_value = None
        mock_script_context.return_value.__enter__.return_value = (config, db)

        content = NewsletterContent(subject="Weekly", body_markdown="   \n  \n  ", source_content_ids=[])
        MockAssembler.return_value.assemble.return_value = content

        main()

        assert "No content published this week" in caplog.text


class TestDryRun:
    """--dry-run flag should print content without sending."""

    @patch("send_newsletter.ButtondownClient")
    @patch("send_newsletter.NewsletterAssembler")
    @patch("send_newsletter.script_context")
    def test_dry_run_prints_content_without_sending(
        self, mock_script_context, MockAssembler, MockClient, caplog
    ):
        caplog.set_level(logging.INFO)
        config = _make_config()
        db = MagicMock()
        db.get_last_newsletter_send.return_value = None
        mock_script_context.return_value.__enter__.return_value = (config, db)

        content = NewsletterContent(
            subject="Weekly Update",
            body_markdown="# Newsletter\nGreat content this week.",
            source_content_ids=[1, 2],
        )
        MockAssembler.return_value.assemble.return_value = content

        original_argv = sys.argv
        try:
            sys.argv = ["send_newsletter.py", "--dry-run"]
            main()
        finally:
            sys.argv = original_argv

        assert "DRY RUN" in caplog.text
        assert "Great content this week." in caplog.text
        MockClient.assert_not_called()


class TestSendSuccess:
    """Successful send should record in DB and call monitoring."""

    @patch("send_newsletter.update_monitoring")
    @patch("send_newsletter.ButtondownClient")
    @patch("send_newsletter.NewsletterAssembler")
    @patch("send_newsletter.script_context")
    def test_records_send_in_db(
        self, mock_script_context, MockAssembler, MockClient, mock_monitoring, caplog
    ):
        caplog.set_level(logging.INFO)
        config = _make_config()
        db = MagicMock()
        db.get_last_newsletter_send.return_value = None
        mock_script_context.return_value.__enter__.return_value = (config, db)

        content = NewsletterContent(
            subject="Weekly Update",
            body_markdown="# Newsletter\nContent.",
            source_content_ids=[1, 2, 3],
        )
        MockAssembler.return_value.assemble.return_value = content

        result = NewsletterResult(
            success=True, issue_id="issue-42", url="https://buttondown.com/issue/42"
        )
        MockClient.return_value.send.return_value = result
        MockClient.return_value.get_subscriber_count.return_value = 25

        main()

        db.insert_newsletter_send.assert_called_once_with(
            issue_id="issue-42",
            subject="Weekly Update",
            content_ids=[1, 2, 3],
            subscriber_count=25,
        )
        assert "Newsletter sent" in caplog.text
        assert "https://buttondown.com/issue/42" in caplog.text

    @patch("send_newsletter.update_monitoring")
    @patch("send_newsletter.ButtondownClient")
    @patch("send_newsletter.NewsletterAssembler")
    @patch("send_newsletter.script_context")
    def test_records_empty_issue_id_when_none(
        self, mock_script_context, MockAssembler, MockClient, mock_monitoring
    ):
        config = _make_config()
        db = MagicMock()
        db.get_last_newsletter_send.return_value = None
        mock_script_context.return_value.__enter__.return_value = (config, db)

        content = NewsletterContent(
            subject="Update", body_markdown="Content.", source_content_ids=[1]
        )
        MockAssembler.return_value.assemble.return_value = content

        result = NewsletterResult(success=True, issue_id=None, url="https://example.com")
        MockClient.return_value.send.return_value = result
        MockClient.return_value.get_subscriber_count.return_value = 10

        main()

        db.insert_newsletter_send.assert_called_once()
        call_kwargs = db.insert_newsletter_send.call_args
        assert call_kwargs[1]["issue_id"] == ""

    @patch("send_newsletter.update_monitoring")
    @patch("send_newsletter.ButtondownClient")
    @patch("send_newsletter.NewsletterAssembler")
    @patch("send_newsletter.script_context")
    def test_calls_update_monitoring(
        self, mock_script_context, MockAssembler, MockClient, mock_monitoring
    ):
        config = _make_config()
        db = MagicMock()
        db.get_last_newsletter_send.return_value = None
        mock_script_context.return_value.__enter__.return_value = (config, db)

        content = NewsletterContent(
            subject="Update", body_markdown="Content.", source_content_ids=[1]
        )
        MockAssembler.return_value.assemble.return_value = content

        result = NewsletterResult(success=True, issue_id="id-1", url="https://example.com")
        MockClient.return_value.send.return_value = result
        MockClient.return_value.get_subscriber_count.return_value = 5

        main()

        mock_monitoring.assert_called_once()


class TestSendFailure:
    """Failed send should log the error and not record in DB."""

    @patch("send_newsletter.update_monitoring")
    @patch("send_newsletter.ButtondownClient")
    @patch("send_newsletter.NewsletterAssembler")
    @patch("send_newsletter.script_context")
    def test_logs_error_on_failure(
        self, mock_script_context, MockAssembler, MockClient, mock_monitoring, caplog
    ):
        config = _make_config()
        db = MagicMock()
        db.get_last_newsletter_send.return_value = None
        mock_script_context.return_value.__enter__.return_value = (config, db)

        content = NewsletterContent(
            subject="Update", body_markdown="Content.", source_content_ids=[1]
        )
        MockAssembler.return_value.assemble.return_value = content

        result = NewsletterResult(success=False, error="API rate limit exceeded")
        MockClient.return_value.send.return_value = result
        MockClient.return_value.get_subscriber_count.return_value = 10

        main()

        assert "Send failed" in caplog.text
        assert "API rate limit exceeded" in caplog.text
        db.insert_newsletter_send.assert_not_called()
