"""Tests for scripts/send_newsletter.py — weekly newsletter delivery orchestration."""

import logging
import sys
from contextlib import contextmanager
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
    config.timeouts.http_seconds = 30
    return config


def _mock_script_context(config, db):
    """Create a mock context manager that yields (config, db)."""
    @contextmanager
    def _ctx():
        yield (config, db)
    return _ctx


class TestEarlyExits:
    """Tests for conditions that cause main() to return before sending."""

    @pytest.fixture(autouse=True)
    def _set_log_level(self, caplog):
        caplog.set_level(logging.INFO)

    @patch("send_newsletter.update_monitoring")
    @patch("send_newsletter.script_context")
    def test_exits_when_newsletter_disabled(self, mock_ctx, mock_monitoring, caplog):
        config = _make_config(enabled=False)
        db = MagicMock()
        mock_ctx.return_value = _mock_script_context(config, db)()

        main()

        out = caplog.text
        assert "not enabled" in out

    @patch("send_newsletter.update_monitoring")
    @patch("send_newsletter.script_context")
    def test_exits_when_newsletter_is_none(self, mock_ctx, mock_monitoring, caplog):
        config = MagicMock()
        config.newsletter = None
        db = MagicMock()
        mock_ctx.return_value = _mock_script_context(config, db)()

        main()

        out = caplog.text
        assert "not enabled" in out

    @patch("send_newsletter.update_monitoring")
    @patch("send_newsletter.script_context")
    def test_exits_when_api_key_empty(self, mock_ctx, mock_monitoring, caplog):
        config = _make_config(api_key="")
        db = MagicMock()
        mock_ctx.return_value = _mock_script_context(config, db)()

        main()

        out = caplog.text
        assert "API key not configured" in out

    @patch("send_newsletter.update_monitoring")
    @patch("send_newsletter.script_context")
    def test_exits_when_api_key_none(self, mock_ctx, mock_monitoring, caplog):
        config = _make_config(api_key=None)
        db = MagicMock()
        mock_ctx.return_value = _mock_script_context(config, db)()

        main()

        out = caplog.text
        assert "API key not configured" in out


class TestIdempotency:
    """Newsletter must not send more than once per 6-day window."""

    @pytest.fixture(autouse=True)
    def _set_log_level(self, caplog):
        caplog.set_level(logging.INFO)

    @patch("send_newsletter.update_monitoring")
    @patch("send_newsletter.script_context")
    def test_skips_when_sent_recently(self, mock_ctx, mock_monitoring, caplog):
        config = _make_config()
        db = MagicMock()
        db.get_last_newsletter_send.return_value = datetime.now(timezone.utc) - timedelta(days=3)
        mock_ctx.return_value = _mock_script_context(config, db)()

        main()

        out = caplog.text
        assert "already sent" in out
        assert "skipping" in out

    @patch("send_newsletter.update_monitoring")
    @patch("send_newsletter.ButtondownClient")
    @patch("send_newsletter.NewsletterAssembler")
    @patch("send_newsletter.script_context")
    def test_proceeds_when_last_send_over_6_days(
        self, mock_ctx, MockAssembler, MockClient, mock_monitoring
    ):
        config = _make_config()
        db = MagicMock()
        db.get_last_newsletter_send.return_value = datetime.now(timezone.utc) - timedelta(days=7)
        mock_ctx.return_value = _mock_script_context(config, db)()

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
        self, mock_ctx, MockAssembler, MockClient, mock_monitoring
    ):
        config = _make_config()
        db = MagicMock()
        db.get_last_newsletter_send.return_value = None
        mock_ctx.return_value = _mock_script_context(config, db)()

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

    @pytest.fixture(autouse=True)
    def _set_log_level(self, caplog):
        caplog.set_level(logging.INFO)

    @patch("send_newsletter.update_monitoring")
    @patch("send_newsletter.NewsletterAssembler")
    @patch("send_newsletter.script_context")
    def test_exits_when_body_empty(self, mock_ctx, MockAssembler, mock_monitoring, caplog):
        config = _make_config()
        db = MagicMock()
        db.get_last_newsletter_send.return_value = None
        mock_ctx.return_value = _mock_script_context(config, db)()

        content = NewsletterContent(subject="", body_markdown="", source_content_ids=[])
        MockAssembler.return_value.assemble.return_value = content

        main()

        out = caplog.text
        assert "No content published this week" in out

    @patch("send_newsletter.update_monitoring")
    @patch("send_newsletter.NewsletterAssembler")
    @patch("send_newsletter.script_context")
    def test_exits_when_body_whitespace_only(self, mock_ctx, MockAssembler, mock_monitoring, caplog):
        config = _make_config()
        db = MagicMock()
        db.get_last_newsletter_send.return_value = None
        mock_ctx.return_value = _mock_script_context(config, db)()

        content = NewsletterContent(subject="Weekly", body_markdown="   \n  \n  ", source_content_ids=[])
        MockAssembler.return_value.assemble.return_value = content

        main()

        out = caplog.text
        assert "No content published this week" in out


class TestDryRun:
    """--dry-run flag should print content without sending."""

    @pytest.fixture(autouse=True)
    def _set_log_level(self, caplog):
        caplog.set_level(logging.INFO)

    @patch("send_newsletter.update_monitoring")
    @patch("send_newsletter.ButtondownClient")
    @patch("send_newsletter.NewsletterAssembler")
    @patch("send_newsletter.script_context")
    def test_dry_run_prints_content_without_sending(
        self, mock_ctx, MockAssembler, MockClient, mock_monitoring, caplog
    ):
        config = _make_config()
        db = MagicMock()
        db.get_last_newsletter_send.return_value = None
        mock_ctx.return_value = _mock_script_context(config, db)()

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

        out = caplog.text
        assert "DRY RUN" in out
        assert "Great content this week." in out
        MockClient.assert_not_called()


class TestSendSuccess:
    """Successful send should record in DB and call monitoring."""

    @pytest.fixture(autouse=True)
    def _set_log_level(self, caplog):
        caplog.set_level(logging.INFO)

    @patch("send_newsletter.update_monitoring")
    @patch("send_newsletter.ButtondownClient")
    @patch("send_newsletter.NewsletterAssembler")
    @patch("send_newsletter.script_context")
    def test_records_send_in_db(
        self, mock_ctx, MockAssembler, MockClient, mock_monitoring, caplog
    ):
        config = _make_config()
        db = MagicMock()
        db.get_last_newsletter_send.return_value = None
        mock_ctx.return_value = _mock_script_context(config, db)()

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
        out = caplog.text
        assert "Newsletter sent" in out
        assert "https://buttondown.com/issue/42" in out

    @patch("send_newsletter.update_monitoring")
    @patch("send_newsletter.ButtondownClient")
    @patch("send_newsletter.NewsletterAssembler")
    @patch("send_newsletter.script_context")
    def test_records_empty_issue_id_when_none(
        self, mock_ctx, MockAssembler, MockClient, mock_monitoring
    ):
        config = _make_config()
        db = MagicMock()
        db.get_last_newsletter_send.return_value = None
        mock_ctx.return_value = _mock_script_context(config, db)()

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
        self, mock_ctx, MockAssembler, MockClient, mock_monitoring
    ):
        config = _make_config()
        db = MagicMock()
        db.get_last_newsletter_send.return_value = None
        mock_ctx.return_value = _mock_script_context(config, db)()

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

    @pytest.fixture(autouse=True)
    def _set_log_level(self, caplog):
        caplog.set_level(logging.INFO)

    @patch("send_newsletter.update_monitoring")
    @patch("send_newsletter.ButtondownClient")
    @patch("send_newsletter.NewsletterAssembler")
    @patch("send_newsletter.script_context")
    def test_logs_error_on_failure(
        self, mock_ctx, MockAssembler, MockClient, mock_monitoring, caplog
    ):
        config = _make_config()
        db = MagicMock()
        db.get_last_newsletter_send.return_value = None
        mock_ctx.return_value = _mock_script_context(config, db)()

        content = NewsletterContent(
            subject="Update", body_markdown="Content.", source_content_ids=[1]
        )
        MockAssembler.return_value.assemble.return_value = content

        result = NewsletterResult(success=False, error="API rate limit exceeded")
        MockClient.return_value.send.return_value = result
        MockClient.return_value.get_subscriber_count.return_value = 10

        main()

        out = caplog.text
        assert "Send failed" in out
        assert "API rate limit exceeded" in out
        db.insert_newsletter_send.assert_not_called()
