"""Tests for scripts/send_newsletter.py — weekly newsletter delivery orchestration."""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Add scripts/ and src/ to path so we can import send_newsletter and its deps
_project_root = Path(__file__).parent.parent
sys.path.insert(0, str(_project_root / "scripts"))
sys.path.insert(0, str(_project_root / "src"))

from send_newsletter import main, _update_monitoring
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

    @patch("send_newsletter.load_config")
    def test_exits_when_newsletter_disabled(self, mock_load_config, capsys):
        mock_load_config.return_value = _make_config(enabled=False)

        main()

        out = capsys.readouterr().out
        assert "not enabled" in out

    @patch("send_newsletter.load_config")
    def test_exits_when_newsletter_is_none(self, mock_load_config, capsys):
        config = MagicMock()
        config.newsletter = None
        mock_load_config.return_value = config

        main()

        out = capsys.readouterr().out
        assert "not enabled" in out

    @patch("send_newsletter.load_config")
    def test_exits_when_api_key_empty(self, mock_load_config, capsys):
        mock_load_config.return_value = _make_config(api_key="")

        main()

        out = capsys.readouterr().out
        assert "API key not configured" in out

    @patch("send_newsletter.load_config")
    def test_exits_when_api_key_none(self, mock_load_config, capsys):
        mock_load_config.return_value = _make_config(api_key=None)

        main()

        out = capsys.readouterr().out
        assert "API key not configured" in out


class TestIdempotency:
    """Newsletter must not send more than once per 6-day window."""

    @patch("send_newsletter.ButtondownClient")
    @patch("send_newsletter.NewsletterAssembler")
    @patch("send_newsletter.Database")
    @patch("send_newsletter.load_config")
    def test_skips_when_sent_recently(
        self, mock_load_config, MockDB, MockAssembler, MockClient, capsys
    ):
        mock_load_config.return_value = _make_config()

        db = MockDB.return_value
        db.get_last_newsletter_send.return_value = datetime.now(timezone.utc) - timedelta(days=3)

        main()

        out = capsys.readouterr().out
        assert "already sent" in out
        assert "skipping" in out
        MockAssembler.assert_not_called()
        MockClient.assert_not_called()
        db.close.assert_called_once()

    @patch("send_newsletter._update_monitoring")
    @patch("send_newsletter.ButtondownClient")
    @patch("send_newsletter.NewsletterAssembler")
    @patch("send_newsletter.Database")
    @patch("send_newsletter.load_config")
    def test_proceeds_when_last_send_over_6_days(
        self, mock_load_config, MockDB, MockAssembler, MockClient, mock_monitoring
    ):
        mock_load_config.return_value = _make_config()

        db = MockDB.return_value
        db.get_last_newsletter_send.return_value = datetime.now(timezone.utc) - timedelta(days=7)

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

    @patch("send_newsletter._update_monitoring")
    @patch("send_newsletter.ButtondownClient")
    @patch("send_newsletter.NewsletterAssembler")
    @patch("send_newsletter.Database")
    @patch("send_newsletter.load_config")
    def test_proceeds_when_no_previous_send(
        self, mock_load_config, MockDB, MockAssembler, MockClient, mock_monitoring
    ):
        mock_load_config.return_value = _make_config()

        db = MockDB.return_value
        db.get_last_newsletter_send.return_value = None

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
    @patch("send_newsletter.Database")
    @patch("send_newsletter.load_config")
    def test_exits_when_body_empty(self, mock_load_config, MockDB, MockAssembler, capsys):
        mock_load_config.return_value = _make_config()

        db = MockDB.return_value
        db.get_last_newsletter_send.return_value = None

        content = NewsletterContent(subject="", body_markdown="", source_content_ids=[])
        MockAssembler.return_value.assemble.return_value = content

        main()

        out = capsys.readouterr().out
        assert "No content published this week" in out
        db.close.assert_called_once()

    @patch("send_newsletter.NewsletterAssembler")
    @patch("send_newsletter.Database")
    @patch("send_newsletter.load_config")
    def test_exits_when_body_whitespace_only(self, mock_load_config, MockDB, MockAssembler, capsys):
        mock_load_config.return_value = _make_config()

        db = MockDB.return_value
        db.get_last_newsletter_send.return_value = None

        content = NewsletterContent(subject="Weekly", body_markdown="   \n  \n  ", source_content_ids=[])
        MockAssembler.return_value.assemble.return_value = content

        main()

        out = capsys.readouterr().out
        assert "No content published this week" in out


class TestDryRun:
    """--dry-run flag should print content without sending."""

    @patch("send_newsletter.ButtondownClient")
    @patch("send_newsletter.NewsletterAssembler")
    @patch("send_newsletter.Database")
    @patch("send_newsletter.load_config")
    def test_dry_run_prints_content_without_sending(
        self, mock_load_config, MockDB, MockAssembler, MockClient, capsys
    ):
        mock_load_config.return_value = _make_config()

        db = MockDB.return_value
        db.get_last_newsletter_send.return_value = None

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

        out = capsys.readouterr().out
        assert "DRY RUN" in out
        assert "Great content this week." in out
        MockClient.assert_not_called()
        db.close.assert_called_once()


class TestSendSuccess:
    """Successful send should record in DB and call monitoring."""

    @patch("send_newsletter._update_monitoring")
    @patch("send_newsletter.ButtondownClient")
    @patch("send_newsletter.NewsletterAssembler")
    @patch("send_newsletter.Database")
    @patch("send_newsletter.load_config")
    def test_records_send_in_db(
        self, mock_load_config, MockDB, MockAssembler, MockClient, mock_monitoring, capsys
    ):
        mock_load_config.return_value = _make_config()

        db = MockDB.return_value
        db.get_last_newsletter_send.return_value = None

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
        out = capsys.readouterr().out
        assert "Newsletter sent" in out
        assert "https://buttondown.com/issue/42" in out

    @patch("send_newsletter._update_monitoring")
    @patch("send_newsletter.ButtondownClient")
    @patch("send_newsletter.NewsletterAssembler")
    @patch("send_newsletter.Database")
    @patch("send_newsletter.load_config")
    def test_records_empty_issue_id_when_none(
        self, mock_load_config, MockDB, MockAssembler, MockClient, mock_monitoring
    ):
        mock_load_config.return_value = _make_config()

        db = MockDB.return_value
        db.get_last_newsletter_send.return_value = None

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

    @patch("send_newsletter._update_monitoring")
    @patch("send_newsletter.ButtondownClient")
    @patch("send_newsletter.NewsletterAssembler")
    @patch("send_newsletter.Database")
    @patch("send_newsletter.load_config")
    def test_calls_update_monitoring(
        self, mock_load_config, MockDB, MockAssembler, MockClient, mock_monitoring
    ):
        mock_load_config.return_value = _make_config()

        db = MockDB.return_value
        db.get_last_newsletter_send.return_value = None

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

    @patch("send_newsletter._update_monitoring")
    @patch("send_newsletter.ButtondownClient")
    @patch("send_newsletter.NewsletterAssembler")
    @patch("send_newsletter.Database")
    @patch("send_newsletter.load_config")
    def test_logs_error_on_failure(
        self, mock_load_config, MockDB, MockAssembler, MockClient, mock_monitoring, capsys
    ):
        mock_load_config.return_value = _make_config()

        db = MockDB.return_value
        db.get_last_newsletter_send.return_value = None

        content = NewsletterContent(
            subject="Update", body_markdown="Content.", source_content_ids=[1]
        )
        MockAssembler.return_value.assemble.return_value = content

        result = NewsletterResult(success=False, error="API rate limit exceeded")
        MockClient.return_value.send.return_value = result
        MockClient.return_value.get_subscriber_count.return_value = 10

        main()

        out = capsys.readouterr().out
        assert "Send failed" in out
        assert "API rate limit exceeded" in out
        db.insert_newsletter_send.assert_not_called()


class TestUpdateMonitoring:
    """_update_monitoring should be resilient to failures."""

    @patch("send_newsletter.subprocess.run")
    def test_runs_sync_script_when_exists(self, mock_run):
        # The real Path is used; if the script file doesn't exist on disk
        # the function silently skips.  We patch subprocess.run and check
        # that it's called with the right args when the script exists.
        sync_script = Path(__file__).parent.parent / "scripts" / "update_operations_state.py"
        if not sync_script.exists():
            pytest.skip("update_operations_state.py not present on disk")

        _update_monitoring()

        mock_run.assert_called_once()
        call_args = mock_run.call_args
        assert "--operation" in call_args[0][0]
        assert "send-newsletter" in call_args[0][0]

    @patch("send_newsletter.subprocess.run")
    @patch("pathlib.Path.exists", return_value=False)
    def test_noop_when_script_missing(self, mock_exists, mock_run):
        _update_monitoring()

        mock_run.assert_not_called()

    @patch("send_newsletter.subprocess.run", side_effect=OSError("spawn failed"))
    def test_swallows_exceptions(self, mock_run):
        # Should not raise
        _update_monitoring()
