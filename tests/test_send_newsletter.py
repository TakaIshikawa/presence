"""Tests for scripts/send_newsletter.py — weekly newsletter delivery orchestration."""

import json
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
from output.newsletter import (
    NewsletterContent,
    NewsletterResult,
    NewsletterSubjectCandidate,
)
from output.link_health import LinkCheckResult, LinkHealthReport, LinkOccurrence


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
            metadata={"suppressed_content_ids": [9, 10], "repeat_lookback_weeks": 4},
        )
        MockAssembler.return_value.assemble.return_value = content

        original_argv = sys.argv
        try:
            sys.argv = [
                "send_newsletter.py",
                "--dry-run",
                "--repeat-lookback-weeks",
                "4",
            ]
            main()
        finally:
            sys.argv = original_argv

        out = caplog.text
        assert "DRY RUN" in out
        assert "suppressed_content_ids" in out
        assert "9" in out
        assert "10" in out
        assert "Great content this week." in out
        assert MockAssembler.call_args.kwargs["repeat_lookback_weeks"] == 4
        MockClient.assert_not_called()

    @patch("send_newsletter.update_monitoring")
    @patch("send_newsletter.ButtondownClient")
    @patch("send_newsletter.NewsletterAssembler")
    @patch("send_newsletter.script_context")
    def test_repeat_lookback_weeks_defaults_to_eight(
        self, mock_ctx, MockAssembler, MockClient, mock_monitoring
    ):
        config = _make_config()
        db = MagicMock()
        db.get_last_newsletter_send.return_value = None
        mock_ctx.return_value = _mock_script_context(config, db)()
        MockAssembler.return_value.assemble.return_value = NewsletterContent(
            subject="Weekly Update",
            body_markdown="Content.",
            source_content_ids=[1],
        )

        original_argv = sys.argv
        try:
            sys.argv = ["send_newsletter.py", "--dry-run"]
            main()
        finally:
            sys.argv = original_argv

        assert MockAssembler.call_args.kwargs["repeat_lookback_weeks"] == 8
        MockClient.assert_not_called()


class TestPreviewOut:
    """--preview-out writes a review artifact without sending."""

    @patch("send_newsletter.update_monitoring")
    @patch("send_newsletter.ButtondownClient")
    @patch("send_newsletter.NewsletterAssembler")
    @patch("send_newsletter.script_context")
    def test_preview_json_writes_artifact_without_buttondown_or_send_record(
        self, mock_ctx, MockAssembler, MockClient, mock_monitoring, tmp_path
    ):
        config = _make_config(api_key="")
        config.newsletter.utm_source = "newsletter"
        config.newsletter.utm_medium = "email"
        config.newsletter.utm_campaign_template = "weekly-{week_end_compact}"
        db = MagicMock()
        db.get_last_newsletter_send.return_value = datetime.now(timezone.utc)
        mock_ctx.return_value = _mock_script_context(config, db)()

        content = NewsletterContent(
            subject="Default subject",
            body_markdown="# Newsletter\nPreview body.",
            source_content_ids=[10, 20],
            metadata={"utm_campaign": "weekly-20260423"},
            subject_candidates=[
                NewsletterSubjectCandidate(
                subject="Candidate subject",
                score=9.1,
                rationale="specific",
                source="heuristic",
                metadata={
                    "reason": "title",
                    "history": {
                        "bonus": 0.85,
                        "matched_tokens": ["candidate", "subject"],
                        "matched_subjects": [{"subject": "Candidate subject"}],
                        "baseline_performance": 34.2,
                        "profiled_subjects": 4,
                    },
                },
            )
        ],
        )
        MockAssembler.return_value.assemble.return_value = content
        preview_path = tmp_path / "newsletter-preview.json"

        original_argv = sys.argv
        try:
            sys.argv = ["send_newsletter.py", "--preview-out", str(preview_path)]
            main()
        finally:
            sys.argv = original_argv

        payload = json.loads(preview_path.read_text())
        assert payload["selected_subject"] == "Candidate subject"
        assert payload["body_markdown"] == "# Newsletter\nPreview body."
        assert payload["source_content_ids"] == [10, 20]
        assert payload["subject_candidates"][0]["score"] == 9.1
        assert payload["subject_selection"]["selected_candidate"]["subject"] == "Candidate subject"
        assert payload["subject_selection"]["history"]["matched_tokens"] == [
            "candidate",
            "subject",
        ]
        assert payload["week_range"]["start"]
        assert payload["week_range"]["end"]
        assert payload["utm_metadata"] == {
            "utm_source": "newsletter",
            "utm_medium": "email",
            "utm_campaign_template": "weekly-{week_end_compact}",
            "utm_campaign": "weekly-20260423",
        }
        MockClient.assert_not_called()
        db.insert_newsletter_send.assert_not_called()
        db.insert_newsletter_subject_candidates.assert_not_called()

    @patch("send_newsletter.update_monitoring")
    @patch("send_newsletter.ButtondownClient")
    @patch("send_newsletter.NewsletterAssembler")
    @patch("send_newsletter.script_context")
    def test_preview_markdown_uses_markdown_extension(
        self, mock_ctx, MockAssembler, MockClient, mock_monitoring, tmp_path
    ):
        config = _make_config()
        db = MagicMock()
        db.get_last_newsletter_send.return_value = None
        mock_ctx.return_value = _mock_script_context(config, db)()
        MockAssembler.return_value.assemble.return_value = NewsletterContent(
            subject="Markdown subject",
            body_markdown="Markdown body.",
            source_content_ids=[1],
        )
        preview_path = tmp_path / "newsletter-preview.md"

        original_argv = sys.argv
        try:
            sys.argv = ["send_newsletter.py", "--preview-out", str(preview_path)]
            main()
        finally:
            sys.argv = original_argv

        rendered = preview_path.read_text()
        assert rendered.startswith("# Newsletter Preview")
        assert "## Selected Subject" in rendered
        assert "## Subject Selection" in rendered
        assert "Markdown subject" in rendered
        assert "## Body" in rendered
        assert "Markdown body." in rendered
        MockClient.assert_not_called()
        db.insert_newsletter_send.assert_not_called()

    @patch("send_newsletter.update_monitoring")
    @patch("send_newsletter.ButtondownClient")
    @patch("send_newsletter.NewsletterAssembler")
    @patch("send_newsletter.script_context")
    def test_preview_persists_candidates_only_with_explicit_flag(
        self, mock_ctx, MockAssembler, MockClient, mock_monitoring, tmp_path
    ):
        config = _make_config()
        config.newsletter.subject_override = "Manual preview subject"
        db = MagicMock()
        db.get_last_newsletter_send.return_value = None
        mock_ctx.return_value = _mock_script_context(config, db)()
        MockAssembler.return_value.assemble.return_value = NewsletterContent(
            subject="Default subject",
            body_markdown="Preview body.",
            source_content_ids=[5],
            subject_candidates=[
                NewsletterSubjectCandidate(
                    subject="Generated subject",
                    score=7.0,
                    rationale="generated",
                )
            ],
        )
        preview_path = tmp_path / "newsletter-preview.json"

        original_argv = sys.argv
        try:
            sys.argv = [
                "send_newsletter.py",
                "--preview-out",
                str(preview_path),
                "--persist-candidates",
            ]
            main()
        finally:
            sys.argv = original_argv

        db.insert_newsletter_subject_candidates.assert_called_once()
        stored_candidates = db.insert_newsletter_subject_candidates.call_args.args[0]
        assert stored_candidates[0].subject == "Manual preview subject"
        assert stored_candidates[0].source == "manual"
        assert db.insert_newsletter_subject_candidates.call_args.kwargs[
            "selected_subject"
        ] == "Manual preview subject"
        MockClient.assert_not_called()
        db.insert_newsletter_send.assert_not_called()


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
            metadata={
                "subject_selection": {
                    "selected_subject": "Weekly Update",
                    "manual_subject": "",
                    "selected_candidate": {},
                    "ranked_candidates": [],
                    "alternatives": [],
                }
            },
        )
        out = caplog.text
        assert "Newsletter sent" in out
        assert "https://buttondown.com/issue/42" in out

    @patch("send_newsletter.update_monitoring")
    @patch("send_newsletter.ButtondownClient")
    @patch("send_newsletter.NewsletterAssembler")
    @patch("send_newsletter.script_context")
    def test_sends_top_subject_candidate_and_stores_scores(
        self, mock_ctx, MockAssembler, MockClient, mock_monitoring
    ):
        config = _make_config()
        db = MagicMock()
        db.get_last_newsletter_send.return_value = None
        mock_ctx.return_value = _mock_script_context(config, db)()

        content = NewsletterContent(
            subject="Building with AI — Week of Apr 16",
            body_markdown="Content.",
            source_content_ids=[1],
            subject_candidates=[
                NewsletterSubjectCandidate(
                    subject="Shipping Better AI Tools",
                    score=8.5,
                    rationale="issue-specific",
                ),
                NewsletterSubjectCandidate(
                    subject="Building with AI — Week of Apr 16",
                    score=6.75,
                    rationale="default format",
                ),
            ],
        )
        MockAssembler.return_value.assemble.return_value = content
        MockClient.return_value.send.return_value = NewsletterResult(
            success=True,
            issue_id="issue-1",
            url="https://example.com",
        )
        MockClient.return_value.get_subscriber_count.return_value = 10

        main()

        MockClient.return_value.send.assert_called_once_with(
            "Shipping Better AI Tools",
            "Content.",
        )
        db.insert_newsletter_subject_candidates.assert_called_once()
        storage_kwargs = db.insert_newsletter_subject_candidates.call_args.kwargs
        assert storage_kwargs["selected_subject"] == "Shipping Better AI Tools"
        assert storage_kwargs["content_ids"] == [1]
        db.insert_newsletter_send.assert_called_once()
        send_kwargs = db.insert_newsletter_send.call_args.kwargs
        assert send_kwargs["subject"] == "Shipping Better AI Tools"
        assert send_kwargs["metadata"]["subject_selection"]["selected_subject"] == (
            "Shipping Better AI Tools"
        )
        assert send_kwargs["metadata"]["subject_selection"]["alternatives"][0][
            "subject"
        ] == "Building with AI — Week of Apr 16"

    @patch("send_newsletter.update_monitoring")
    @patch("send_newsletter.ButtondownClient")
    @patch("send_newsletter.NewsletterAssembler")
    @patch("send_newsletter.script_context")
    def test_manual_subject_override_wins_over_candidates(
        self, mock_ctx, MockAssembler, MockClient, mock_monitoring
    ):
        config = _make_config()
        config.newsletter.subject_override = "Manual subject"
        db = MagicMock()
        db.get_last_newsletter_send.return_value = None
        mock_ctx.return_value = _mock_script_context(config, db)()

        content = NewsletterContent(
            subject="Building with AI — Week of Apr 16",
            body_markdown="Content.",
            source_content_ids=[1],
            subject_candidates=[
                NewsletterSubjectCandidate(
                    subject="Shipping Better AI Tools",
                    score=8.5,
                    rationale="issue-specific",
                ),
            ],
        )
        MockAssembler.return_value.assemble.return_value = content
        MockClient.return_value.send.return_value = NewsletterResult(
            success=True,
            issue_id="issue-1",
            url="https://example.com",
        )
        MockClient.return_value.get_subscriber_count.return_value = 10

        main()

        MockClient.return_value.send.assert_called_once_with(
            "Manual subject",
            "Content.",
        )
        stored_candidates = db.insert_newsletter_subject_candidates.call_args.args[0]
        assert stored_candidates[0].subject == "Manual subject"
        assert stored_candidates[0].source == "manual"
        send_kwargs = db.insert_newsletter_send.call_args.kwargs
        assert send_kwargs["subject"] == "Manual subject"
        assert send_kwargs["metadata"]["subject_selection"]["selected_subject"] == "Manual subject"
        assert send_kwargs["metadata"]["subject_selection"]["selected_candidate"]["source"] == "manual"

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


class TestLinkChecks:
    """--check-links validates newsletter links before Buttondown delivery."""

    @pytest.fixture(autouse=True)
    def _set_log_level(self, caplog):
        caplog.set_level(logging.INFO)

    def _failing_report(self):
        return LinkHealthReport(
            checked=[
                LinkCheckResult(
                    url="https://example.com/broken",
                    ok=False,
                    status_code=500,
                    error="HTTP 500",
                    occurrences=[
                        LinkOccurrence(
                            url="https://example.com/broken",
                            normalized_url="https://example.com/broken",
                            label="broken",
                            line=1,
                            column=1,
                        )
                    ],
                )
            ]
        )

    @patch("send_newsletter.update_monitoring")
    @patch("send_newsletter.LinkHealthChecker")
    @patch("send_newsletter.ButtondownClient")
    @patch("send_newsletter.NewsletterAssembler")
    @patch("send_newsletter.script_context")
    def test_check_links_warn_mode_reports_failures_and_still_sends(
        self,
        mock_ctx,
        MockAssembler,
        MockClient,
        MockChecker,
        mock_monitoring,
        caplog,
    ):
        config = _make_config()
        db = MagicMock()
        db.get_last_newsletter_send.return_value = None
        mock_ctx.return_value = _mock_script_context(config, db)()
        MockAssembler.return_value.assemble.return_value = NewsletterContent(
            subject="Update",
            body_markdown="[broken](https://example.com/broken)",
            source_content_ids=[1],
        )
        MockChecker.return_value.check_markdown.return_value = self._failing_report()
        MockClient.return_value.get_subscriber_count.return_value = 10
        MockClient.return_value.send.return_value = NewsletterResult(
            success=True,
            issue_id="issue-1",
            url="https://buttondown.com/issue/1",
        )

        original_argv = sys.argv
        try:
            sys.argv = [
                "send_newsletter.py",
                "--check-links",
                "--link-check-mode",
                "warn",
            ]
            main()
        finally:
            sys.argv = original_argv

        MockChecker.assert_called_once_with(timeout=30)
        MockChecker.return_value.check_markdown.assert_called_once_with(
            "[broken](https://example.com/broken)"
        )
        MockClient.return_value.send.assert_called_once_with(
            "Update",
            "[broken](https://example.com/broken)",
        )
        assert "link check found 1 failing link" in caplog.text
        send_metadata = db.insert_newsletter_send.call_args.kwargs["metadata"]
        assert send_metadata["link_health"]["ok"] is False
        assert send_metadata["link_health"]["failures"][0]["url"] == (
            "https://example.com/broken"
        )

    @patch("send_newsletter.update_monitoring")
    @patch("send_newsletter.LinkHealthChecker")
    @patch("send_newsletter.ButtondownClient")
    @patch("send_newsletter.NewsletterAssembler")
    @patch("send_newsletter.script_context")
    def test_check_links_block_mode_aborts_before_buttondown_delivery(
        self,
        mock_ctx,
        MockAssembler,
        MockClient,
        MockChecker,
        mock_monitoring,
        caplog,
    ):
        config = _make_config()
        db = MagicMock()
        db.get_last_newsletter_send.return_value = None
        mock_ctx.return_value = _mock_script_context(config, db)()
        MockAssembler.return_value.assemble.return_value = NewsletterContent(
            subject="Update",
            body_markdown="[broken](https://example.com/broken)",
            source_content_ids=[1],
        )
        MockChecker.return_value.check_markdown.return_value = self._failing_report()

        original_argv = sys.argv
        try:
            sys.argv = [
                "send_newsletter.py",
                "--check-links",
                "--link-check-mode",
                "block",
            ]
            main()
        finally:
            sys.argv = original_argv

        MockClient.assert_not_called()
        db.insert_newsletter_send.assert_not_called()
        assert "delivery blocked" in caplog.text
