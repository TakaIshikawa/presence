"""Tests for the GitHub activity polling script."""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from poll_github_activity import determine_since, ingest_github_activity, main, parse_since


TIMESTAMP = datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)


def test_parse_since_accepts_z_suffix():
    assert parse_since("2026-04-01T12:00:00Z") == TIMESTAMP


def test_determine_since_uses_explicit_value(db):
    db.set_last_github_activity_poll_time(TIMESTAMP - timedelta(days=1))

    assert determine_since(db, TIMESTAMP, 90) == TIMESTAMP


def test_determine_since_uses_activity_poll_watermark(db):
    db.set_last_github_activity_poll_time(TIMESTAMP)

    assert determine_since(db, None, 90) == TIMESTAMP


@patch("poll_github_activity.poll_new_activity")
def test_ingest_github_activity_passes_dry_run(mock_poll, db):
    mock_poll.return_value = []

    ingest_github_activity(
        db=db,
        token="tok",
        username="taka",
        since=TIMESTAMP,
        repositories=["repo"],
        include_issues=False,
        include_discussions=True,
        include_pull_requests=True,
        dry_run=True,
        timeout=10,
        redaction_patterns=[{"name": "ticket", "pattern": "ticket-\\d+"}],
    )

    mock_poll.assert_called_once_with(
        token="tok",
        username="taka",
        since=TIMESTAMP,
        db=db,
        repositories=["repo"],
        include_issues=False,
        include_discussions=True,
        include_pull_requests=True,
        dry_run=True,
        timeout=10,
        redaction_patterns=[{"name": "ticket", "pattern": "ticket-\\d+"}],
    )


@patch("poll_github_activity.update_monitoring")
@patch("poll_github_activity.ingest_github_activity")
@patch("poll_github_activity.script_context")
def test_main_dry_run_does_not_update_watermark(mock_context, mock_ingest, mock_update, db):
    config = MagicMock()
    config.github.token = "tok"
    config.github.username = "taka"
    config.github.repositories = ["repo"]
    config.github.include_issues = False
    config.github.include_discussions = True
    config.github.include_pull_requests = True
    config.privacy.redaction_patterns = []
    config.timeouts.github_seconds = 10
    mock_context.return_value.__enter__.return_value = (config, db)
    mock_context.return_value.__exit__.return_value = None
    mock_ingest.return_value = []

    assert main(["--dry-run", "--since", "2026-04-01T12:00:00Z"]) == 0

    assert db.get_last_github_activity_poll_time() is None
    mock_update.assert_not_called()
    assert mock_ingest.call_args.kwargs["dry_run"] is True
    assert mock_ingest.call_args.kwargs["include_issues"] is False
    assert mock_ingest.call_args.kwargs["include_discussions"] is True
    assert mock_ingest.call_args.kwargs["include_pull_requests"] is True


@patch("poll_github_activity.update_monitoring")
@patch("poll_github_activity.ingest_github_activity")
@patch("poll_github_activity.script_context")
def test_main_persists_watermark_after_success(mock_context, mock_ingest, mock_update, db):
    config = MagicMock()
    config.github.token = "tok"
    config.github.username = "taka"
    config.github.repositories = []
    config.github.include_issues = True
    config.github.include_discussions = False
    config.github.include_pull_requests = False
    config.privacy.redaction_patterns = []
    config.timeouts.github_seconds = 10
    mock_context.return_value.__enter__.return_value = (config, db)
    mock_context.return_value.__exit__.return_value = None
    mock_ingest.return_value = []

    assert main(["--since", "2026-04-01T12:00:00Z"]) == 0

    assert db.get_last_github_activity_poll_time() is not None
    mock_update.assert_called_once_with("poll-github-activity")
    assert mock_ingest.call_args.kwargs["dry_run"] is False
    assert mock_ingest.call_args.kwargs["include_issues"] is True
    assert mock_ingest.call_args.kwargs["include_discussions"] is False
    assert mock_ingest.call_args.kwargs["include_pull_requests"] is False
