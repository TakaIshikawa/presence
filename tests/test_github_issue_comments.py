"""Tests for first-class GitHub issue comment ingestion."""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import requests

from ingestion.github_issue_comments import (
    ACTIVITY_TYPE,
    GitHubIssueComment,
    GitHubIssueCommentClient,
    normalize_issue_comment_payload,
    poll_new_issue_comments,
)

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from poll_github_issue_comments import determine_since, main, parse_since


TIMESTAMP = datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)

if not hasattr(requests, "exceptions"):
    requests.exceptions = SimpleNamespace(
        HTTPError=requests.HTTPError,
        ConnectionError=requests.ConnectionError,
    )


def _mock_response(status_code: int = 200, json_data=None):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data if json_data is not None else []
    if status_code < 400:
        resp.raise_for_status.side_effect = None
    else:
        error = requests.exceptions.HTTPError("HTTP error")
        error.response = resp
        resp.raise_for_status.side_effect = error
    return resp


def _comment_payload(comment_id: int = 701, issue_number: int = 42) -> dict:
    return {
        "id": comment_id,
        "node_id": f"IC_kwDO_{comment_id}",
        "body": "Issue comment body with ticket-1234",
        "user": {"login": "maintainer"},
        "html_url": f"https://github.com/acme/widget/issues/{issue_number}#issuecomment-{comment_id}",
        "url": f"https://api.github.com/repos/acme/widget/issues/comments/{comment_id}",
        "issue_url": f"https://api.github.com/repos/acme/widget/issues/{issue_number}",
        "created_at": "2026-04-01T11:00:00Z",
        "updated_at": "2026-04-01T12:00:00Z",
    }


class TestNormalizeIssueComment:
    def test_normalizes_payload_to_stable_dict_and_redacts(self):
        client = GitHubIssueCommentClient(
            "tok",
            "taka",
            redaction_patterns=[
                {"name": "ticket", "pattern": r"ticket-\d+", "placeholder": "[REDACTED_TICKET]"}
            ],
        )

        record = normalize_issue_comment_payload(
            _comment_payload(),
            repo="acme/widget",
            redactor=client.redactor,
        )

        assert list(record.keys()) == [
            "source_type",
            "repo",
            "issue_number",
            "comment_id",
            "author",
            "body",
            "url",
            "created_at",
            "updated_at",
            "metadata",
        ]
        assert record["source_type"] == ACTIVITY_TYPE
        assert record["repo"] == "acme/widget"
        assert record["issue_number"] == 42
        assert record["comment_id"] == 701
        assert record["author"] == "maintainer"
        assert "ticket-1234" not in record["body"]
        assert "[REDACTED_TICKET]" in record["body"]
        assert record["url"] == "https://github.com/acme/widget/issues/42#issuecomment-701"
        assert record["created_at"].isoformat() == "2026-04-01T11:00:00+00:00"
        assert record["updated_at"].isoformat() == "2026-04-01T12:00:00+00:00"
        assert record["metadata"]["issue_url"].endswith("/issues/42")
        assert record["metadata"]["node_id"] == "IC_kwDO_701"

    def test_missing_optional_fields_do_not_crash_normalization(self):
        payload = {
            "id": 702,
            "body": None,
            "created_at": "2026-04-01T11:00:00Z",
        }

        record = normalize_issue_comment_payload(payload, repo="acme/widget")

        assert record["issue_number"] is None
        assert record["body"] == ""
        assert record["author"] == ""
        assert record["url"] == ""
        assert record["metadata"] == {}
        assert record["updated_at"].isoformat() == "2026-04-01T11:00:00+00:00"


class TestGitHubIssueCommentClient:
    def test_get_repo_issue_comments_paginates_limits_and_filters_since(self):
        first_page = [_comment_payload(comment_id=800 + index) for index in range(100)]
        second = _comment_payload(950)
        old = _comment_payload(951)
        old["updated_at"] = "2026-03-01T12:00:00Z"
        session = MagicMock()
        session.get.side_effect = [
            _mock_response(json_data=first_page),
            _mock_response(json_data=[second, old]),
        ]
        client = GitHubIssueCommentClient("tok", "taka", session=session)

        comments = list(
            client.get_repo_issue_comments(
                "acme",
                "widget",
                repo_name="acme/widget",
                since=TIMESTAMP,
                limit=102,
            )
        )

        assert len(comments) == 101
        assert comments[0].comment_id == 800
        assert comments[-1].comment_id == 950
        assert session.get.call_args_list[0].kwargs["params"]["since"] == TIMESTAMP.isoformat()
        assert session.get.call_args_list[0].kwargs["params"]["per_page"] == 100
        assert session.get.call_args_list[0].kwargs["params"]["page"] == 1
        assert session.get.call_args_list[1].kwargs["params"]["per_page"] == 2
        assert session.get.call_args_list[1].kwargs["params"]["page"] == 2

    def test_get_repo_issue_comments_keeps_edited_comments(self):
        edited = _comment_payload(960)
        edited["created_at"] = "2026-03-01T12:00:00Z"
        edited["updated_at"] = "2026-04-01T12:30:00Z"
        session = MagicMock()
        session.get.return_value = _mock_response(json_data=[edited])
        client = GitHubIssueCommentClient("tok", "taka", session=session)

        comments = list(
            client.get_repo_issue_comments(
                "acme",
                "widget",
                repo_name="acme/widget",
                since=TIMESTAMP,
            )
        )

        assert len(comments) == 1
        assert comments[0].comment_id == 960
        assert comments[0].created_at.isoformat() == "2026-03-01T12:00:00+00:00"
        assert comments[0].updated_at.isoformat() == "2026-04-01T12:30:00+00:00"

    def test_get_repo_issue_comments_handles_empty_response(self):
        session = MagicMock()
        session.get.return_value = _mock_response(json_data=[])
        client = GitHubIssueCommentClient("tok", "taka", session=session)

        assert list(client.get_repo_issue_comments("acme", "widget", since=TIMESTAMP)) == []
        session.get.assert_called_once()

    def test_comment_to_activity_dict_uses_github_activity_shape(self):
        comment = GitHubIssueComment(
            repo="acme/widget",
            issue_number=42,
            comment_id=701,
            author="maintainer",
            body="Body",
            url="https://github.com/acme/widget/issues/42#issuecomment-701",
            created_at=TIMESTAMP - timedelta(hours=1),
            updated_at=TIMESTAMP,
            metadata={"node_id": "IC_kwDO_701"},
        )

        activity = comment.to_activity_dict()

        assert comment.activity_id == f"acme/widget#701:{ACTIVITY_TYPE}"
        assert activity["repo_name"] == "acme/widget"
        assert activity["activity_type"] == ACTIVITY_TYPE
        assert activity["number"] == 701
        assert activity["title"] == "Issue comment on #42"
        assert activity["metadata"]["source_type"] == ACTIVITY_TYPE
        assert activity["metadata"]["issue_number"] == 42
        assert activity["metadata"]["parent_type"] == "issue"


class TestPollNewIssueComments:
    @patch.object(GitHubIssueCommentClient, "get_all_recent_issue_comments")
    def test_persists_only_new_unique_comments(self, mock_comments):
        new_comment = GitHubIssueComment(
            repo="acme/widget",
            issue_number=42,
            comment_id=701,
            author="maintainer",
            body="Body",
            url="url",
            created_at=TIMESTAMP,
            updated_at=TIMESTAMP,
        )
        existing = GitHubIssueComment(
            repo="acme/widget",
            issue_number=43,
            comment_id=702,
            author="maintainer",
            body="Old",
            url="url",
            created_at=TIMESTAMP,
            updated_at=TIMESTAMP,
        )
        duplicate = GitHubIssueComment(**new_comment.__dict__)
        mock_comments.return_value = iter([new_comment, duplicate, existing])
        db = MagicMock()
        db.is_github_activity_processed.side_effect = [False, True]

        result = poll_new_issue_comments("tok", "taka", TIMESTAMP, db, repositories=["acme/widget"])

        assert result == [new_comment]
        assert db.is_github_activity_processed.call_count == 2
        db.upsert_github_activity.assert_called_once_with(**new_comment.to_activity_dict())

    @patch.object(GitHubIssueCommentClient, "get_all_recent_issue_comments")
    def test_dry_run_does_not_persist(self, mock_comments):
        comment = GitHubIssueComment(
            repo="acme/widget",
            issue_number=42,
            comment_id=701,
            author="maintainer",
            body="Body",
            url="url",
            created_at=TIMESTAMP,
            updated_at=TIMESTAMP,
        )
        mock_comments.return_value = iter([comment])
        db = MagicMock()
        db.is_github_activity_processed.return_value = False

        assert poll_new_issue_comments("tok", "taka", TIMESTAMP, db, dry_run=True) == [comment]
        db.upsert_github_activity.assert_not_called()

    @patch.object(GitHubIssueCommentClient, "get_all_recent_issue_comments")
    def test_persists_to_existing_github_activity_table(self, mock_comments, db):
        comment = GitHubIssueComment(
            repo="acme/widget",
            issue_number=42,
            comment_id=701,
            author="maintainer",
            body="Body",
            url="url",
            created_at=TIMESTAMP,
            updated_at=TIMESTAMP,
        )
        mock_comments.return_value = iter([comment])

        result = poll_new_issue_comments("tok", "taka", TIMESTAMP, db)
        rows = db.get_github_activity_in_range(TIMESTAMP, TIMESTAMP + timedelta(seconds=1))

        assert result == [comment]
        assert len(rows) == 1
        assert rows[0]["activity_type"] == ACTIVITY_TYPE
        assert rows[0]["number"] == 701
        assert rows[0]["metadata"]["comment_id"] == 701
        assert rows[0]["metadata"]["issue_number"] == 42


def test_parse_since_accepts_z_suffix():
    assert parse_since("2026-04-01T12:00:00Z") == TIMESTAMP


def test_determine_since_uses_issue_comment_poll_watermark(db):
    db.set_last_github_issue_comment_poll_time(TIMESTAMP - timedelta(hours=1))

    assert determine_since(db, None, 90) == TIMESTAMP - timedelta(hours=1)


@patch("poll_github_issue_comments.update_monitoring")
@patch("poll_github_issue_comments.ingest_github_issue_comments")
@patch("poll_github_issue_comments.script_context")
def test_main_dry_run_prints_comments_without_watermark(
    mock_context,
    mock_ingest,
    mock_update,
    db,
    capsys,
):
    config = MagicMock()
    config.github.token = "tok"
    config.github.username = "taka"
    config.github.repositories = ["acme/widget"]
    config.privacy.redaction_patterns = []
    config.timeouts.github_seconds = 10
    mock_context.return_value.__enter__.return_value = (config, db)
    mock_context.return_value.__exit__.return_value = None
    comment = GitHubIssueComment(
        repo="acme/widget",
        issue_number=42,
        comment_id=701,
        author="maintainer",
        body="Body",
        url="https://github.com/acme/widget/issues/42#issuecomment-701",
        created_at=TIMESTAMP,
        updated_at=TIMESTAMP,
    )
    mock_ingest.return_value = [comment]

    assert main(["--dry-run", "--since", "2026-04-01T12:00:00Z"]) == 0

    assert "Would ingest acme/widget#701:github_issue_comment" in capsys.readouterr().out
    assert db.get_last_github_issue_comment_poll_time() is None
    mock_update.assert_not_called()
    assert mock_ingest.call_args.kwargs["dry_run"] is True


@patch("poll_github_issue_comments.update_monitoring")
@patch("poll_github_issue_comments.ingest_github_issue_comments")
@patch("poll_github_issue_comments.script_context")
def test_main_persists_watermark_when_enabled(mock_context, mock_ingest, mock_update, db):
    config = MagicMock()
    config.github.token = "tok"
    config.github.username = "taka"
    config.github.repositories = []
    config.privacy.redaction_patterns = []
    config.timeouts.github_seconds = 10
    mock_context.return_value.__enter__.return_value = (config, db)
    mock_context.return_value.__exit__.return_value = None
    mock_ingest.return_value = []

    assert main(["--since", "2026-04-01T12:00:00Z"]) == 0

    assert db.get_last_github_issue_comment_poll_time() is not None
    mock_update.assert_called_once_with("poll-github-issue-comments")
