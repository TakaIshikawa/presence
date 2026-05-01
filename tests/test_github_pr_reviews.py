"""Tests for first-class GitHub PR review comment ingestion."""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import requests

from ingestion.github_pr_reviews import (
    ACTIVITY_TYPE,
    GitHubPRReviewClient,
    GitHubPRReviewComment,
    normalize_review_comment_payload,
    poll_new_pr_review_comments,
)

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from poll_github_pr_reviews import determine_since, main, parse_since


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


def _comment_payload(comment_id: int = 601, pr_number: int = 8) -> dict:
    return {
        "id": comment_id,
        "pull_request_review_id": 77,
        "body": "Review comment body with ticket-1234",
        "user": {"login": "reviewer"},
        "html_url": f"https://github.com/acme/widget/pull/{pr_number}#discussion_r{comment_id}",
        "pull_request_url": f"https://api.github.com/repos/acme/widget/pulls/{pr_number}",
        "path": "src/app.py",
        "position": 4,
        "original_position": 4,
        "line": 12,
        "side": "RIGHT",
        "commit_id": "abc123",
        "original_commit_id": "abc123",
        "diff_hunk": "@@ -1 +1 @@",
        "created_at": "2026-04-01T11:00:00Z",
        "updated_at": "2026-04-01T12:00:00Z",
    }


class TestNormalizeReviewComment:
    def test_normalizes_payload_to_stable_dict_and_redacts(self):
        client = GitHubPRReviewClient(
            "tok",
            "taka",
            redaction_patterns=[
                {"name": "ticket", "pattern": r"ticket-\d+", "placeholder": "[REDACTED_TICKET]"}
            ],
        )

        record = normalize_review_comment_payload(
            _comment_payload(),
            repo="acme/widget",
            redactor=client.redactor,
        )

        assert list(record.keys()) == [
            "repo",
            "pr_number",
            "comment_id",
            "path",
            "diff_hunk",
            "body",
            "author",
            "created_at",
            "updated_at",
            "url",
            "metadata",
        ]
        assert record["repo"] == "acme/widget"
        assert record["pr_number"] == 8
        assert record["comment_id"] == 601
        assert record["path"] == "src/app.py"
        assert record["diff_hunk"] == "@@ -1 +1 @@"
        assert "ticket-1234" not in record["body"]
        assert "[REDACTED_TICKET]" in record["body"]
        assert record["author"] == "reviewer"
        assert record["created_at"].isoformat() == "2026-04-01T11:00:00+00:00"
        assert record["updated_at"].isoformat() == "2026-04-01T12:00:00+00:00"
        assert record["metadata"]["pull_request_review_id"] == 77
        assert record["metadata"]["line"] == 12
        assert record["metadata"]["side"] == "RIGHT"

    def test_missing_optional_fields_do_not_crash_normalization(self):
        payload = {
            "id": 602,
            "body": None,
            "created_at": "2026-04-01T11:00:00Z",
        }

        record = normalize_review_comment_payload(payload, repo="acme/widget")

        assert record["pr_number"] is None
        assert record["path"] == ""
        assert record["diff_hunk"] == ""
        assert record["body"] == ""
        assert record["author"] == ""
        assert record["url"] == ""
        assert record["updated_at"].isoformat() == "2026-04-01T11:00:00+00:00"


class TestGitHubPRReviewClient:
    @patch("requests.get", create=True)
    def test_get_repo_review_comments_paginates_limits_and_filters_since(self, mock_get):
        first_page = [_comment_payload(comment_id=700 + index) for index in range(100)]
        second = _comment_payload(900)
        old = _comment_payload(901)
        old["updated_at"] = "2026-03-01T12:00:00Z"
        mock_get.side_effect = [
            _mock_response(json_data=first_page),
            _mock_response(json_data=[second, old]),
        ]
        client = GitHubPRReviewClient("tok", "taka")

        comments = list(
            client.get_repo_review_comments(
                "acme",
                "widget",
                repo_name="acme/widget",
                since=TIMESTAMP,
                limit=102,
            )
        )

        assert len(comments) == 101
        assert comments[0].comment_id == 700
        assert comments[-1].comment_id == 900
        assert mock_get.call_args_list[0].kwargs["params"]["since"] == TIMESTAMP.isoformat()
        assert mock_get.call_args_list[0].kwargs["params"]["per_page"] == 100
        assert mock_get.call_args_list[0].kwargs["params"]["page"] == 1
        assert mock_get.call_args_list[1].kwargs["params"]["per_page"] == 2
        assert mock_get.call_args_list[1].kwargs["params"]["page"] == 2

    def test_comment_to_activity_dict_uses_github_activity_shape(self):
        comment = GitHubPRReviewComment(
            repo="acme/widget",
            pr_number=8,
            comment_id=601,
            path="src/app.py",
            diff_hunk="@@ -1 +1 @@",
            body="Body",
            author="reviewer",
            created_at=TIMESTAMP - timedelta(hours=1),
            updated_at=TIMESTAMP,
            url="https://github.com/acme/widget/pull/8#discussion_r601",
            metadata={"pull_request_review_id": 77, "resolved": True},
        )

        activity = comment.to_activity_dict()

        assert comment.activity_id == f"acme/widget#601:{ACTIVITY_TYPE}"
        assert activity["repo_name"] == "acme/widget"
        assert activity["activity_type"] == ACTIVITY_TYPE
        assert activity["number"] == 601
        assert activity["title"] == "PR review comment on #8 src/app.py"
        assert activity["metadata"]["parent_pr_number"] == 8
        assert activity["metadata"]["resolved"] is True


class TestPollNewPRReviewComments:
    @patch.object(GitHubPRReviewClient, "get_all_recent_review_comments")
    def test_persists_only_new_unique_comments(self, mock_comments):
        new_comment = GitHubPRReviewComment(
            repo="acme/widget",
            pr_number=8,
            comment_id=601,
            path="src/app.py",
            diff_hunk="@@",
            body="Body",
            author="reviewer",
            created_at=TIMESTAMP,
            updated_at=TIMESTAMP,
            url="url",
        )
        existing = GitHubPRReviewComment(
            repo="acme/widget",
            pr_number=8,
            comment_id=602,
            path="src/app.py",
            diff_hunk="@@",
            body="Old",
            author="reviewer",
            created_at=TIMESTAMP,
            updated_at=TIMESTAMP,
            url="url",
        )
        duplicate = GitHubPRReviewComment(**new_comment.__dict__)
        mock_comments.return_value = iter([new_comment, duplicate, existing])
        db = MagicMock()
        db.is_github_activity_processed.side_effect = [False, True]

        result = poll_new_pr_review_comments("tok", "taka", TIMESTAMP, db, repositories=["acme/widget"])

        assert result == [new_comment]
        assert db.is_github_activity_processed.call_count == 2
        db.upsert_github_activity.assert_called_once_with(**new_comment.to_activity_dict())

    @patch.object(GitHubPRReviewClient, "get_all_recent_review_comments")
    def test_dry_run_does_not_persist(self, mock_comments):
        comment = GitHubPRReviewComment(
            repo="acme/widget",
            pr_number=8,
            comment_id=601,
            path="src/app.py",
            diff_hunk="@@",
            body="Body",
            author="reviewer",
            created_at=TIMESTAMP,
            updated_at=TIMESTAMP,
            url="url",
        )
        mock_comments.return_value = iter([comment])
        db = MagicMock()
        db.is_github_activity_processed.return_value = False

        assert poll_new_pr_review_comments("tok", "taka", TIMESTAMP, db, dry_run=True) == [comment]
        db.upsert_github_activity.assert_not_called()

    @patch.object(GitHubPRReviewClient, "get_all_recent_review_comments")
    def test_persists_to_existing_github_activity_table(self, mock_comments, db):
        comment = GitHubPRReviewComment(
            repo="acme/widget",
            pr_number=8,
            comment_id=601,
            path="src/app.py",
            diff_hunk="@@",
            body="Body",
            author="reviewer",
            created_at=TIMESTAMP,
            updated_at=TIMESTAMP,
            url="url",
        )
        mock_comments.return_value = iter([comment])

        result = poll_new_pr_review_comments("tok", "taka", TIMESTAMP, db)
        rows = db.get_github_activity_in_range(TIMESTAMP, TIMESTAMP + timedelta(seconds=1))

        assert result == [comment]
        assert len(rows) == 1
        assert rows[0]["activity_type"] == ACTIVITY_TYPE
        assert rows[0]["number"] == 601
        assert rows[0]["metadata"]["comment_id"] == 601


def test_parse_since_accepts_z_suffix():
    assert parse_since("2026-04-01T12:00:00Z") == TIMESTAMP


def test_determine_since_uses_pr_review_poll_watermark(db):
    db.set_last_github_pr_review_poll_time(TIMESTAMP - timedelta(hours=1))

    assert determine_since(db, None, 90) == TIMESTAMP - timedelta(hours=1)


@patch("poll_github_pr_reviews.update_monitoring")
@patch("poll_github_pr_reviews.ingest_github_pr_review_comments")
@patch("poll_github_pr_reviews.script_context")
def test_main_dry_run_prints_comments_without_watermark(mock_context, mock_ingest, mock_update, db, capsys):
    config = MagicMock()
    config.github.token = "tok"
    config.github.username = "taka"
    config.github.repositories = ["acme/widget"]
    config.privacy.redaction_patterns = []
    config.timeouts.github_seconds = 10
    mock_context.return_value.__enter__.return_value = (config, db)
    mock_context.return_value.__exit__.return_value = None
    comment = GitHubPRReviewComment(
        repo="acme/widget",
        pr_number=8,
        comment_id=601,
        path="src/app.py",
        diff_hunk="@@",
        body="Body",
        author="reviewer",
        created_at=TIMESTAMP,
        updated_at=TIMESTAMP,
        url="https://github.com/acme/widget/pull/8#discussion_r601",
    )
    mock_ingest.return_value = [comment]

    assert main(["--dry-run", "--repo", "acme/widget", "--since", "2026-04-01T12:00:00Z"]) == 0

    assert f"Would ingest acme/widget#601:{ACTIVITY_TYPE}" in capsys.readouterr().out
    assert db.get_last_github_pr_review_poll_time() is None
    mock_update.assert_not_called()
    assert mock_ingest.call_args.kwargs["repositories"] == ["acme/widget"]
    assert mock_ingest.call_args.kwargs["dry_run"] is True


@patch("poll_github_pr_reviews.update_monitoring")
@patch("poll_github_pr_reviews.ingest_github_pr_review_comments")
@patch("poll_github_pr_reviews.script_context")
def test_main_persists_watermark_after_success(mock_context, mock_ingest, mock_update, db):
    config = MagicMock()
    config.github.token = "tok"
    config.github.username = "taka"
    config.github.repositories = []
    config.privacy.redaction_patterns = []
    config.timeouts.github_seconds = 10
    mock_context.return_value.__enter__.return_value = (config, db)
    mock_context.return_value.__exit__.return_value = None
    mock_ingest.return_value = []

    assert main(["--since", "2026-04-01T12:00:00Z", "--limit", "25"]) == 0

    assert db.get_last_github_pr_review_poll_time() is not None
    mock_update.assert_called_once_with("poll-github-pr-reviews")
    assert mock_ingest.call_args.kwargs["limit"] == 25
