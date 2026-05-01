"""Tests for combined GitHub pull request comment ingestion."""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import requests

from ingestion.github_commits import GitHubAuthError, GitHubRateLimitError
from ingestion.github_pr_comments import (
    ISSUE_COMMENT_TYPE,
    REVIEW_COMMENT_TYPE,
    GitHubPRComment,
    GitHubPRCommentClient,
    normalize_issue_style_pr_comment_payload,
    normalize_review_comment_payload,
    poll_new_pr_comments,
)

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from poll_github_pr_comments import determine_since, main, parse_since


TIMESTAMP = datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)

if not hasattr(requests, "exceptions"):
    requests.exceptions = SimpleNamespace(
        HTTPError=requests.HTTPError,
        ConnectionError=requests.ConnectionError,
    )


def _mock_response(status_code: int = 200, json_data=None):
    resp = MagicMock()
    resp.status_code = status_code
    resp.headers = {}
    resp.json.return_value = json_data if json_data is not None else []
    if status_code < 400:
        resp.raise_for_status.side_effect = None
    else:
        error = requests.exceptions.HTTPError("HTTP error")
        error.response = resp
        resp.raise_for_status.side_effect = error
    return resp


def _review_payload(comment_id: int = 601, pr_number: int = 8) -> dict:
    return {
        "id": comment_id,
        "pull_request_review_id": 77,
        "body": "Review body with ticket-1234",
        "user": {"login": "reviewer"},
        "html_url": f"https://github.com/acme/widget/pull/{pr_number}#discussion_r{comment_id}",
        "pull_request_url": f"https://api.github.com/repos/acme/widget/pulls/{pr_number}",
        "path": "src/app.py",
        "position": 4,
        "line": 12,
        "side": "RIGHT",
        "diff_hunk": "@@ -1 +1 @@",
        "created_at": "2026-04-01T11:00:00Z",
        "updated_at": "2026-04-01T12:00:00Z",
    }


def _issue_payload(comment_id: int = 701, pr_number: int = 8) -> dict:
    return {
        "id": comment_id,
        "node_id": f"IC_kwDO_{comment_id}",
        "body": "Conversation body with api_key=abc123secret",
        "user": {"login": "maintainer"},
        "html_url": f"https://github.com/acme/widget/pull/{pr_number}#issuecomment-{comment_id}",
        "url": f"https://api.github.com/repos/acme/widget/issues/comments/{comment_id}",
        "issue_url": f"https://api.github.com/repos/acme/widget/issues/{pr_number}",
        "created_at": "2026-04-01T10:00:00Z",
        "updated_at": "2026-04-01T12:00:00Z",
    }


class TestNormalizePRComments:
    def test_normalizes_review_comment_to_stable_redacted_dict(self):
        client = GitHubPRCommentClient(
            "tok",
            "taka",
            redaction_patterns=[
                {"name": "ticket", "pattern": r"ticket-\d+", "placeholder": "[REDACTED_TICKET]"}
            ],
        )

        record = normalize_review_comment_payload(
            _review_payload(),
            repo="acme/widget",
            redactor=client.redactor,
        )

        assert list(record.keys()) == [
            "repo",
            "pr_number",
            "author",
            "body",
            "url",
            "created_at",
            "updated_at",
            "external_id",
            "source_type",
            "comment_id",
            "path",
            "diff_hunk",
            "metadata",
        ]
        assert record["repo"] == "acme/widget"
        assert record["pr_number"] == 8
        assert record["author"] == "reviewer"
        assert "ticket-1234" not in record["body"]
        assert "[REDACTED_TICKET]" in record["body"]
        assert record["external_id"] == "github:github_pr_review_comment:acme/widget:601"
        assert record["source_type"] == REVIEW_COMMENT_TYPE
        assert record["comment_id"] == 601
        assert record["path"] == "src/app.py"
        assert record["metadata"]["pull_request_review_id"] == 77

    def test_normalizes_issue_style_pr_comment_to_stable_redacted_dict(self):
        record = normalize_issue_style_pr_comment_payload(_issue_payload(), repo="acme/widget")

        assert record["repo"] == "acme/widget"
        assert record["pr_number"] == 8
        assert record["author"] == "maintainer"
        assert "abc123secret" not in record["body"]
        assert "[REDACTED_SECRET]" in record["body"]
        assert record["url"].endswith("/pull/8#issuecomment-701")
        assert record["created_at"].isoformat() == "2026-04-01T10:00:00+00:00"
        assert record["updated_at"].isoformat() == "2026-04-01T12:00:00+00:00"
        assert record["external_id"] == "github:github_pr_issue_comment:acme/widget:701"
        assert record["source_type"] == ISSUE_COMMENT_TYPE
        assert record["comment_id"] == 701
        assert record["metadata"]["node_id"] == "IC_kwDO_701"


class TestGitHubPRCommentClient:
    def test_review_comment_pagination_limit_and_since_filter(self):
        first_page = [_review_payload(comment_id=800 + index) for index in range(100)]
        second = _review_payload(950)
        old = _review_payload(951)
        old["updated_at"] = "2026-03-01T12:00:00Z"
        session = MagicMock()
        session.get.side_effect = [
            _mock_response(json_data=first_page),
            _mock_response(json_data=[second, old]),
        ]
        client = GitHubPRCommentClient("tok", "taka", session=session)

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
        assert comments[0].comment_id == 800
        assert comments[-1].comment_id == 950
        assert session.get.call_args_list[0].kwargs["params"]["since"] == TIMESTAMP.isoformat()
        assert session.get.call_args_list[0].kwargs["params"]["per_page"] == 100
        assert session.get.call_args_list[1].kwargs["params"]["per_page"] == 2

    def test_issue_style_pr_comments_filters_non_pr_issue_comments(self):
        issue = _issue_payload(702, pr_number=42)
        issue["html_url"] = "https://github.com/acme/widget/issues/42#issuecomment-702"
        session = MagicMock()
        session.get.return_value = _mock_response(json_data=[_issue_payload(701), issue])
        client = GitHubPRCommentClient("tok", "taka", session=session)

        comments = list(
            client.get_repo_issue_style_pr_comments(
                "acme",
                "widget",
                repo_name="acme/widget",
                since=TIMESTAMP,
            )
        )

        assert [comment.comment_id for comment in comments] == [701]
        assert comments[0].source_type == ISSUE_COMMENT_TYPE

    def test_get_maps_github_error_responses(self):
        session = MagicMock()
        session.get.return_value = _mock_response(status_code=401)
        client = GitHubPRCommentClient("tok", "taka", session=session)

        try:
            list(client.get_repo_review_comments("acme", "widget", since=TIMESTAMP))
        except GitHubAuthError:
            pass
        else:
            raise AssertionError("expected GitHubAuthError")

        session.get.return_value = _mock_response(status_code=403)
        try:
            list(client.get_repo_review_comments("acme", "widget", since=TIMESTAMP))
        except GitHubRateLimitError:
            pass
        else:
            raise AssertionError("expected GitHubRateLimitError")

    def test_comment_to_activity_dict_uses_github_activity_shape(self):
        comment = GitHubPRComment(
            repo="acme/widget",
            pr_number=8,
            comment_id=601,
            author="reviewer",
            body="Body",
            url="https://github.com/acme/widget/pull/8#discussion_r601",
            created_at=TIMESTAMP - timedelta(hours=1),
            updated_at=TIMESTAMP,
            source_type=REVIEW_COMMENT_TYPE,
            external_id="github:github_pr_review_comment:acme/widget:601",
            path="src/app.py",
            diff_hunk="@@",
            metadata={"pull_request_review_id": 77},
        )

        activity = comment.to_activity_dict()

        assert comment.activity_id == f"acme/widget#601:{REVIEW_COMMENT_TYPE}"
        assert activity["repo_name"] == "acme/widget"
        assert activity["activity_type"] == REVIEW_COMMENT_TYPE
        assert activity["number"] == 601
        assert activity["title"] == "PR review comment on #8 src/app.py"
        assert activity["metadata"]["external_id"] == comment.external_id
        assert activity["metadata"]["parent_type"] == "pull_request"


class TestPollNewPRComments:
    @patch.object(GitHubPRCommentClient, "get_all_recent_pr_comments")
    def test_persists_only_new_unique_comments_and_counts(self, mock_comments):
        new_comment = GitHubPRComment(
            repo="acme/widget",
            pr_number=8,
            comment_id=601,
            author="reviewer",
            body="Body",
            url="url",
            created_at=TIMESTAMP,
            updated_at=TIMESTAMP,
            source_type=REVIEW_COMMENT_TYPE,
            external_id="github:github_pr_review_comment:acme/widget:601",
        )
        duplicate = GitHubPRComment(**new_comment.__dict__)
        existing = GitHubPRComment(
            repo="acme/widget",
            pr_number=8,
            comment_id=701,
            author="maintainer",
            body="Old",
            url="url",
            created_at=TIMESTAMP,
            updated_at=TIMESTAMP,
            source_type=ISSUE_COMMENT_TYPE,
            external_id="github:github_pr_issue_comment:acme/widget:701",
        )
        mock_comments.return_value = iter([new_comment, duplicate, existing])
        db = MagicMock()
        db.is_github_activity_processed.side_effect = [False, True]

        result = poll_new_pr_comments("tok", "taka", TIMESTAMP, db, repositories=["acme/widget"])

        assert result.comments == [new_comment]
        assert result.fetched_count == 3
        assert result.duplicate_count == 1
        assert result.skipped_count == 1
        db.upsert_github_activity.assert_called_once_with(**new_comment.to_activity_dict())

    @patch.object(GitHubPRCommentClient, "get_all_recent_pr_comments")
    def test_dry_run_does_not_persist(self, mock_comments):
        comment = GitHubPRComment(
            repo="acme/widget",
            pr_number=8,
            comment_id=701,
            author="maintainer",
            body="Body",
            url="url",
            created_at=TIMESTAMP,
            updated_at=TIMESTAMP,
            source_type=ISSUE_COMMENT_TYPE,
            external_id="github:github_pr_issue_comment:acme/widget:701",
        )
        mock_comments.return_value = iter([comment])
        db = MagicMock()
        db.is_github_activity_processed.return_value = False

        result = poll_new_pr_comments("tok", "taka", TIMESTAMP, db, dry_run=True)

        assert result.comments == [comment]
        db.upsert_github_activity.assert_not_called()

    @patch.object(GitHubPRCommentClient, "get_all_recent_pr_comments")
    def test_persists_to_existing_github_activity_table(self, mock_comments, db):
        comment = GitHubPRComment(
            repo="acme/widget",
            pr_number=8,
            comment_id=701,
            author="maintainer",
            body="Body",
            url="url",
            created_at=TIMESTAMP,
            updated_at=TIMESTAMP,
            source_type=ISSUE_COMMENT_TYPE,
            external_id="github:github_pr_issue_comment:acme/widget:701",
        )
        mock_comments.return_value = iter([comment])

        result = poll_new_pr_comments("tok", "taka", TIMESTAMP, db)
        rows = db.get_github_activity_in_range(TIMESTAMP, TIMESTAMP + timedelta(seconds=1))

        assert result.comments == [comment]
        assert len(rows) == 1
        assert rows[0]["activity_type"] == ISSUE_COMMENT_TYPE
        assert rows[0]["number"] == 701
        assert rows[0]["metadata"]["external_id"] == comment.external_id


def test_parse_since_accepts_z_suffix():
    assert parse_since("2026-04-01T12:00:00Z") == TIMESTAMP


def test_determine_since_uses_pr_comment_poll_watermark(db):
    db.set_last_github_pr_comment_poll_time(TIMESTAMP - timedelta(hours=1))

    assert determine_since(db, None, 90) == TIMESTAMP - timedelta(hours=1)


@patch("poll_github_pr_comments.update_monitoring")
@patch("poll_github_pr_comments.ingest_github_pr_comments")
@patch("poll_github_pr_comments.script_context")
def test_main_dry_run_prints_comments_and_summary_without_watermark(
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
    comment = GitHubPRComment(
        repo="acme/widget",
        pr_number=8,
        comment_id=701,
        author="maintainer",
        body="Body",
        url="https://github.com/acme/widget/pull/8#issuecomment-701",
        created_at=TIMESTAMP,
        updated_at=TIMESTAMP,
        source_type=ISSUE_COMMENT_TYPE,
        external_id="github:github_pr_issue_comment:acme/widget:701",
    )
    result = SimpleNamespace(
        comments=[comment],
        fetched_count=3,
        skipped_count=1,
        duplicate_count=1,
    )
    mock_ingest.return_value = result

    assert main(["--dry-run", "--repo", "acme/widget", "--since", "2026-04-01T12:00:00Z"]) == 0

    out = capsys.readouterr().out
    assert f"Would ingest acme/widget#701:{ISSUE_COMMENT_TYPE}" in out
    assert "Dry run summary: fetched=3 skipped=1 duplicates=1 new=1" in out
    assert db.get_last_github_pr_comment_poll_time() is None
    mock_update.assert_not_called()
    assert mock_ingest.call_args.kwargs["repositories"] == ["acme/widget"]
    assert mock_ingest.call_args.kwargs["dry_run"] is True


@patch("poll_github_pr_comments.update_monitoring")
@patch("poll_github_pr_comments.ingest_github_pr_comments")
@patch("poll_github_pr_comments.script_context")
def test_main_persists_watermark_after_success(mock_context, mock_ingest, mock_update, db):
    config = MagicMock()
    config.github.token = "tok"
    config.github.username = "taka"
    config.github.repositories = []
    config.privacy.redaction_patterns = []
    config.timeouts.github_seconds = 10
    mock_context.return_value.__enter__.return_value = (config, db)
    mock_context.return_value.__exit__.return_value = None
    mock_ingest.return_value = SimpleNamespace(
        comments=[],
        fetched_count=0,
        skipped_count=0,
        duplicate_count=0,
    )

    assert main(["--since", "2026-04-01T12:00:00Z", "--limit", "25"]) == 0

    assert db.get_last_github_pr_comment_poll_time() is not None
    mock_update.assert_called_once_with("poll-github-pr-comments")
    assert mock_ingest.call_args.kwargs["limit"] == 25
