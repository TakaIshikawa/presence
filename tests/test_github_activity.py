"""Unit tests for GitHub issue, pull request, release, and discussion ingestion."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import requests

from ingestion.github_activity import (
    GitHubActivity,
    GitHubActivityClient,
    poll_new_activity,
)
from ingestion.github_commits import GitHubAuthError, GitHubNotFoundError

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


def _issue_payload(number: int = 1, title: str = "Issue", updated_at: str = "2026-04-01T12:00:00Z") -> dict:
    return {
        "number": number,
        "title": title,
        "state": "open",
        "body": "Issue body",
        "comments": 0,
        "user": {"login": "taka"},
        "html_url": f"https://github.com/taka/repo/issues/{number}",
        "updated_at": updated_at,
        "created_at": "2026-04-01T10:00:00Z",
        "closed_at": None,
        "labels": [{"name": "bug"}],
    }


def _pull_payload(number: int = 2, title: str = "PR") -> dict:
    return {
        "number": number,
        "title": title,
        "state": "closed",
        "body": "PR body",
        "user": {"login": "taka"},
        "html_url": f"https://github.com/taka/repo/pull/{number}",
        "updated_at": "2026-04-01T12:00:00Z",
        "created_at": "2026-04-01T10:00:00Z",
        "closed_at": "2026-04-01T12:30:00Z",
        "merged_at": "2026-04-01T12:20:00Z",
        "labels": [{"name": "enhancement"}],
    }


def _release_payload(release_id: int = 101, tag: str = "v1.0.0") -> dict:
    return {
        "id": release_id,
        "tag_name": tag,
        "target_commitish": "main",
        "name": "Release 1.0.0",
        "body": "Release notes",
        "draft": False,
        "prerelease": True,
        "author": {"login": "taka"},
        "html_url": f"https://github.com/taka/repo/releases/tag/{tag}",
        "published_at": "2026-04-01T12:00:00Z",
        "created_at": "2026-04-01T10:00:00Z",
    }


def _issue_comment_payload(comment_id: int = 501, issue_number: int = 7) -> dict:
    return {
        "id": comment_id,
        "body": "Issue comment body with ticket-1234",
        "user": {"login": "octo"},
        "html_url": f"https://github.com/taka/repo/issues/{issue_number}#issuecomment-{comment_id}",
        "issue_url": f"https://api.github.com/repos/taka/repo/issues/{issue_number}",
        "created_at": "2026-04-01T11:00:00Z",
        "updated_at": "2026-04-01T12:00:00Z",
    }


def _review_comment_payload(comment_id: int = 601, pr_number: int = 8) -> dict:
    return {
        "id": comment_id,
        "pull_request_review_id": 77,
        "body": "Review comment body with ticket-1234",
        "user": {"login": "reviewer"},
        "html_url": f"https://github.com/taka/repo/pull/{pr_number}#discussion_r{comment_id}",
        "pull_request_url": f"https://api.github.com/repos/taka/repo/pulls/{pr_number}",
        "path": "src/app.py",
        "position": 4,
        "original_position": 4,
        "commit_id": "abc123",
        "original_commit_id": "abc123",
        "diff_hunk": "@@ -1 +1 @@",
        "created_at": "2026-04-01T11:00:00Z",
        "updated_at": "2026-04-01T12:00:00Z",
    }


def _discussion_payload(number: int = 4, title: str = "Discussion") -> dict:
    return {
        "number": number,
        "title": title,
        "bodyText": "Discussion body with secret ticket-1234",
        "url": f"https://github.com/taka/repo/discussions/{number}",
        "createdAt": "2026-04-01T10:00:00Z",
        "updatedAt": "2026-04-01T12:00:00Z",
        "answerChosenAt": "2026-04-01T12:30:00Z",
        "answerChosenBy": {"login": "taka"},
        "author": {"login": "octo"},
        "category": {"name": "Q&A", "slug": "q-a", "emoji": ":bulb:"},
        "comments": {"totalCount": 3},
        "answer": {
            "url": f"https://github.com/taka/repo/discussions/{number}#discussioncomment-1",
            "bodyText": "Answer body with ticket-1234",
            "author": {"login": "taka"},
            "createdAt": "2026-04-01T12:20:00Z",
            "updatedAt": "2026-04-01T12:25:00Z",
        },
    }


def _workflow_run_payload(run_id: int = 1001, name: str = "CI") -> dict:
    return {
        "id": run_id,
        "name": name,
        "display_title": "Fix ticket-1234 validation",
        "status": "completed",
        "conclusion": "success",
        "event": "push",
        "head_branch": "feature/ticket-1234",
        "head_sha": "abc123def456",
        "html_url": f"https://github.com/taka/repo/actions/runs/{run_id}",
        "created_at": "2026-04-01T12:00:00Z",
        "run_started_at": "2026-04-01T12:05:00Z",
        "updated_at": "2026-04-01T12:08:30Z",
        "actor": {"login": "taka"},
    }


class TestGitHubActivityModel:
    def test_to_dict_serializes_datetimes_and_labels(self):
        activity = GitHubActivity(
            repo_name="repo",
            activity_type="issue",
            number=1,
            title="Title",
            state="open",
            author="taka",
            url="url",
            updated_at=TIMESTAMP,
            created_at=TIMESTAMP,
            labels=["bug"],
        )

        assert activity.activity_id == "repo#1:issue"
        assert activity.to_dict()["updated_at"] == "2026-04-01T12:00:00+00:00"
        assert activity.to_dict()["labels"] == ["bug"]
        assert activity.to_dict()["metadata"] == {}


class TestGitHubActivityClient:
    def test_normalizes_configured_repositories(self):
        client = GitHubActivityClient("tok", "taka")

        repos = client.get_configured_repos(["repo-a", "octo/repo-b"])

        assert repos == [
            {"owner": "taka", "name": "repo-a", "repo_name": "repo-a"},
            {"owner": "octo", "name": "repo-b", "repo_name": "octo/repo-b"},
        ]

    @patch("requests.get", create=True)
    def test_get_repo_issues_skips_pull_request_items(self, mock_get):
        pr_issue = _issue_payload(2, "PR issue")
        pr_issue["pull_request"] = {"url": "api-url"}
        mock_get.side_effect = [
            _mock_response(json_data=[_issue_payload(1), pr_issue]),
            _mock_response(json_data=[]),
        ]

        client = GitHubActivityClient("tok", "taka")
        issues = list(client.get_repo_issues("taka", "repo", since=TIMESTAMP, limit=1))

        assert len(issues) == 1
        assert issues[0].activity_type == "issue"
        assert issues[0].labels == ["bug"]
        assert issues[0].metadata["issue_event_key"] == "repo#issue:1:updated:2026-04-01T12:00:00+00:00"
        assert mock_get.call_args_list[0].kwargs["params"]["since"] == TIMESTAMP.isoformat()

    @patch("requests.get", create=True)
    def test_get_repo_issues_paginates_and_adds_latest_comment_metadata(self, mock_get):
        first = _issue_payload(1, "First", updated_at="2026-04-01T12:00:00Z")
        first["comments"] = 1
        pr_issue = _issue_payload(9, "PR filler", updated_at="2026-04-01T12:03:00Z")
        pr_issue["pull_request"] = {"url": "api-url"}
        second = _issue_payload(2, "Second", updated_at="2026-04-01T12:05:00Z")
        second["comments"] = 1
        mock_get.side_effect = [
            _mock_response(json_data=[first, pr_issue]),
            _mock_response(
                json_data=[
                    {
                        "id": 501,
                        "body": "Latest issue comment with ticket-1234",
                        "user": {"login": "octo"},
                        "html_url": "https://github.com/taka/repo/issues/1#issuecomment-501",
                        "created_at": "2026-04-01T12:01:00Z",
                        "updated_at": "2026-04-01T12:02:00Z",
                    }
                ]
            ),
            _mock_response(json_data=[]),
            _mock_response(json_data=[second]),
            _mock_response(
                json_data=[
                    {
                        "id": 502,
                        "body": "Second page comment",
                        "user": {"login": "taka"},
                        "html_url": "https://github.com/taka/repo/issues/2#issuecomment-502",
                        "created_at": "2026-04-01T12:06:00Z",
                        "updated_at": "2026-04-01T12:06:00Z",
                    }
                ]
            ),
            _mock_response(json_data=[]),
        ]

        client = GitHubActivityClient(
            "tok",
            "taka",
            redaction_patterns=[
                {"name": "ticket", "pattern": r"ticket-\d+", "placeholder": "[REDACTED_TICKET]"}
            ],
        )
        issues = list(client.get_repo_issues("taka", "repo", repo_name="repo", since=TIMESTAMP, limit=2))

        assert [issue.number for issue in issues] == [1, 2]
        assert issues[0].metadata["issue_event_type"] == "commented"
        assert issues[0].metadata["issue_event_key"] == "repo#issue:1:commented:501"
        assert issues[0].metadata["issue_event_author"] == "octo"
        assert "ticket-1234" not in issues[0].metadata["comment_excerpt"]
        assert "Latest comment:" in issues[0].body
        assert mock_get.call_args_list[0].kwargs["params"]["page"] == 1
        assert mock_get.call_args_list[3].kwargs["params"]["page"] == 2

    @patch("requests.get", create=True)
    def test_get_repo_issues_parses_closed_and_reopened_events(self, mock_get):
        issue = _issue_payload(3, "State changes", updated_at="2026-04-01T12:10:00Z")
        mock_get.side_effect = [
            _mock_response(json_data=[issue]),
            _mock_response(
                json_data=[
                    {
                        "id": 701,
                        "event": "closed",
                        "actor": {"login": "taka"},
                        "created_at": "2026-04-01T12:03:00Z",
                    },
                    {
                        "id": 702,
                        "event": "reopened",
                        "actor": {"login": "octo"},
                        "created_at": "2026-04-01T12:09:00Z",
                    },
                ]
            ),
            _mock_response(json_data=[]),
        ]

        client = GitHubActivityClient("tok", "taka")
        issues = list(client.get_repo_issues("taka", "repo", repo_name="repo", since=TIMESTAMP, limit=1))

        assert len(issues) == 1
        assert issues[0].metadata["issue_event_type"] == "reopened"
        assert issues[0].metadata["issue_event_key"] == "repo#issue:3:reopened:702"
        assert issues[0].metadata["issue_event_author"] == "octo"

    @patch("requests.get", create=True)
    def test_get_repo_pull_requests_stops_before_since(self, mock_get):
        old = _pull_payload(3, "Old")
        old["updated_at"] = "2026-03-01T12:00:00Z"
        detail = _pull_payload(2)
        detail["changed_files"] = 4
        detail["merged"] = True
        detail["additions"] = 20
        detail["deletions"] = 5
        detail["commits"] = 2
        mock_get.side_effect = [
            _mock_response(json_data=[_pull_payload(2), old]),
            _mock_response(json_data=detail),
        ]

        client = GitHubActivityClient("tok", "taka")
        pulls = list(client.get_repo_pull_requests("taka", "repo", since=TIMESTAMP))

        assert len(pulls) == 1
        assert pulls[0].activity_type == "pull_request"
        assert pulls[0].merged_at.isoformat() == "2026-04-01T12:20:00+00:00"
        assert pulls[0].metadata["merged"] is True
        assert pulls[0].metadata["changed_files"] == 4
        assert pulls[0].metadata["additions"] == 20
        assert mock_get.call_args_list[1].args[0].endswith("/repos/taka/repo/pulls/2")

    def test_pull_request_parser_redacts_title_and_body_excerpt(self):
        payload = _pull_payload(
            5,
            "Remove ticket-1234 from logs",
        )
        payload["body"] = "Body mentions ticket-1234 " + ("word " * 300)
        payload["changed_files"] = 1
        client = GitHubActivityClient(
            "tok",
            "taka",
            redaction_patterns=[
                {"name": "ticket", "pattern": r"ticket-\d+", "placeholder": "[REDACTED_TICKET]"}
            ],
        )

        activity = client._pull_request_to_activity(payload, "repo")

        assert "ticket-1234" not in activity.title
        assert "[REDACTED_TICKET]" in activity.title
        assert "ticket-1234" not in activity.body
        assert "[REDACTED_TICKET]" in activity.body
        assert len(activity.body) <= 1000

    @patch("requests.get", create=True)
    def test_get_repo_issue_comments_normalizes_comment_activity(self, mock_get):
        mock_get.return_value = _mock_response(json_data=[_issue_comment_payload()])

        client = GitHubActivityClient(
            "tok",
            "taka",
            redaction_patterns=[
                {"name": "ticket", "pattern": r"ticket-\d+", "placeholder": "[REDACTED_TICKET]"}
            ],
        )
        comments = list(client.get_repo_issue_comments("taka", "repo", repo_name="taka/repo", since=TIMESTAMP))

        assert len(comments) == 1
        comment = comments[0]
        assert comment.activity_type == "issue_comment"
        assert comment.number == 501
        assert comment.title == "Issue comment on #7"
        assert comment.author == "octo"
        assert comment.url.endswith("#issuecomment-501")
        assert "ticket-1234" not in comment.body
        assert comment.metadata["comment_id"] == 501
        assert comment.metadata["parent_issue_number"] == 7
        assert comment.metadata["parent_number"] == 7
        assert mock_get.call_args.kwargs["params"]["since"] == TIMESTAMP.isoformat()

    @patch("requests.get", create=True)
    def test_get_repo_review_comments_normalizes_comment_activity(self, mock_get):
        mock_get.return_value = _mock_response(json_data=[_review_comment_payload()])

        client = GitHubActivityClient(
            "tok",
            "taka",
            redaction_patterns=[
                {"name": "ticket", "pattern": r"ticket-\d+", "placeholder": "[REDACTED_TICKET]"}
            ],
        )
        comments = list(client.get_repo_review_comments("taka", "repo", repo_name="taka/repo", since=TIMESTAMP))

        assert len(comments) == 1
        comment = comments[0]
        assert comment.activity_type == "review_comment"
        assert comment.number == 601
        assert comment.title == "Review comment on #8"
        assert comment.author == "reviewer"
        assert "ticket-1234" not in comment.body
        assert comment.metadata["comment_id"] == 601
        assert comment.metadata["parent_pr_number"] == 8
        assert comment.metadata["parent_number"] == 8
        assert comment.metadata["pull_request_review_id"] == 77
        assert comment.metadata["path"] == "src/app.py"

    @patch("requests.get", create=True)
    def test_get_repo_releases_normalizes_metadata(self, mock_get):
        old = _release_payload(102, "v0.9.0")
        old["published_at"] = "2026-03-01T12:00:00Z"
        mock_get.return_value = _mock_response(json_data=[_release_payload(), old])

        client = GitHubActivityClient("tok", "taka")
        releases = list(client.get_repo_releases("taka", "repo", since=TIMESTAMP))

        assert len(releases) == 1
        release = releases[0]
        assert release.activity_type == "release"
        assert release.number == 101
        assert release.title == "Release 1.0.0"
        assert release.state == "prerelease"
        assert release.author == "taka"
        assert release.updated_at.isoformat() == "2026-04-01T12:00:00+00:00"
        assert release.created_at.isoformat() == "2026-04-01T10:00:00+00:00"
        assert release.metadata == {
            "release_id": 101,
            "tag_name": "v1.0.0",
            "target_commitish": "main",
            "published_at": "2026-04-01T12:00:00+00:00",
            "created_at": "2026-04-01T10:00:00+00:00",
            "draft": False,
            "prerelease": True,
        }

    @patch("requests.get", create=True)
    def test_get_repo_workflow_runs_paginates_limits_and_normalizes_metadata(self, mock_get):
        first = _workflow_run_payload(1001)
        filler = [_workflow_run_payload(2000 + index, f"CI {index}") for index in range(99)]
        second = _workflow_run_payload(1101, "Deploy")
        old = _workflow_run_payload(1003, "Old")
        old["updated_at"] = "2026-03-01T12:00:00Z"
        mock_get.side_effect = [
            _mock_response(json_data={"workflow_runs": [first, *filler]}),
            _mock_response(json_data={"workflow_runs": [second, old]}),
        ]

        client = GitHubActivityClient(
            "tok",
            "taka",
            redaction_patterns=[
                {"name": "ticket", "pattern": r"ticket-\d+", "placeholder": "[REDACTED_TICKET]"}
            ],
        )
        runs = list(
            client.get_repo_workflow_runs(
                "taka",
                "repo",
                repo_name="taka/repo",
                since=TIMESTAMP,
                limit=101,
            )
        )

        assert len(runs) == 101
        assert runs[0].number == 1001
        assert runs[-1].number == 1101
        run = runs[0]
        assert run.activity_type == "workflow_run"
        assert run.repo_name == "taka/repo"
        assert run.title == "CI - Fix [REDACTED_TICKET] validation (success)"
        assert run.state == "success"
        assert run.author == "taka"
        assert run.url == "https://github.com/taka/repo/actions/runs/1001"
        assert run.updated_at.isoformat() == "2026-04-01T12:08:30+00:00"
        assert run.created_at.isoformat() == "2026-04-01T12:00:00+00:00"
        assert run.body == "event=push branch=feature/[REDACTED_TICKET] head_sha=abc123def456"
        assert run.metadata == {
            "workflow_name": "CI",
            "event": "push",
            "branch": "feature/[REDACTED_TICKET]",
            "head_sha": "abc123def456",
            "conclusion": "success",
            "duration_seconds": 210,
        }
        assert mock_get.call_args_list[0].kwargs["params"]["per_page"] == 100
        assert mock_get.call_args_list[0].kwargs["params"]["page"] == 1
        assert mock_get.call_args_list[1].kwargs["params"]["per_page"] == 1
        assert mock_get.call_args_list[1].kwargs["params"]["page"] == 2

    @patch("requests.post", create=True)
    def test_get_repo_discussions_normalizes_graphql_metadata_and_redacts(self, mock_post):
        mock_post.return_value = _mock_response(
            json_data={
                "data": {
                    "repository": {
                        "discussions": {
                            "nodes": [_discussion_payload()],
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                        }
                    }
                }
            }
        )

        client = GitHubActivityClient(
            "tok",
            "taka",
            redaction_patterns=[
                {"name": "ticket", "pattern": r"ticket-\d+", "placeholder": "[REDACTED_TICKET]"}
            ],
        )
        discussions = list(client.get_repo_discussions("taka", "repo", repo_name="taka/repo"))

        assert len(discussions) == 1
        discussion = discussions[0]
        assert discussion.activity_type == "discussion"
        assert discussion.number == 4
        assert discussion.title == "Discussion"
        assert discussion.state == "answered"
        assert discussion.author == "octo"
        assert discussion.body == "Discussion body with secret [REDACTED_TICKET]"
        assert discussion.metadata["category"] == {
            "name": "Q&A",
            "slug": "q-a",
            "emoji": ":bulb:",
        }
        assert discussion.metadata["discussion_url"] == "https://github.com/taka/repo/discussions/4"
        assert discussion.metadata["answer_state"] == "answered"
        assert discussion.metadata["comments_count"] == 3
        assert discussion.metadata["answer"]["chosen_by"] == "taka"
        assert discussion.metadata["answer"]["body"] == "Answer body with [REDACTED_TICKET]"
        assert mock_post.call_args.kwargs["json"]["variables"]["owner"] == "taka"

    @patch("requests.get", create=True)
    def test_auth_error_maps_to_shared_exception(self, mock_get):
        mock_get.return_value = _mock_response(status_code=401)
        client = GitHubActivityClient("bad", "taka")

        with pytest.raises(GitHubAuthError):
            list(client.get_repo_issues("taka", "repo"))

    @patch.object(GitHubActivityClient, "get_repo_workflow_runs")
    @patch.object(GitHubActivityClient, "get_repo_pull_requests")
    @patch.object(GitHubActivityClient, "get_repo_review_comments")
    @patch.object(GitHubActivityClient, "get_repo_issue_comments")
    @patch.object(GitHubActivityClient, "get_repo_releases")
    @patch.object(GitHubActivityClient, "get_repo_issues")
    @patch.object(GitHubActivityClient, "get_repo_discussions")
    @patch.object(GitHubActivityClient, "get_configured_repos")
    def test_get_all_recent_activity_skips_not_found_repos(
        self,
        mock_repos,
        mock_discussions,
        mock_issues,
        mock_releases,
        mock_issue_comments,
        mock_review_comments,
        mock_pulls,
        mock_workflows,
    ):
        mock_repos.return_value = [
            {"owner": "taka", "name": "missing", "repo_name": "missing"},
            {"owner": "taka", "name": "repo", "repo_name": "repo"},
        ]
        activity = GitHubActivity(
            repo_name="repo",
            activity_type="issue",
            number=1,
            title="Issue",
            state="open",
            author="taka",
            url="url",
            updated_at=TIMESTAMP,
            created_at=TIMESTAMP,
        )

        def issue_side_effect(owner, repo, **kwargs):
            if repo == "missing":
                raise GitHubNotFoundError("missing")
            return iter([activity])

        mock_issues.side_effect = issue_side_effect
        mock_pulls.return_value = iter([])
        mock_issue_comments.return_value = iter([])
        mock_review_comments.return_value = iter([])
        mock_releases.return_value = iter([])
        mock_discussions.return_value = iter([])
        mock_workflows.return_value = iter([])

        client = GitHubActivityClient("tok", "taka")
        results = list(
            client.get_all_recent_activity(
                include_discussions=True,
                include_pull_requests=True,
                include_comments=True,
                include_workflow_runs=True,
                include_releases=True,
            )
        )

        assert results == [activity]
        assert mock_discussions.call_count == 1
        assert mock_pulls.call_count == 1
        assert mock_issue_comments.call_count == 1
        assert mock_review_comments.call_count == 1
        assert mock_workflows.call_count == 1

    @patch.object(GitHubActivityClient, "get_repo_workflow_runs")
    @patch.object(GitHubActivityClient, "get_repo_pull_requests")
    @patch.object(GitHubActivityClient, "get_repo_releases")
    @patch.object(GitHubActivityClient, "get_repo_issues")
    @patch.object(GitHubActivityClient, "get_configured_repos")
    def test_get_all_recent_activity_skips_pull_requests_by_default(
        self, mock_repos, mock_issues, mock_releases, mock_pulls, mock_workflows
    ):
        mock_repos.return_value = [{"owner": "taka", "name": "repo", "repo_name": "repo"}]
        mock_issues.return_value = iter([])
        mock_releases.return_value = iter([])

        client = GitHubActivityClient("tok", "taka")

        assert list(client.get_all_recent_activity()) == []
        mock_pulls.assert_not_called()
        mock_workflows.assert_not_called()
        mock_releases.assert_not_called()
        mock_workflows.assert_not_called()
        mock_releases.assert_not_called()

    @patch.object(GitHubActivityClient, "get_repo_discussions")
    @patch.object(GitHubActivityClient, "get_repo_releases")
    @patch.object(GitHubActivityClient, "get_repo_issues")
    @patch.object(GitHubActivityClient, "get_configured_repos")
    def test_get_all_recent_activity_skips_discussions_when_disabled(
        self, mock_repos, mock_issues, mock_releases, mock_discussions
    ):
        mock_repos.return_value = [{"owner": "taka", "name": "repo", "repo_name": "repo"}]
        mock_issues.return_value = iter([])
        mock_releases.return_value = iter([])

        client = GitHubActivityClient("tok", "taka")

        assert list(client.get_all_recent_activity(include_discussions=False)) == []
        mock_discussions.assert_not_called()
    @patch.object(GitHubActivityClient, "get_repo_workflow_runs")
    @patch.object(GitHubActivityClient, "get_repo_releases")
    @patch.object(GitHubActivityClient, "get_repo_issues")
    @patch.object(GitHubActivityClient, "get_configured_repos")
    def test_get_all_recent_activity_includes_releases_when_enabled(
        self, mock_repos, mock_issues, mock_releases, mock_workflows
    ):
        mock_repos.return_value = [{"owner": "taka", "name": "repo", "repo_name": "repo"}]
        mock_releases.return_value = iter([])

        client = GitHubActivityClient("tok", "taka")

        assert list(
            client.get_all_recent_activity(
                include_issues=False,
                include_releases=True,
            )
        ) == []
        mock_issues.assert_not_called()
        mock_releases.assert_called_once()
        mock_workflows.assert_not_called()

    @patch.object(GitHubActivityClient, "get_repo_workflow_runs")
    @patch.object(GitHubActivityClient, "get_repo_releases")
    @patch.object(GitHubActivityClient, "get_repo_issues")
    @patch.object(GitHubActivityClient, "get_configured_repos")
    def test_get_all_recent_activity_includes_workflow_runs_only_when_enabled(
        self, mock_repos, mock_issues, mock_releases, mock_workflows
    ):
        activity = GitHubActivity(
            repo_name="repo",
            activity_type="workflow_run",
            number=1001,
            title="CI (success)",
            state="success",
            author="taka",
            url="url",
            updated_at=TIMESTAMP,
            created_at=TIMESTAMP,
        )
        mock_repos.return_value = [{"owner": "taka", "name": "repo", "repo_name": "repo"}]
        mock_issues.return_value = iter([])
        mock_releases.return_value = iter([])
        mock_workflows.return_value = iter([activity])

        client = GitHubActivityClient("tok", "taka")

        assert list(client.get_all_recent_activity(include_workflow_runs=True)) == [activity]
        mock_workflows.assert_called_once()

    @patch.object(GitHubActivityClient, "get_repo_review_comments")
    @patch.object(GitHubActivityClient, "get_repo_issue_comments")
    @patch.object(GitHubActivityClient, "get_repo_releases")
    @patch.object(GitHubActivityClient, "get_repo_issues")
    @patch.object(GitHubActivityClient, "get_configured_repos")
    def test_get_all_recent_activity_includes_comments_only_when_enabled(
        self, mock_repos, mock_issues, mock_releases, mock_issue_comments, mock_review_comments
    ):
        mock_repos.return_value = [{"owner": "taka", "name": "repo", "repo_name": "repo"}]
        mock_issues.return_value = iter([])
        mock_releases.return_value = iter([])
        mock_issue_comments.return_value = iter([])
        mock_review_comments.return_value = iter([])

        client = GitHubActivityClient("tok", "taka")

        assert list(client.get_all_recent_activity()) == []
        mock_issue_comments.assert_not_called()
        mock_review_comments.assert_not_called()

        assert list(client.get_all_recent_activity(include_comments=True)) == []
        mock_issue_comments.assert_called_once()
        mock_review_comments.assert_called_once()


class TestPollNewActivity:
    @patch.object(GitHubActivityClient, "get_all_recent_activity")
    def test_persists_only_new_or_updated_activity(self, mock_activity):
        new = GitHubActivity(
            repo_name="repo",
            activity_type="issue",
            number=1,
            title="Issue",
            state="open",
            author="taka",
            url="url",
            updated_at=TIMESTAMP,
            created_at=TIMESTAMP,
        )
        existing = GitHubActivity(
            repo_name="repo",
            activity_type="pull_request",
            number=2,
            title="PR",
            state="open",
            author="taka",
            url="url",
            updated_at=TIMESTAMP,
            created_at=TIMESTAMP,
        )
        mock_activity.return_value = iter([new, existing])
        db = MagicMock()
        db.is_github_activity_processed.side_effect = [False, True]

        result = poll_new_activity(
            "tok",
            "taka",
            TIMESTAMP,
            db,
            include_discussions=True,
            include_pull_requests=True,
            include_issues=False,
            include_comments=True,
            include_workflow_runs=True,
            include_releases=True,
        )

        assert result == [new]
        db.upsert_github_activity.assert_called_once()
        mock_activity.assert_called_once()
        assert mock_activity.call_args.kwargs["include_discussions"] is True
        assert mock_activity.call_args.kwargs["include_pull_requests"] is True
        assert mock_activity.call_args.kwargs["include_issues"] is False
        assert mock_activity.call_args.kwargs["include_comments"] is True
        assert mock_activity.call_args.kwargs["include_workflow_runs"] is True
        assert mock_activity.call_args.kwargs["include_releases"] is True

    @patch.object(GitHubActivityClient, "get_all_recent_activity")
    def test_persists_workflow_run_fields(self, mock_activity, db):
        workflow_run = GitHubActivity(
            repo_name="repo",
            activity_type="workflow_run",
            number=1001,
            title="CI (success)",
            state="success",
            author="taka",
            url="https://github.com/taka/repo/actions/runs/1001",
            updated_at=TIMESTAMP,
            created_at=TIMESTAMP,
            body="event=push branch=main head_sha=abc123",
            metadata={
                "workflow_name": "CI",
                "event": "push",
                "branch": "main",
                "head_sha": "abc123",
                "conclusion": "success",
                "duration_seconds": 30,
            },
        )
        mock_activity.return_value = iter([workflow_run])

        result = poll_new_activity(
            "tok",
            "taka",
            TIMESTAMP,
            db,
            include_workflow_runs=True,
        )
        rows = db.get_github_activity_in_range(TIMESTAMP, TIMESTAMP + timedelta(seconds=1))

        assert result == [workflow_run]
        assert len(rows) == 1
        assert rows[0]["activity_type"] == "workflow_run"
        assert rows[0]["number"] == 1001
        assert rows[0]["title"] == "CI (success)"
        assert rows[0]["state"] == "success"
        assert rows[0]["url"] == "https://github.com/taka/repo/actions/runs/1001"
        assert rows[0]["metadata"]["workflow_name"] == "CI"
        assert rows[0]["metadata"]["duration_seconds"] == 30

    @patch.object(GitHubActivityClient, "get_all_recent_activity")
    def test_dry_run_does_not_persist(self, mock_activity):
        activity = GitHubActivity(
            repo_name="repo",
            activity_type="issue",
            number=1,
            title="Issue",
            state="open",
            author="taka",
            url="url",
            updated_at=TIMESTAMP,
            created_at=TIMESTAMP,
        )
        mock_activity.return_value = iter([activity])
        db = MagicMock()
        db.is_github_activity_processed.return_value = False

        result = poll_new_activity("tok", "taka", TIMESTAMP, db, dry_run=True)

        assert result == [activity]
        db.upsert_github_activity.assert_not_called()
