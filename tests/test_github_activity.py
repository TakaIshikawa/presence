"""Unit tests for GitHub issue, pull request, and release ingestion."""

from datetime import datetime, timezone
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


def _issue_payload(number: int = 1, title: str = "Issue") -> dict:
    return {
        "number": number,
        "title": title,
        "state": "open",
        "body": "Issue body",
        "user": {"login": "taka"},
        "html_url": f"https://github.com/taka/repo/issues/{number}",
        "updated_at": "2026-04-01T12:00:00Z",
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
        issues = list(client.get_repo_issues("taka", "repo", since=TIMESTAMP))

        assert len(issues) == 1
        assert issues[0].activity_type == "issue"
        assert issues[0].labels == ["bug"]
        assert mock_get.call_args_list[0].kwargs["params"]["since"] == TIMESTAMP.isoformat()

    @patch("requests.get", create=True)
    def test_get_repo_pull_requests_stops_before_since(self, mock_get):
        old = _pull_payload(3, "Old")
        old["updated_at"] = "2026-03-01T12:00:00Z"
        mock_get.return_value = _mock_response(json_data=[_pull_payload(2), old])

        client = GitHubActivityClient("tok", "taka")
        pulls = list(client.get_repo_pull_requests("taka", "repo", since=TIMESTAMP))

        assert len(pulls) == 1
        assert pulls[0].activity_type == "pull_request"
        assert pulls[0].merged_at.isoformat() == "2026-04-01T12:20:00+00:00"

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
    def test_auth_error_maps_to_shared_exception(self, mock_get):
        mock_get.return_value = _mock_response(status_code=401)
        client = GitHubActivityClient("bad", "taka")

        with pytest.raises(GitHubAuthError):
            list(client.get_repo_issues("taka", "repo"))

    @patch.object(GitHubActivityClient, "get_repo_pull_requests")
    @patch.object(GitHubActivityClient, "get_repo_releases")
    @patch.object(GitHubActivityClient, "get_repo_issues")
    @patch.object(GitHubActivityClient, "get_configured_repos")
    def test_get_all_recent_activity_skips_not_found_repos(
        self, mock_repos, mock_issues, mock_releases, mock_pulls
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
        mock_releases.return_value = iter([])

        client = GitHubActivityClient("tok", "taka")
        results = list(client.get_all_recent_activity())

        assert results == [activity]


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

        result = poll_new_activity("tok", "taka", TIMESTAMP, db)

        assert result == [new]
        db.upsert_github_activity.assert_called_once()

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
