"""Unit tests for src/ingestion/github_commits.py — GitHubClient and poll_new_commits."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
import requests

from ingestion.github_commits import (
    Commit,
    GitHubClient,
    GitHubAuthError,
    GitHubClientError,
    GitHubNotFoundError,
    GitHubRateLimitError,
    poll_new_commits,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TIMESTAMP = datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)

SAMPLE_COMMIT_PAYLOAD = {
    "sha": "abc123",
    "commit": {
        "message": "feat: add widget",
        "author": {
            "name": "taka",
            "date": "2026-04-01T12:00:00Z",
        },
    },
    "html_url": "https://github.com/taka/repo-a/commit/abc123",
}


def _make_repo(name: str, fork: bool = False) -> dict:
    return {"name": name, "fork": fork}


def _make_commit_payload(sha: str, message: str = "msg") -> dict:
    return {
        "sha": sha,
        "commit": {
            "message": message,
            "author": {"name": "taka", "date": "2026-04-01T12:00:00Z"},
        },
        "html_url": f"https://github.com/taka/repo/commit/{sha}",
    }


def _mock_response(status_code: int = 200, json_data=None):
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data if json_data is not None else []
    resp.raise_for_status.side_effect = (
        None
        if status_code < 400
        else requests.exceptions.HTTPError(response=resp)
    )
    return resp


# ---------------------------------------------------------------------------
# Commit.to_dict
# ---------------------------------------------------------------------------


class TestCommitToDict:
    def test_serialization_includes_all_fields(self):
        commit = Commit(
            repo_name="my-repo",
            sha="deadbeef",
            message="fix: typo",
            timestamp=TIMESTAMP,
            author="taka",
            url="https://github.com/taka/my-repo/commit/deadbeef",
        )
        d = commit.to_dict()
        assert d == {
            "repo_name": "my-repo",
            "sha": "deadbeef",
            "message": "fix: typo",
            "timestamp": "2026-04-01T12:00:00+00:00",
            "author": "taka",
            "url": "https://github.com/taka/my-repo/commit/deadbeef",
        }

    def test_timestamp_is_iso_string(self):
        commit = Commit("r", "s", "m", TIMESTAMP, "a", "u")
        assert isinstance(commit.to_dict()["timestamp"], str)


# ---------------------------------------------------------------------------
# GitHubClient.get_user_repos
# ---------------------------------------------------------------------------


class TestGetUserRepos:
    @patch("requests.get")
    def test_fetches_multiple_pages(self, mock_get):
        page1 = [_make_repo("repo-a"), _make_repo("repo-b")]
        page2 = [_make_repo("repo-c")]
        page3 = []  # signals end of pagination

        mock_get.side_effect = [
            _mock_response(json_data=page1),
            _mock_response(json_data=page2),
            _mock_response(json_data=page3),
        ]

        client = GitHubClient(token="tok", username="taka")
        repos = client.get_user_repos()

        assert len(repos) == 3
        assert [r["name"] for r in repos] == ["repo-a", "repo-b", "repo-c"]
        assert mock_get.call_count == 3

    @patch("requests.get")
    def test_excludes_forks_by_default(self, mock_get):
        repos_data = [
            _make_repo("own-repo", fork=False),
            _make_repo("forked-repo", fork=True),
        ]
        mock_get.side_effect = [
            _mock_response(json_data=repos_data),
            _mock_response(json_data=[]),
        ]

        client = GitHubClient(token="tok", username="taka")
        repos = client.get_user_repos(include_forks=False)

        assert len(repos) == 1
        assert repos[0]["name"] == "own-repo"

    @patch("requests.get")
    def test_includes_forks_when_requested(self, mock_get):
        repos_data = [
            _make_repo("own-repo", fork=False),
            _make_repo("forked-repo", fork=True),
        ]
        mock_get.side_effect = [
            _mock_response(json_data=repos_data),
            _mock_response(json_data=[]),
        ]

        client = GitHubClient(token="tok", username="taka")
        repos = client.get_user_repos(include_forks=True)

        assert len(repos) == 2
        assert {r["name"] for r in repos} == {"own-repo", "forked-repo"}

    @patch("requests.get")
    def test_401_raises_auth_error(self, mock_get):
        mock_get.return_value = _mock_response(status_code=401)

        client = GitHubClient(token="bad-token", username="taka")
        with pytest.raises(GitHubAuthError):
            client.get_user_repos()

    @patch("requests.get")
    def test_403_raises_rate_limit_error(self, mock_get):
        mock_get.return_value = _mock_response(status_code=403)

        client = GitHubClient(token="tok", username="taka")
        with pytest.raises(GitHubRateLimitError):
            client.get_user_repos()

    @patch("requests.get")
    def test_404_raises_not_found_error(self, mock_get):
        mock_get.return_value = _mock_response(status_code=404)

        client = GitHubClient(token="tok", username="taka")
        with pytest.raises(GitHubNotFoundError):
            client.get_user_repos()

    @patch("requests.get")
    def test_connection_error_raises_client_error(self, mock_get):
        mock_get.side_effect = requests.exceptions.ConnectionError("Network error")

        client = GitHubClient(token="tok", username="taka")
        with pytest.raises(GitHubClientError, match="Connection error"):
            client.get_user_repos()


# ---------------------------------------------------------------------------
# GitHubClient.get_repo_commits
# ---------------------------------------------------------------------------


class TestGetRepoCommits:
    @patch("requests.get")
    def test_parses_commit_fields(self, mock_get):
        mock_get.return_value = _mock_response(
            json_data=[SAMPLE_COMMIT_PAYLOAD]
        )

        client = GitHubClient(token="tok", username="taka")
        commits = list(client.get_repo_commits("repo-a"))

        assert len(commits) == 1
        c = commits[0]
        assert c.repo_name == "repo-a"
        assert c.sha == "abc123"
        assert c.message == "feat: add widget"
        assert c.author == "taka"
        assert c.url == "https://github.com/taka/repo-a/commit/abc123"
        assert c.timestamp == datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)

    @patch("requests.get")
    def test_since_parameter_passed_as_iso(self, mock_get):
        mock_get.return_value = _mock_response(json_data=[])

        client = GitHubClient(token="tok", username="taka")
        since = datetime(2026, 3, 30, 0, 0, 0, tzinfo=timezone.utc)
        list(client.get_repo_commits("repo-a", since=since))

        call_kwargs = mock_get.call_args
        assert call_kwargs.kwargs["params"]["since"] == since.isoformat()

    @patch("requests.get")
    def test_409_empty_repo_returns_no_results(self, mock_get):
        mock_get.return_value = _mock_response(status_code=409)

        client = GitHubClient(token="tok", username="taka")
        commits = list(client.get_repo_commits("empty-repo"))

        assert commits == []

    @patch("requests.get")
    def test_other_http_errors_propagate(self, mock_get):
        mock_get.return_value = _mock_response(status_code=500)

        client = GitHubClient(token="tok", username="taka")
        with pytest.raises(GitHubClientError):
            list(client.get_repo_commits("bad-repo"))

    @patch("requests.get")
    def test_401_raises_auth_error(self, mock_get):
        mock_get.return_value = _mock_response(status_code=401)

        client = GitHubClient(token="bad-token", username="taka")
        with pytest.raises(GitHubAuthError):
            list(client.get_repo_commits("repo-a"))

    @patch("requests.get")
    def test_403_raises_rate_limit_error(self, mock_get):
        mock_get.return_value = _mock_response(status_code=403)

        client = GitHubClient(token="tok", username="taka")
        with pytest.raises(GitHubRateLimitError):
            list(client.get_repo_commits("repo-a"))

    @patch("requests.get")
    def test_404_raises_not_found_error(self, mock_get):
        mock_get.return_value = _mock_response(status_code=404)

        client = GitHubClient(token="tok", username="taka")
        with pytest.raises(GitHubNotFoundError):
            list(client.get_repo_commits("nonexistent"))

    @patch("requests.get")
    def test_connection_error_raises_client_error(self, mock_get):
        mock_get.side_effect = requests.exceptions.ConnectionError("Network error")

        client = GitHubClient(token="tok", username="taka")
        with pytest.raises(GitHubClientError, match="Connection error"):
            list(client.get_repo_commits("repo-a"))


# ---------------------------------------------------------------------------
# GitHubClient.get_all_recent_commits
# ---------------------------------------------------------------------------


class TestGetAllRecentCommits:
    @patch.object(GitHubClient, "get_repo_commits")
    @patch.object(GitHubClient, "get_user_repos")
    def test_yields_commits_from_all_repos(self, mock_repos, mock_commits):
        mock_repos.return_value = [_make_repo("repo-a"), _make_repo("repo-b")]

        commit_a = Commit("repo-a", "sha1", "msg1", TIMESTAMP, "taka", "url1")
        commit_b = Commit("repo-b", "sha2", "msg2", TIMESTAMP, "taka", "url2")
        mock_commits.side_effect = [iter([commit_a]), iter([commit_b])]

        client = GitHubClient(token="tok", username="taka")
        all_commits = list(client.get_all_recent_commits())

        assert len(all_commits) == 2
        assert all_commits[0].sha == "sha1"
        assert all_commits[1].sha == "sha2"

    @patch.object(GitHubClient, "get_repo_commits")
    @patch.object(GitHubClient, "get_user_repos")
    def test_skips_403_and_404_repos(self, mock_repos, mock_commits):
        mock_repos.return_value = [
            _make_repo("private"),
            _make_repo("ok-repo"),
        ]

        commit_ok = Commit("ok-repo", "sha-ok", "m", TIMESTAMP, "taka", "u")

        # The side_effect for "private" is an exception, so we need to
        # make get_repo_commits raise on first call and yield on second.
        def side_effect(repo_name, **kwargs):
            if repo_name == "private":
                raise GitHubRateLimitError("Rate limit exceeded")
            return iter([commit_ok])

        mock_commits.side_effect = side_effect

        client = GitHubClient(token="tok", username="taka")
        all_commits = list(client.get_all_recent_commits())

        assert len(all_commits) == 1
        assert all_commits[0].repo_name == "ok-repo"

    @patch.object(GitHubClient, "get_repo_commits")
    @patch.object(GitHubClient, "get_user_repos")
    def test_skips_404_repos(self, mock_repos, mock_commits):
        mock_repos.return_value = [_make_repo("gone"), _make_repo("ok")]

        commit_ok = Commit("ok", "sha1", "m", TIMESTAMP, "taka", "u")

        def side_effect(repo_name, **kwargs):
            if repo_name == "gone":
                raise GitHubNotFoundError("Not found")
            return iter([commit_ok])

        mock_commits.side_effect = side_effect

        client = GitHubClient(token="tok", username="taka")
        all_commits = list(client.get_all_recent_commits())

        assert len(all_commits) == 1
        assert all_commits[0].repo_name == "ok"

    @patch.object(GitHubClient, "get_repo_commits")
    @patch.object(GitHubClient, "get_user_repos")
    def test_500_error_propagates(self, mock_repos, mock_commits):
        mock_repos.return_value = [_make_repo("broken")]

        mock_commits.side_effect = GitHubClientError("HTTP error: 500")

        client = GitHubClient(token="tok", username="taka")
        with pytest.raises(GitHubClientError):
            list(client.get_all_recent_commits())


# ---------------------------------------------------------------------------
# poll_new_commits
# ---------------------------------------------------------------------------


class TestPollNewCommits:
    def test_inserts_and_returns_only_new_commits(self):
        commit_new = Commit("repo", "sha-new", "new", TIMESTAMP, "taka", "u1")
        commit_existing = Commit("repo", "sha-old", "old", TIMESTAMP, "taka", "u2")

        with patch.object(GitHubClient, "get_all_recent_commits") as mock_all:
            mock_all.return_value = iter([commit_new, commit_existing])

            db = MagicMock()
            db.is_commit_processed.side_effect = (
                lambda sha: sha == "sha-old"
            )

            result = poll_new_commits(
                token="tok",
                username="taka",
                since=TIMESTAMP,
                db=db,
            )

        assert len(result) == 1
        assert result[0].sha == "sha-new"

        db.insert_commit.assert_called_once_with(
            repo_name="repo",
            commit_sha="sha-new",
            commit_message="new",
            timestamp=TIMESTAMP.isoformat(),
            author="taka",
        )

    def test_no_new_commits_returns_empty_list(self):
        commit = Commit("repo", "sha-old", "old", TIMESTAMP, "taka", "u")

        with patch.object(GitHubClient, "get_all_recent_commits") as mock_all:
            mock_all.return_value = iter([commit])

            db = MagicMock()
            db.is_commit_processed.return_value = True

            result = poll_new_commits(
                token="tok",
                username="taka",
                since=TIMESTAMP,
                db=db,
            )

        assert result == []
        db.insert_commit.assert_not_called()
