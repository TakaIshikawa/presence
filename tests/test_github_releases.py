"""Tests for first-class GitHub release ingestion."""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import requests

from ingestion.github_releases import GitHubRelease, GitHubReleaseClient, poll_new_releases

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from poll_github_releases import determine_since, main, parse_since


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


def _release_payload(tag: str = "v1.2.3", release_id: int = 123) -> dict:
    return {
        "id": release_id,
        "tag_name": tag,
        "target_commitish": "main",
        "name": "Launch ticket-1234",
        "body": "Release body with ticket-1234 " + ("detail " * 250),
        "draft": False,
        "prerelease": False,
        "author": {"login": "taka"},
        "html_url": f"https://github.com/acme/widget/releases/tag/{tag}",
        "published_at": "2026-04-01T12:00:00Z",
        "created_at": "2026-04-01T10:00:00Z",
    }


class TestGitHubReleaseClient:
    @patch("requests.get", create=True)
    def test_get_repo_releases_normalizes_redacts_filters_and_paginates(self, mock_get):
        old = _release_payload("v1.0.0", 100)
        old["published_at"] = "2026-03-01T12:00:00Z"
        mock_get.side_effect = [
            _mock_response(json_data=[_release_payload(), old]),
        ]
        client = GitHubReleaseClient(
            "tok",
            "taka",
            redaction_patterns=[
                {"name": "ticket", "pattern": r"ticket-\d+", "placeholder": "[REDACTED_TICKET]"}
            ],
        )

        releases = list(
            client.get_repo_releases(
                "acme",
                "widget",
                repo_name="acme/widget",
                since=TIMESTAMP,
            )
        )

        assert len(releases) == 1
        release = releases[0]
        assert release.repo_name == "acme/widget"
        assert release.tag == "v1.2.3"
        assert release.activity_id == "acme/widget#v1.2.3:release"
        assert release.title == "Launch [REDACTED_TICKET]"
        assert "ticket-1234" not in release.body_excerpt
        assert "[REDACTED_TICKET]" in release.body_excerpt
        assert len(release.body_excerpt) <= 1000
        assert release.url == "https://github.com/acme/widget/releases/tag/v1.2.3"
        assert release.published_at.isoformat() == "2026-04-01T12:00:00+00:00"
        assert release.to_activity_dict()["number"] == "v1.2.3"
        assert release.to_activity_dict()["metadata"]["release_id"] == 123
        assert mock_get.call_args.kwargs["params"]["page"] == 1

    @patch("requests.get", create=True)
    def test_get_repo_releases_pages_until_limit(self, mock_get):
        first_page = [_release_payload(f"v1.0.{i}", i) for i in range(100)]
        second_page = [_release_payload("v1.0.100", 100)]
        mock_get.side_effect = [
            _mock_response(json_data=first_page),
            _mock_response(json_data=second_page),
        ]
        client = GitHubReleaseClient("tok", "taka")

        releases = list(client.get_repo_releases("acme", "widget", limit=101))

        assert len(releases) == 101
        assert mock_get.call_args_list[0].kwargs["params"]["page"] == 1
        assert mock_get.call_args_list[1].kwargs["params"]["page"] == 2


class TestPollNewReleases:
    @patch.object(GitHubReleaseClient, "get_all_recent_releases")
    def test_persists_only_new_releases(self, mock_releases):
        new_release = GitHubRelease(
            repo_name="acme/widget",
            tag="v1.2.3",
            title="Launch",
            body_excerpt="Notes",
            url="url",
            published_at=TIMESTAMP,
        )
        existing_release = GitHubRelease(
            repo_name="acme/widget",
            tag="v1.2.2",
            title="Old launch",
            body_excerpt="Notes",
            url="url",
            published_at=TIMESTAMP,
        )
        mock_releases.return_value = iter([new_release, existing_release])
        db = MagicMock()
        db.is_github_release_processed.side_effect = [False, True]

        result = poll_new_releases("tok", "taka", TIMESTAMP, db, repositories=["acme/widget"])

        assert result == [new_release]
        db.upsert_github_release.assert_called_once_with(new_release.to_activity_dict())

    @patch.object(GitHubReleaseClient, "get_all_recent_releases")
    def test_dry_run_does_not_persist(self, mock_releases):
        release = GitHubRelease(
            repo_name="acme/widget",
            tag="v1.2.3",
            title="Launch",
            body_excerpt="Notes",
            url="url",
            published_at=TIMESTAMP,
        )
        mock_releases.return_value = iter([release])
        db = MagicMock()
        db.is_github_release_processed.return_value = False

        assert poll_new_releases("tok", "taka", TIMESTAMP, db, dry_run=True) == [release]
        db.upsert_github_release.assert_not_called()


def test_parse_since_accepts_z_suffix():
    assert parse_since("2026-04-01T12:00:00Z") == TIMESTAMP


def test_determine_since_uses_release_poll_watermark(db):
    db.set_last_github_release_poll_time(TIMESTAMP - timedelta(hours=1))

    assert determine_since(db, None, 90) == TIMESTAMP - timedelta(hours=1)


@patch("poll_github_releases.update_monitoring")
@patch("poll_github_releases.ingest_github_releases")
@patch("poll_github_releases.script_context")
def test_main_skips_when_include_releases_false(mock_context, mock_ingest, mock_update, db):
    config = MagicMock()
    config.github.include_releases = False
    mock_context.return_value.__enter__.return_value = (config, db)
    mock_context.return_value.__exit__.return_value = None

    assert main(["--since", "2026-04-01T12:00:00Z"]) == 0

    mock_ingest.assert_not_called()
    mock_update.assert_not_called()


@patch("poll_github_releases.update_monitoring")
@patch("poll_github_releases.ingest_github_releases")
@patch("poll_github_releases.script_context")
def test_main_dry_run_prints_releases_without_watermark(
    mock_context,
    mock_ingest,
    mock_update,
    db,
    capsys,
):
    config = MagicMock()
    config.github.include_releases = True
    config.github.token = "tok"
    config.github.username = "taka"
    config.github.repositories = ["acme/widget"]
    config.privacy.redaction_patterns = []
    config.timeouts.github_seconds = 10
    mock_context.return_value.__enter__.return_value = (config, db)
    mock_context.return_value.__exit__.return_value = None
    release = GitHubRelease(
        repo_name="acme/widget",
        tag="v1.2.3",
        title="Launch",
        body_excerpt="Notes",
        url="https://github.com/acme/widget/releases/tag/v1.2.3",
        published_at=TIMESTAMP,
    )
    mock_ingest.return_value = [release]

    assert main(["--dry-run", "--since", "2026-04-01T12:00:00Z"]) == 0

    assert "Would ingest acme/widget#v1.2.3:release" in capsys.readouterr().out
    assert db.get_last_github_release_poll_time() is None
    mock_update.assert_not_called()
    assert mock_ingest.call_args.kwargs["dry_run"] is True


@patch("poll_github_releases.update_monitoring")
@patch("poll_github_releases.ingest_github_releases")
@patch("poll_github_releases.script_context")
def test_main_persists_watermark_when_enabled(mock_context, mock_ingest, mock_update, db):
    config = MagicMock()
    config.github.include_releases = True
    config.github.token = "tok"
    config.github.username = "taka"
    config.github.repositories = []
    config.privacy.redaction_patterns = []
    config.timeouts.github_seconds = 10
    mock_context.return_value.__enter__.return_value = (config, db)
    mock_context.return_value.__exit__.return_value = None
    mock_ingest.return_value = []

    assert main(["--since", "2026-04-01T12:00:00Z"]) == 0

    assert db.get_last_github_release_poll_time() is not None
    mock_update.assert_called_once_with("poll-github-releases")
