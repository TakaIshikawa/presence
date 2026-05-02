"""Tests for GitHub security advisory ingestion."""

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import requests

from ingestion.github_security_advisories import (
    ACTIVITY_TYPE,
    GitHubSecurityAdvisoryClient,
    normalize_security_advisory_payload,
    poll_security_advisories,
)

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from poll_github_security_advisories import CURSOR_KEY, determine_since, main, parse_since


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


def _advisory_payload(
    ghsa_id: str = "GHSA-aaaa-bbbb-cccc",
    *,
    severity: str = "high",
    updated_at: str = "2026-04-01T12:00:00Z",
    summary: str = "Dependency issue in ticket-1234 parser",
) -> dict:
    return {
        "id": 9001,
        "ghsa_id": ghsa_id,
        "node_id": f"RSA_{ghsa_id}",
        "summary": summary,
        "description": "Patch ticket-1234 parser dependency before exposing maintainers.",
        "severity": severity,
        "state": "published",
        "html_url": f"https://github.com/acme/widget/security/advisories/{ghsa_id}",
        "url": f"https://api.github.com/repos/acme/widget/security-advisories/{ghsa_id}",
        "publisher": {"login": "taka"},
        "identifiers": [
            {"type": "GHSA", "value": ghsa_id},
            {"type": "CVE", "value": "CVE-2026-0001"},
        ],
        "cvss": {"score": 8.1, "vector_string": "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:N"},
        "cwe_ids": ["CWE-79"],
        "vulnerabilities": [
            {
                "package": {"ecosystem": "pip", "name": "parser-lib"},
                "vulnerable_version_range": "< 1.2.3",
                "patched_versions": ">= 1.2.3",
            }
        ],
        "published_at": "2026-03-31T10:00:00Z",
        "updated_at": updated_at,
        "withdrawn_at": None,
    }


class TestNormalizeSecurityAdvisory:
    def test_normalizes_to_github_activity_metadata_shape_and_redacts(self):
        client = GitHubSecurityAdvisoryClient(
            "tok",
            "taka",
            redaction_patterns=[
                {"name": "ticket", "pattern": r"ticket-\d+", "placeholder": "[REDACTED_TICKET]"}
            ],
        )

        advisory = normalize_security_advisory_payload(
            _advisory_payload(),
            repo_name="acme/widget",
            redactor=client.redactor,
        )
        activity = advisory.to_activity_dict()

        assert advisory.activity_id == f"acme/widget#GHSA-aaaa-bbbb-cccc:{ACTIVITY_TYPE}"
        assert advisory.title == "Dependency issue in [REDACTED_TICKET] parser"
        assert "ticket-1234" not in advisory.body
        assert activity["repo_name"] == "acme/widget"
        assert activity["activity_type"] == ACTIVITY_TYPE
        assert activity["number"] == "GHSA-aaaa-bbbb-cccc"
        assert activity["state"] == "published"
        assert activity["author"] == "taka"
        assert activity["metadata"]["severity"] == "high"
        assert activity["metadata"]["cves"] == ["CVE-2026-0001"]
        assert activity["metadata"]["ecosystem"] == "pip"
        assert activity["metadata"]["ecosystems"] == ["pip"]
        assert activity["metadata"]["package_names"] == ["parser-lib"]
        assert activity["metadata"]["affected_packages"] == [
            {
                "ecosystem": "pip",
                "name": "parser-lib",
                "patched_versions": ">= 1.2.3",
                "vulnerable_version_range": "< 1.2.3",
            }
        ]
        assert activity["metadata"]["advisory_url"].endswith("/GHSA-aaaa-bbbb-cccc")
        assert activity["metadata"]["published_at"] == "2026-03-31T10:00:00+00:00"
        assert activity["metadata"]["updated_at"] == "2026-04-01T12:00:00+00:00"

    def test_uses_id_when_ghsa_id_is_missing_and_preserves_withdrawn_at(self):
        payload = _advisory_payload()
        payload.pop("ghsa_id")
        payload["id"] = 42
        payload["withdrawn_at"] = "2026-04-02T12:00:00Z"

        advisory = normalize_security_advisory_payload(payload, "acme/widget")
        activity = advisory.to_activity_dict()

        assert advisory.advisory_number == "42"
        assert activity["number"] == "42"
        assert activity["closed_at"] == "2026-04-02T12:00:00+00:00"
        assert activity["metadata"]["withdrawn_at"] == "2026-04-02T12:00:00+00:00"


class TestGitHubSecurityAdvisoryClient:
    def test_get_repo_security_advisories_paginates_and_filters_since(self):
        first = _advisory_payload("GHSA-1111-2222-3333")
        old = _advisory_payload("GHSA-old-old-old", updated_at="2026-03-01T12:00:00Z")
        older = _advisory_payload("GHSA-older-older-older", updated_at="2026-02-01T12:00:00Z")
        second = _advisory_payload("GHSA-4444-5555-6666")
        session = MagicMock()
        session.get.side_effect = [
            _mock_response(json_data=[first, old, older]),
            _mock_response(json_data=[second]),
        ]
        client = GitHubSecurityAdvisoryClient("tok", "taka", session=session)

        advisories = list(
            client.get_repo_security_advisories(
                "acme",
                "widget",
                repo_name="acme/widget",
                since=TIMESTAMP,
                limit=3,
            )
        )

        assert [advisory.advisory_number for advisory in advisories] == [
            "GHSA-1111-2222-3333",
            "GHSA-4444-5555-6666",
        ]
        assert session.get.call_args_list[0].kwargs["params"]["sort"] == "updated"
        assert session.get.call_args_list[0].kwargs["params"]["direction"] == "desc"
        assert session.get.call_args_list[0].kwargs["params"]["page"] == 1
        assert session.get.call_args_list[1].kwargs["params"]["page"] == 2


class TestPollSecurityAdvisories:
    @patch.object(GitHubSecurityAdvisoryClient, "get_all_recent_security_advisories")
    def test_persists_only_new_unique_advisories(self, mock_advisories):
        new_advisory = normalize_security_advisory_payload(_advisory_payload(), "acme/widget")
        existing = normalize_security_advisory_payload(
            _advisory_payload("GHSA-existing-existing-existing"),
            "acme/widget",
        )
        duplicate = normalize_security_advisory_payload(_advisory_payload(), "acme/widget")
        mock_advisories.return_value = iter([new_advisory, duplicate, existing])
        db = MagicMock()
        db.is_github_activity_processed.side_effect = [False, True]

        result = poll_security_advisories("tok", "taka", TIMESTAMP, db, repositories=["acme/widget"])

        assert result == [new_advisory]
        assert db.is_github_activity_processed.call_count == 2
        db.upsert_github_activity.assert_called_once_with(**new_advisory.to_activity_dict())

    @patch.object(GitHubSecurityAdvisoryClient, "get_all_recent_security_advisories")
    def test_dry_run_does_not_persist(self, mock_advisories):
        advisory = normalize_security_advisory_payload(_advisory_payload(), "acme/widget")
        mock_advisories.return_value = iter([advisory])
        db = MagicMock()
        db.is_github_activity_processed.return_value = False

        assert poll_security_advisories("tok", "taka", TIMESTAMP, db, dry_run=True) == [advisory]
        db.upsert_github_activity.assert_not_called()

    @patch.object(GitHubSecurityAdvisoryClient, "get_all_recent_security_advisories")
    def test_persists_to_github_activity_and_updates_existing_row(self, mock_advisories, db):
        first = normalize_security_advisory_payload(_advisory_payload(severity="medium"), "acme/widget")
        mock_advisories.return_value = iter([first])

        assert poll_security_advisories("tok", "taka", TIMESTAMP, db) == [first]

        updated_payload = _advisory_payload(severity="critical", updated_at="2026-04-02T12:00:00Z")
        updated_payload["vulnerabilities"][0]["package"]["name"] = "parser-lib-core"
        updated = normalize_security_advisory_payload(updated_payload, "acme/widget")
        mock_advisories.return_value = iter([updated])

        assert poll_security_advisories("tok", "taka", TIMESTAMP, db) == [updated]

        rows = db.get_github_activity_in_range(
            datetime(2026, 4, 1, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2026, 4, 3, 0, 0, 0, tzinfo=timezone.utc),
            activity_type=ACTIVITY_TYPE,
        )
        assert len(rows) == 1
        assert rows[0]["activity_type"] == ACTIVITY_TYPE
        assert rows[0]["number"] == "GHSA-aaaa-bbbb-cccc"
        assert rows[0]["metadata"]["severity"] == "critical"
        assert rows[0]["metadata"]["cves"] == ["CVE-2026-0001"]
        assert rows[0]["metadata"]["package_names"] == ["parser-lib-core"]
        assert rows[0]["metadata"]["affected_packages"][0]["name"] == "parser-lib-core"
        assert rows[0]["metadata"]["published_at"] == "2026-03-31T10:00:00+00:00"


def test_parse_since_accepts_z_suffix():
    assert parse_since("2026-04-01T12:00:00Z") == TIMESTAMP


def test_determine_since_uses_security_advisory_meta_cursor(db):
    db.set_meta(CURSOR_KEY, (TIMESTAMP - timedelta(hours=1)).isoformat())

    assert determine_since(db, None, 24) == TIMESTAMP - timedelta(hours=1)


@patch("poll_github_security_advisories.update_monitoring")
@patch("poll_github_security_advisories.ingest_github_security_advisories")
@patch("poll_github_security_advisories.script_context")
def test_main_dry_run_prints_candidate_rows_without_cursor(
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
    advisory = normalize_security_advisory_payload(_advisory_payload(), "acme/widget")
    mock_ingest.return_value = [advisory]

    assert main(["--dry-run", "--format", "json", "--since", "2026-04-01T12:00:00Z"]) == 0

    out = capsys.readouterr().out
    assert "Would ingest" in out
    payload = json.loads(out.split("Would ingest ", 1)[1])
    assert payload["activity_type"] == ACTIVITY_TYPE
    assert payload["metadata"]["severity"] == "high"
    assert payload["metadata"]["package_names"] == ["parser-lib"]
    assert db.get_meta(CURSOR_KEY) is None
    mock_update.assert_not_called()
    assert mock_ingest.call_args.kwargs["dry_run"] is True


@patch("poll_github_security_advisories.update_monitoring")
@patch("poll_github_security_advisories.ingest_github_security_advisories")
@patch("poll_github_security_advisories.script_context")
def test_main_persists_cursor_when_enabled(mock_context, mock_ingest, mock_update, db):
    config = MagicMock()
    config.github.token = "tok"
    config.github.username = "taka"
    config.github.repositories = []
    config.privacy.redaction_patterns = []
    config.timeouts.github_seconds = 10
    mock_context.return_value.__enter__.return_value = (config, db)
    mock_context.return_value.__exit__.return_value = None
    mock_ingest.return_value = []

    assert main(["--lookback-hours", "6", "--limit", "25", "--repo", "acme/widget"]) == 0

    assert db.get_meta(CURSOR_KEY) is not None
    assert mock_ingest.call_args.kwargs["repositories"] == ["acme/widget"]
    assert mock_ingest.call_args.kwargs["limit"] == 25
    mock_update.assert_called_once_with("poll-github-security-advisories")
