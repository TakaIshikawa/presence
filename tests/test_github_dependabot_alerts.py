"""Tests for GitHub Dependabot alert ingestion."""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import requests

from ingestion.github_dependabot_alerts import (
    ACTIVITY_TYPE,
    GitHubDependabotAlertClient,
    normalize_dependabot_alert_payload,
    poll_dependabot_alerts,
)

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from poll_github_dependabot_alerts import (  # noqa: E402
    format_dependabot_alert_json,
    format_dependabot_alert_text,
    main,
)


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


def _alert_payload(
    number: int = 7,
    *,
    state: str = "open",
    severity: str = "high",
    created_at: str = "2026-04-01T12:00:00Z",
    fixed_at: str | None = None,
    dismissed_at: str | None = None,
) -> dict:
    return {
        "number": number,
        "state": state,
        "dependency": {
            "package": {"ecosystem": "pip", "name": "parser-lib"},
            "manifest_path": "requirements.txt",
            "scope": "runtime",
        },
        "security_advisory": {
            "ghsa_id": "GHSA-aaaa-bbbb-cccc",
            "cve_id": "CVE-2026-0001",
            "summary": "parser-lib allows ticket-1234 bypass",
            "severity": severity,
            "url": "https://api.github.com/advisories/GHSA-aaaa-bbbb-cccc",
            "identifiers": [
                {"type": "GHSA", "value": "GHSA-aaaa-bbbb-cccc"},
                {"type": "CVE", "value": "CVE-2026-0001"},
            ],
            "cvss": {"score": 8.1},
            "cwes": [{"cwe_id": "CWE-79", "name": "XSS"}],
        },
        "security_vulnerability": {
            "package": {"ecosystem": "pip", "name": "parser-lib"},
            "vulnerable_version_range": "< 1.2.3",
            "patched_versions": ">= 1.2.3",
        },
        "html_url": "https://github.com/acme/widget/security/dependabot/7",
        "url": "https://api.github.com/repos/acme/widget/dependabot/alerts/7",
        "created_at": created_at,
        "fixed_at": fixed_at,
        "dismissed_at": dismissed_at,
        "dismissed_reason": None,
        "dismissed_comment": None,
    }


class TestNormalizeDependabotAlert:
    def test_normalizes_to_deterministic_activity_dictionary_and_redacts(self):
        client = GitHubDependabotAlertClient(
            "tok",
            "taka",
            redaction_patterns=[
                {"name": "ticket", "pattern": r"ticket-\d+", "placeholder": "[REDACTED_TICKET]"}
            ],
        )

        alert = normalize_dependabot_alert_payload(
            _alert_payload(),
            "acme/widget",
            redactor=client.redactor,
        )
        activity = alert.to_activity_dict()

        assert alert.external_id == "dependabot_alert:acme/widget:7"
        assert activity["repo_name"] == "acme/widget"
        assert activity["activity_type"] == ACTIVITY_TYPE
        assert activity["number"] == 7
        assert activity["title"] == "Dependabot high alert for parser-lib (pip)"
        assert activity["state"] == "open"
        assert activity["updated_at"] == "2026-04-01T12:00:00+00:00"
        assert activity["metadata"]["external_id"] == "dependabot_alert:acme/widget:7"
        assert activity["metadata"]["package"] == "parser-lib"
        assert activity["metadata"]["ecosystem"] == "pip"
        assert activity["metadata"]["severity"] == "high"
        assert activity["metadata"]["ghsa_id"] == "GHSA-aaaa-bbbb-cccc"
        assert activity["metadata"]["cve_id"] == "CVE-2026-0001"
        assert activity["metadata"]["advisory_summary"] == (
            "parser-lib allows [REDACTED_TICKET] bypass"
        )
        assert "ticket-1234" not in activity["body"]

    def test_uses_fixed_or_dismissed_time_as_updated_time(self):
        alert = normalize_dependabot_alert_payload(
            _alert_payload(state="fixed", fixed_at="2026-04-02T12:00:00Z"),
            "acme/widget",
        )
        activity = alert.to_activity_dict()

        assert activity["state"] == "fixed"
        assert activity["updated_at"] == "2026-04-02T12:00:00+00:00"
        assert activity["closed_at"] == "2026-04-02T12:00:00+00:00"
        assert activity["metadata"]["fixed_at"] == "2026-04-02T12:00:00+00:00"


class TestGitHubDependabotAlertClient:
    def test_get_repo_dependabot_alerts_paginates_and_filters_state(self):
        first_page = [_alert_payload(number) for number in range(1, 101)]
        second_page = [_alert_payload(101)]
        session = MagicMock()
        session.get.side_effect = [
            _mock_response(json_data=first_page),
            _mock_response(json_data=second_page),
        ]
        client = GitHubDependabotAlertClient("tok", "taka", session=session)

        alerts = list(
            client.get_repo_dependabot_alerts(
                "acme",
                "widget",
                repo_name="acme/widget",
                state="open",
                limit=101,
            )
        )

        assert alerts[0].number == 1
        assert alerts[-1].number == 101
        assert len(alerts) == 101
        assert session.get.call_args_list[0].kwargs["params"]["state"] == "open"
        assert session.get.call_args_list[0].kwargs["params"]["sort"] == "created"
        assert session.get.call_args_list[0].kwargs["params"]["direction"] == "desc"
        assert session.get.call_args_list[0].kwargs["params"]["page"] == 1
        assert session.get.call_args_list[1].kwargs["params"]["page"] == 2


class TestPollDependabotAlerts:
    @patch.object(GitHubDependabotAlertClient, "get_all_dependabot_alerts")
    def test_persists_only_new_unique_alerts(self, mock_alerts):
        new_alert = normalize_dependabot_alert_payload(_alert_payload(7), "acme/widget")
        duplicate = normalize_dependabot_alert_payload(_alert_payload(7), "acme/widget")
        existing = normalize_dependabot_alert_payload(_alert_payload(8), "acme/widget")
        mock_alerts.return_value = iter([new_alert, duplicate, existing])
        db = MagicMock()
        db.is_github_activity_processed.side_effect = [False, True]

        result = poll_dependabot_alerts("tok", "taka", db, repositories=["acme/widget"])

        assert result == [new_alert]
        assert db.is_github_activity_processed.call_count == 2
        db.upsert_github_activity.assert_called_once_with(**new_alert.to_activity_dict())

    @patch.object(GitHubDependabotAlertClient, "get_all_dependabot_alerts")
    def test_dry_run_does_not_persist(self, mock_alerts):
        alert = normalize_dependabot_alert_payload(_alert_payload(), "acme/widget")
        mock_alerts.return_value = iter([alert])
        db = MagicMock()
        db.is_github_activity_processed.return_value = False

        assert poll_dependabot_alerts("tok", "taka", db, dry_run=True) == [alert]
        db.upsert_github_activity.assert_not_called()

    @patch.object(GitHubDependabotAlertClient, "get_all_dependabot_alerts")
    def test_persists_to_github_activity_and_updates_existing_row(self, mock_alerts, db):
        first = normalize_dependabot_alert_payload(_alert_payload(severity="medium"), "acme/widget")
        mock_alerts.return_value = iter([first])

        assert poll_dependabot_alerts("tok", "taka", db) == [first]

        updated = normalize_dependabot_alert_payload(
            _alert_payload(
                state="fixed",
                severity="critical",
                fixed_at="2026-04-02T12:00:00Z",
            ),
            "acme/widget",
        )
        mock_alerts.return_value = iter([updated])

        assert poll_dependabot_alerts("tok", "taka", db) == [updated]

        rows = db.get_github_activity_in_range(
            datetime(2026, 4, 1, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2026, 4, 3, 0, 0, 0, tzinfo=timezone.utc),
            activity_type=ACTIVITY_TYPE,
        )
        assert len(rows) == 1
        assert rows[0]["number"] == 7
        assert rows[0]["state"] == "fixed"
        assert rows[0]["metadata"]["external_id"] == "dependabot_alert:acme/widget:7"
        assert rows[0]["metadata"]["severity"] == "critical"
        assert rows[0]["metadata"]["fixed_at"] == "2026-04-02T12:00:00+00:00"


def test_format_dependabot_alert_text_and_json_are_deterministic():
    alert = normalize_dependabot_alert_payload(_alert_payload(), "acme/widget")

    text = format_dependabot_alert_text(alert)
    json_payload = json.loads(format_dependabot_alert_json(alert))

    assert text.startswith("dependabot_alert:acme/widget:7 severity=high state=open")
    assert "package=parser-lib ecosystem=pip" in text
    assert json_payload["metadata"]["external_id"] == "dependabot_alert:acme/widget:7"
    assert list(json_payload.keys()) == sorted(json_payload.keys())


@patch("poll_github_dependabot_alerts.update_monitoring")
@patch("poll_github_dependabot_alerts.ingest_github_dependabot_alerts")
@patch("poll_github_dependabot_alerts.script_context")
def test_main_dry_run_prints_candidate_rows_without_writing(
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
    alert = normalize_dependabot_alert_payload(_alert_payload(), "acme/widget")
    mock_ingest.return_value = [alert]

    assert main(["--dry-run", "--format", "json", "--state", "open", "--limit", "25"]) == 0

    out = capsys.readouterr().out
    assert "Would ingest" in out
    payload = json.loads(out.split("Would ingest ", 1)[1])
    assert payload["activity_type"] == ACTIVITY_TYPE
    assert payload["metadata"]["external_id"] == "dependabot_alert:acme/widget:7"
    assert mock_ingest.call_args.kwargs["dry_run"] is True
    assert mock_ingest.call_args.kwargs["state"] == "open"
    assert mock_ingest.call_args.kwargs["limit"] == 25
    mock_update.assert_not_called()


@patch("poll_github_dependabot_alerts.update_monitoring")
@patch("poll_github_dependabot_alerts.ingest_github_dependabot_alerts")
@patch("poll_github_dependabot_alerts.script_context")
def test_main_updates_monitoring_when_not_dry_run(mock_context, mock_ingest, mock_update, db):
    config = MagicMock()
    config.github.token = "tok"
    config.github.username = "taka"
    config.github.repositories = []
    config.privacy.redaction_patterns = []
    config.timeouts.github_seconds = 10
    mock_context.return_value.__enter__.return_value = (config, db)
    mock_context.return_value.__exit__.return_value = None
    mock_ingest.return_value = []

    assert main(["--repo", "acme/widget"]) == 0

    assert mock_ingest.call_args.kwargs["repositories"] == ["acme/widget"]
    mock_update.assert_called_once_with("poll-github-dependabot-alerts")
