"""Tests for failed GitHub Actions workflow run ingestion."""

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import requests

from ingestion.github_workflow_runs import (
    ACTIVITY_TYPE,
    GitHubWorkflowRun,
    GitHubWorkflowRunClient,
    normalize_workflow_run_payload,
    poll_failed_workflow_runs,
)

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from poll_github_workflow_runs import CURSOR_KEY, determine_since, main, parse_since


TIMESTAMP = datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)

if not hasattr(requests, "exceptions"):
    requests.exceptions = SimpleNamespace(
        HTTPError=requests.HTTPError,
        ConnectionError=requests.ConnectionError,
    )


def _mock_response(status_code: int = 200, json_data=None):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data if json_data is not None else {}
    if status_code < 400:
        resp.raise_for_status.side_effect = None
    else:
        error = requests.exceptions.HTTPError("HTTP error")
        error.response = resp
        resp.raise_for_status.side_effect = error
    return resp


def _run_payload(
    run_id: int = 1001,
    *,
    conclusion: str | None = "failure",
    workflow_name: str = "Tests",
    updated_at: str = "2026-04-01T12:00:00Z",
) -> dict:
    return {
        "id": run_id,
        "node_id": f"WFR_{run_id}",
        "name": workflow_name,
        "display_title": "Fix ticket-1234 regression",
        "run_number": 88,
        "run_attempt": 2,
        "status": "completed",
        "conclusion": conclusion,
        "event": "push",
        "head_branch": "main",
        "head_sha": "abc123",
        "path": ".github/workflows/tests.yml",
        "actor": {"login": "taka"},
        "html_url": f"https://github.com/acme/widget/actions/runs/{run_id}",
        "workflow_id": 42,
        "check_suite_id": 7001,
        "created_at": "2026-04-01T11:00:00Z",
        "updated_at": updated_at,
        "run_started_at": "2026-04-01T11:05:00Z",
        "head_commit": {
            "id": "abc123",
            "message": "Fix ticket-1234 regression",
            "timestamp": "2026-04-01T10:59:00Z",
        },
    }


class TestNormalizeWorkflowRun:
    def test_normalizes_failure_to_activity_metadata_shape_and_redacts(self):
        client = GitHubWorkflowRunClient(
            "tok",
            "taka",
            redaction_patterns=[
                {"name": "ticket", "pattern": r"ticket-\d+", "placeholder": "[REDACTED_TICKET]"}
            ],
        )

        run = normalize_workflow_run_payload(
            _run_payload(),
            repo_name="acme/widget",
            redactor=client.redactor,
        )
        assert run is not None
        activity = run.to_activity_dict()

        assert run.activity_id == f"acme/widget#1001:{ACTIVITY_TYPE}"
        assert run.workflow_name == "Tests"
        assert run.conclusion == "failure"
        assert run.display_title == "Fix [REDACTED_TICKET] regression"
        assert "ticket-1234" not in run.body
        assert activity["repo_name"] == "acme/widget"
        assert activity["activity_type"] == ACTIVITY_TYPE
        assert activity["number"] == 1001
        assert activity["state"] == "failure"
        assert activity["metadata"]["workflow_name"] == "Tests"
        assert activity["metadata"]["conclusion"] == "failure"
        assert activity["metadata"]["branch"] == "main"
        assert activity["metadata"]["run_url"] == "https://github.com/acme/widget/actions/runs/1001"
        assert activity["metadata"]["run_number"] == 88
        assert activity["metadata"]["commit_sha"] == "abc123"
        assert activity["metadata"]["created_at"] == "2026-04-01T11:00:00+00:00"
        assert activity["metadata"]["updated_at"] == "2026-04-01T12:00:00+00:00"
        assert activity["metadata"]["run_started_at"] == "2026-04-01T11:05:00+00:00"

    def test_normalizes_cancelled_and_skips_success(self):
        cancelled = normalize_workflow_run_payload(_run_payload(conclusion="cancelled"), "acme/widget")
        success = normalize_workflow_run_payload(_run_payload(conclusion="success"), "acme/widget")

        assert cancelled is not None
        assert cancelled.to_activity_dict()["state"] == "cancelled"
        assert success is None


class TestGitHubWorkflowRunClient:
    def test_get_repo_workflow_runs_paginates_filters_since_and_conclusion(self):
        first_page = [_run_payload(run_id=2000 + index, conclusion="success") for index in range(99)]
        first_page.append(_run_payload(3000, conclusion="failure"))
        second = _run_payload(3001, conclusion="cancelled")
        old = _run_payload(3002, conclusion="failure", updated_at="2026-03-01T12:00:00Z")
        session = MagicMock()
        session.get.side_effect = [
            _mock_response(json_data={"workflow_runs": first_page}),
            _mock_response(json_data={"workflow_runs": [second, old]}),
        ]
        client = GitHubWorkflowRunClient("tok", "taka", session=session)

        runs = list(
            client.get_repo_workflow_runs(
                "acme",
                "widget",
                repo_name="acme/widget",
                since=TIMESTAMP,
                limit=2,
            )
        )

        assert [run.run_id for run in runs] == [3000, 3001]
        assert session.get.call_args_list[0].kwargs["params"]["status"] == "completed"
        assert session.get.call_args_list[0].kwargs["params"]["created"] == f">={TIMESTAMP.isoformat()}"
        assert session.get.call_args_list[0].kwargs["params"]["per_page"] == 2
        assert session.get.call_args_list[0].kwargs["params"]["page"] == 1
        assert session.get.call_args_list[1].kwargs["params"]["page"] == 2

    def test_get_repo_workflow_runs_handles_empty_response(self):
        session = MagicMock()
        session.get.return_value = _mock_response(json_data={"workflow_runs": []})
        client = GitHubWorkflowRunClient("tok", "taka", session=session)

        assert list(client.get_repo_workflow_runs("acme", "widget", since=TIMESTAMP)) == []
        session.get.assert_called_once()


class TestPollFailedWorkflowRuns:
    @patch.object(GitHubWorkflowRunClient, "get_all_recent_workflow_runs")
    def test_persists_only_new_unique_runs(self, mock_runs):
        new_run = normalize_workflow_run_payload(_run_payload(1001), "acme/widget")
        existing = normalize_workflow_run_payload(_run_payload(1002), "acme/widget")
        duplicate = GitHubWorkflowRun(**new_run.__dict__)
        mock_runs.return_value = iter([new_run, duplicate, existing])
        db = MagicMock()
        db.is_github_activity_processed.side_effect = [False, True]

        result = poll_failed_workflow_runs("tok", "taka", TIMESTAMP, db, repositories=["acme/widget"])

        assert result == [new_run]
        assert db.is_github_activity_processed.call_count == 2
        db.upsert_github_activity.assert_called_once_with(**new_run.to_activity_dict())

    @patch.object(GitHubWorkflowRunClient, "get_all_recent_workflow_runs")
    def test_dry_run_does_not_persist(self, mock_runs):
        run = normalize_workflow_run_payload(_run_payload(1001), "acme/widget")
        mock_runs.return_value = iter([run])
        db = MagicMock()
        db.is_github_activity_processed.return_value = False

        assert poll_failed_workflow_runs("tok", "taka", TIMESTAMP, db, dry_run=True) == [run]
        db.upsert_github_activity.assert_not_called()

    @patch.object(GitHubWorkflowRunClient, "get_all_recent_workflow_runs")
    def test_persists_to_github_activity_and_deduplicates_existing_rows(self, mock_runs, db):
        first = normalize_workflow_run_payload(_run_payload(1001), "acme/widget")
        mock_runs.return_value = iter([first])

        assert poll_failed_workflow_runs("tok", "taka", TIMESTAMP, db) == [first]

        duplicate = normalize_workflow_run_payload(_run_payload(1001), "acme/widget")
        mock_runs.return_value = iter([duplicate])
        assert poll_failed_workflow_runs("tok", "taka", TIMESTAMP, db) == []

        rows = db.get_github_workflow_runs(limit=10)
        assert len(rows) == 1
        assert rows[0]["activity_type"] == ACTIVITY_TYPE
        assert rows[0]["number"] == 1001
        assert rows[0]["metadata"]["run_id"] == 1001
        assert rows[0]["metadata"]["workflow_name"] == "Tests"
        assert rows[0]["metadata"]["conclusion"] == "failure"
        assert rows[0]["metadata"]["commit_sha"] == "abc123"


def test_parse_since_accepts_z_suffix():
    assert parse_since("2026-04-01T12:00:00Z") == TIMESTAMP


def test_determine_since_uses_workflow_run_meta_cursor(db):
    db.set_meta(CURSOR_KEY, (TIMESTAMP - timedelta(hours=1)).isoformat())

    assert determine_since(db, None, 24) == TIMESTAMP - timedelta(hours=1)


@patch("poll_github_workflow_runs.update_monitoring")
@patch("poll_github_workflow_runs.ingest_github_workflow_runs")
@patch("poll_github_workflow_runs.script_context")
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
    run = normalize_workflow_run_payload(_run_payload(1001), "acme/widget")
    mock_ingest.return_value = [run]

    assert main(["--dry-run", "--json", "--since", "2026-04-01T12:00:00Z"]) == 0

    out = capsys.readouterr().out
    assert "Would ingest" in out
    payload = json.loads(out.split("Would ingest ", 1)[1])
    assert payload["activity_type"] == ACTIVITY_TYPE
    assert payload["metadata"]["run_url"] == "https://github.com/acme/widget/actions/runs/1001"
    assert db.get_meta(CURSOR_KEY) is None
    mock_update.assert_not_called()
    assert mock_ingest.call_args.kwargs["dry_run"] is True


@patch("poll_github_workflow_runs.update_monitoring")
@patch("poll_github_workflow_runs.ingest_github_workflow_runs")
@patch("poll_github_workflow_runs.script_context")
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
    mock_update.assert_called_once_with("poll-github-workflow-runs")
