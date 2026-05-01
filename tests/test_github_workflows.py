"""Tests for GitHub Actions workflow run ingestion."""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import requests

from ingestion.github_workflows import (
    ACTIVITY_TYPE,
    GitHubWorkflowRun,
    GitHubWorkflowRunClient,
    normalize_workflow_run_payload,
    poll_new_workflow_runs,
)

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from poll_github_workflows import CURSOR_KEY, determine_since, main


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
    status: str = "completed",
    conclusion: str | None = "failure",
    workflow_name: str = "Tests",
) -> dict:
    return {
        "id": run_id,
        "node_id": f"WFR_{run_id}",
        "name": workflow_name,
        "display_title": "Fix ticket-1234 regression",
        "run_number": 88,
        "run_attempt": 2,
        "status": status,
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
        "updated_at": "2026-04-01T12:00:00Z",
        "run_started_at": "2026-04-01T11:05:00Z",
        "head_commit": {
            "id": "abc123",
            "message": "Fix ticket-1234 regression",
            "timestamp": "2026-04-01T10:59:00Z",
            "author": {"name": "Taka", "email": "taka@example.com"},
        },
    }


class TestNormalizeWorkflowRun:
    def test_normalizes_payload_to_github_activity_shape_and_redacts(self):
        client = GitHubWorkflowRunClient(
            "tok",
            "taka",
            redaction_patterns=[
                {"name": "ticket", "pattern": r"ticket-\d+", "placeholder": "[REDACTED_TICKET]"}
            ],
        )

        record = normalize_workflow_run_payload(
            _run_payload(),
            repo="acme/widget",
            redactor=client.redactor,
        )
        run = GitHubWorkflowRun(**record)
        activity = run.to_activity_dict()

        assert record["repo"] == "acme/widget"
        assert record["run_id"] == 1001
        assert record["workflow_name"] == "Tests"
        assert record["status"] == "completed"
        assert record["conclusion"] == "failure"
        assert record["display_title"] == "Fix [REDACTED_TICKET] regression"
        assert "ticket-1234" not in record["body"]
        assert record["updated_at"].isoformat() == "2026-04-01T12:00:00+00:00"
        assert run.activity_id == f"acme/widget#1001:{ACTIVITY_TYPE}"
        assert activity["repo_name"] == "acme/widget"
        assert activity["activity_type"] == ACTIVITY_TYPE
        assert activity["number"] == 1001
        assert activity["state"] == "failure"
        assert activity["metadata"]["status"] == "completed"
        assert activity["metadata"]["conclusion"] == "failure"
        assert activity["metadata"]["workflow_name"] == "Tests"
        assert activity["metadata"]["head_sha"] == "abc123"
        assert activity["metadata"]["branch"] == "main"

    def test_normalizes_success_cancelled_and_in_progress_states(self):
        success = GitHubWorkflowRun(**normalize_workflow_run_payload(_run_payload(conclusion="success"), "r"))
        cancelled = GitHubWorkflowRun(
            **normalize_workflow_run_payload(_run_payload(conclusion="cancelled"), "r")
        )
        running = GitHubWorkflowRun(
            **normalize_workflow_run_payload(
                _run_payload(status="in_progress", conclusion=None),
                "r",
            )
        )

        assert success.to_activity_dict()["state"] == "success"
        assert cancelled.to_activity_dict()["state"] == "cancelled"
        assert running.to_activity_dict()["state"] == "in_progress"


class TestGitHubWorkflowRunClient:
    def test_get_repo_workflow_runs_paginates_limits_and_filters_since(self):
        first_page = [_run_payload(run_id=2000 + index) for index in range(100)]
        second = _run_payload(3000)
        old = _run_payload(3001)
        old["updated_at"] = "2026-03-01T12:00:00Z"
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
                limit=102,
            )
        )

        assert len(runs) == 101
        assert runs[0].run_id == 2000
        assert runs[-1].run_id == 3000
        assert session.get.call_args_list[0].kwargs["params"]["created"] == f">={TIMESTAMP.isoformat()}"
        assert session.get.call_args_list[0].kwargs["params"]["per_page"] == 100
        assert session.get.call_args_list[0].kwargs["params"]["page"] == 1
        assert session.get.call_args_list[1].kwargs["params"]["per_page"] == 2
        assert session.get.call_args_list[1].kwargs["params"]["page"] == 2

    def test_get_repo_workflow_runs_handles_empty_response(self):
        session = MagicMock()
        session.get.return_value = _mock_response(json_data={"workflow_runs": []})
        client = GitHubWorkflowRunClient("tok", "taka", session=session)

        assert list(client.get_repo_workflow_runs("acme", "widget", since=TIMESTAMP)) == []
        session.get.assert_called_once()


class TestPollNewWorkflowRuns:
    @patch.object(GitHubWorkflowRunClient, "get_all_recent_workflow_runs")
    def test_persists_only_new_unique_runs(self, mock_runs):
        new_run = GitHubWorkflowRun(**normalize_workflow_run_payload(_run_payload(1001), "acme/widget"))
        existing = GitHubWorkflowRun(**normalize_workflow_run_payload(_run_payload(1002), "acme/widget"))
        duplicate = GitHubWorkflowRun(**new_run.__dict__)
        mock_runs.return_value = iter([new_run, duplicate, existing])
        db = MagicMock()
        db.is_github_activity_processed.side_effect = [False, True]

        result = poll_new_workflow_runs("tok", "taka", TIMESTAMP, db, repositories=["acme/widget"])

        assert result == [new_run]
        assert db.is_github_activity_processed.call_count == 2
        db.upsert_github_activity.assert_called_once_with(**new_run.to_activity_dict())

    @patch.object(GitHubWorkflowRunClient, "get_all_recent_workflow_runs")
    def test_dry_run_does_not_persist(self, mock_runs):
        run = GitHubWorkflowRun(**normalize_workflow_run_payload(_run_payload(1001), "acme/widget"))
        mock_runs.return_value = iter([run])
        db = MagicMock()
        db.is_github_activity_processed.return_value = False

        assert poll_new_workflow_runs("tok", "taka", TIMESTAMP, db, dry_run=True) == [run]
        db.upsert_github_activity.assert_not_called()

    @patch.object(GitHubWorkflowRunClient, "get_all_recent_workflow_runs")
    def test_persists_to_existing_github_activity_table_and_deduplicates(self, mock_runs, db):
        first = GitHubWorkflowRun(**normalize_workflow_run_payload(_run_payload(1001), "acme/widget"))
        mock_runs.return_value = iter([first])

        assert poll_new_workflow_runs("tok", "taka", TIMESTAMP, db) == [first]

        duplicate = GitHubWorkflowRun(**normalize_workflow_run_payload(_run_payload(1001), "acme/widget"))
        mock_runs.return_value = iter([duplicate])
        assert poll_new_workflow_runs("tok", "taka", TIMESTAMP, db) == []

        rows = db.get_github_workflow_runs(limit=10)
        assert len(rows) == 1
        assert rows[0]["activity_type"] == ACTIVITY_TYPE
        assert rows[0]["number"] == 1001
        assert rows[0]["metadata"]["run_id"] == 1001
        assert rows[0]["metadata"]["conclusion"] == "failure"


def test_determine_since_uses_workflow_meta_cursor(db):
    db.set_meta(CURSOR_KEY, (TIMESTAMP - timedelta(hours=1)).isoformat())

    assert determine_since(db, 24) == TIMESTAMP - timedelta(hours=1)


@patch("poll_github_workflows.update_monitoring")
@patch("poll_github_workflows.ingest_github_workflow_runs")
@patch("poll_github_workflows.script_context")
def test_main_dry_run_prints_runs_without_cursor(mock_context, mock_ingest, mock_update, db, capsys):
    config = MagicMock()
    config.github.token = "tok"
    config.github.username = "taka"
    config.github.repositories = ["acme/widget"]
    config.privacy.redaction_patterns = []
    config.timeouts.github_seconds = 10
    mock_context.return_value.__enter__.return_value = (config, db)
    mock_context.return_value.__exit__.return_value = None
    run = GitHubWorkflowRun(**normalize_workflow_run_payload(_run_payload(1001), "acme/widget"))
    mock_ingest.return_value = [run]

    assert main(["--dry-run", "--since-hours", "6"]) == 0

    assert "Would ingest acme/widget#1001:workflow_run" in capsys.readouterr().out
    assert db.get_meta(CURSOR_KEY) is None
    mock_update.assert_not_called()
    assert mock_ingest.call_args.kwargs["dry_run"] is True


@patch("poll_github_workflows.update_monitoring")
@patch("poll_github_workflows.ingest_github_workflow_runs")
@patch("poll_github_workflows.script_context")
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

    assert main(["--since-hours", "6", "--limit", "25", "--repo", "acme/widget"]) == 0

    assert db.get_meta(CURSOR_KEY) is not None
    assert mock_ingest.call_args.kwargs["repositories"] == ["acme/widget"]
    assert mock_ingest.call_args.kwargs["limit"] == 25
    mock_update.assert_called_once_with("poll-github-workflows")
