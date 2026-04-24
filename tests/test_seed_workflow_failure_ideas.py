"""Tests for the workflow failure seeding CLI."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from seed_workflow_failure_ideas import format_results_table, main, seed_workflow_failure_ideas


NOW = datetime(2026, 4, 23, tzinfo=timezone.utc)


def _add_workflow_run(
    db,
    *,
    repo: str = "taka/presence",
    run_id: int = 1001,
    workflow_name: str = "Tests",
    conclusion: str = "failure",
    updated_at: str = "2026-04-22T12:00:00+00:00",
) -> int:
    return db.upsert_github_activity(
        repo_name=repo,
        activity_type="workflow_run",
        number=run_id,
        title=workflow_name,
        state=conclusion,
        author="github-actions",
        url=f"https://github.com/{repo}/actions/runs/{run_id}",
        updated_at=updated_at,
        created_at="2026-04-22T10:00:00+00:00",
        metadata={
            "workflow_name": workflow_name,
            "run_number": run_id,
            "conclusion": conclusion,
            "head_sha": "abc123",
            "branch": "main",
            "url": f"https://github.com/{repo}/actions/runs/{run_id}",
            "source_activity_id": "taka/presence#42:pull_request",
        },
    )


def test_cli_json_dry_run_reports_proposals_and_skips_without_writes(db, capsys):
    _add_workflow_run(db, run_id=1)
    _add_workflow_run(db, run_id=2, conclusion="success")
    _add_workflow_run(db, run_id=3, updated_at="2026-03-01T12:00:00+00:00")

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("seed_workflow_failure_ideas.script_context", fake_script_context), patch(
        "synthesis.workflow_failure_seeder.datetime"
    ) as mock_datetime:
        mock_datetime.now.return_value = NOW
        mock_datetime.fromisoformat.side_effect = datetime.fromisoformat
        mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
        main(["--days", "7", "--min-score", "60", "--dry-run", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert {item["run_number"]: item["reason"] for item in payload} == {
        1: "dry run",
        2: "conclusion success",
        3: "stale",
    }
    proposed = [item for item in payload if item["run_number"] == 1][0]
    assert proposed["status"] == "proposed"
    assert db.get_content_ideas(status="open") == []


def test_format_results_table_includes_summary(db):
    _add_workflow_run(db)
    results = seed_workflow_failure_ideas(db, days=7, dry_run=True, now=NOW)

    output = format_results_table(results)

    assert "created=0 proposed=1 skipped=0" in output
    assert "taka/presence" in output
    assert "Tests" in output
    assert "dry run" in output


def test_cli_human_output_creates_idea(db, capsys):
    _add_workflow_run(db)

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("seed_workflow_failure_ideas.script_context", fake_script_context), patch(
        "synthesis.workflow_failure_seeder.datetime"
    ) as mock_datetime:
        mock_datetime.now.return_value = NOW
        mock_datetime.fromisoformat.side_effect = datetime.fromisoformat
        mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
        main(["--days", "7", "--min-score", "60", "--limit", "5"])

    output = capsys.readouterr().out
    assert "created=1 proposed=0 skipped=0" in output
    assert len(db.get_content_ideas(status="open")) == 1
