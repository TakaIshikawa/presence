"""Tests for seeding ideas from repeated GitHub Actions workflow run failures."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from synthesis.workflow_run_idea_seeder import (
    SOURCE_NAME,
    build_workflow_run_idea_candidates,
    seed_workflow_run_ideas,
)

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from seed_workflow_run_ideas import format_results_json, format_results_text, main


NOW = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)


def _add_workflow_run(
    db,
    *,
    repo: str = "taka/presence",
    run_id: int = 1001,
    run_number: int | None = None,
    workflow_name: str = "Tests",
    conclusion: str = "failure",
    updated_at: str = "2026-04-23T10:00:00+00:00",
    commit_sha: str | None = "abc123",
    branch: str = "main",
):
    run_number = run_number or run_id
    metadata = {
        "workflow_name": workflow_name,
        "run_number": run_number,
        "conclusion": conclusion,
        "commit_sha": commit_sha,
        "head_sha": commit_sha,
        "branch": branch,
        "head_branch": branch,
        "run_url": f"https://github.com/{repo}/actions/runs/{run_id}",
        "html_url": f"https://github.com/{repo}/actions/runs/{run_id}",
    }
    return db.upsert_github_activity(
        repo_name=repo,
        activity_type="workflow_run",
        number=run_id,
        title=f"{workflow_name} #{run_number}: {conclusion}",
        state=conclusion,
        author="github-actions",
        url=f"https://github.com/{repo}/actions/runs/{run_id}",
        updated_at=updated_at,
        created_at="2026-04-23T09:00:00+00:00",
        metadata=metadata,
    )


def test_groups_repeated_failed_and_cancelled_runs_by_workflow_and_branch(db):
    _add_workflow_run(db, run_id=1001, workflow_name="Tests", conclusion="failure")
    _add_workflow_run(
        db,
        run_id=1002,
        workflow_name="Tests",
        conclusion="cancelled",
        updated_at="2026-04-23T11:00:00+00:00",
        commit_sha="def456",
    )
    _add_workflow_run(db, run_id=1003, workflow_name="Tests", conclusion="success")
    _add_workflow_run(db, run_id=1004, workflow_name="Tests", branch="feature")
    _add_workflow_run(db, run_id=1005, workflow_name="Lint")

    candidates = build_workflow_run_idea_candidates(db, days=7, min_failures=2, now=NOW)

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.repo_name == "taka/presence"
    assert candidate.workflow_name == "Tests"
    assert candidate.branch == "main"
    assert candidate.failure_count == 2
    assert candidate.latest_run_url == "https://github.com/taka/presence/actions/runs/1002"
    assert candidate.latest_commit_sha == "def456"
    assert candidate.conclusions == ("cancelled", "failure")
    assert candidate.run_numbers == (1001, 1002)
    assert "2 failed/cancelled GitHub Actions runs" in candidate.note
    assert candidate.source_metadata["workflow_run_fingerprint"] == candidate.fingerprint
    assert candidate.source_metadata["latest_run_url"].endswith("/1002")
    assert candidate.source_metadata["commit_sha"] == "def456"


def test_seed_creates_one_content_idea_with_fingerprint_metadata(db):
    _add_workflow_run(db, run_id=1001)
    _add_workflow_run(db, run_id=1002, updated_at="2026-04-23T11:00:00+00:00")

    results = seed_workflow_run_ideas(db, days=7, min_failures=2, now=NOW)

    assert [(result.status, result.reason, result.failure_count) for result in results] == [
        ("created", "created", 2)
    ]
    ideas = db.get_content_ideas(status="open")
    assert len(ideas) == 1
    idea = ideas[0]
    assert idea["source"] == SOURCE_NAME
    assert idea["topic"] == "taka/presence: repeated Tests workflow failures on main"
    assert "Latest run: https://github.com/taka/presence/actions/runs/1002" in idea["note"]
    metadata = json.loads(idea["source_metadata"])
    assert metadata["source"] == SOURCE_NAME
    assert metadata["workflow_name"] == "Tests"
    assert metadata["branch"] == "main"
    assert metadata["failure_count"] == 2
    assert metadata["activity_ids"] == [
        "taka/presence#1001:workflow_run",
        "taka/presence#1002:workflow_run",
    ]
    assert metadata["latest_activity_id"] == "taka/presence#1002:workflow_run"
    assert metadata["latest_run_url"] == "https://github.com/taka/presence/actions/runs/1002"
    assert metadata["commit_sha"] == "abc123"
    assert metadata["workflow_run_fingerprint"]


def test_existing_open_idea_with_same_fingerprint_is_not_duplicated(db):
    _add_workflow_run(db, run_id=1001)
    _add_workflow_run(db, run_id=1002)

    first = seed_workflow_run_ideas(db, days=7, min_failures=2, now=NOW)
    second = seed_workflow_run_ideas(db, days=7, min_failures=2, now=NOW)

    assert first[0].status == "created"
    assert second[0].status == "skipped"
    assert second[0].reason == "open duplicate"
    assert second[0].idea_id == first[0].idea_id
    assert len(db.get_content_ideas(status="open")) == 1


def test_dry_run_reports_would_be_ideas_without_database_writes(db):
    _add_workflow_run(db, run_id=1001)
    _add_workflow_run(db, run_id=1002)

    results = seed_workflow_run_ideas(db, days=7, min_failures=2, dry_run=True, now=NOW)

    assert results[0].status == "proposed"
    assert results[0].reason == "dry run"
    assert results[0].source_metadata["latest_run_url"].endswith("/1002")
    assert db.get_content_ideas(status="open") == []
    assert '"status": "proposed"' in format_results_json(results)
    assert "proposed" in format_results_text(results)


def test_cli_supports_format_json_and_dry_run(db, capsys):
    _add_workflow_run(db, run_id=1001)
    _add_workflow_run(db, run_id=1002)

    def fake_script_context():
        class Context:
            def __enter__(self):
                return (object(), db)

            def __exit__(self, exc_type, exc, tb):
                return None

        return Context()

    with patch("seed_workflow_run_ideas.script_context", fake_script_context):
        exit_code = main(["--days", "30", "--min-failures", "2", "--dry-run", "--format", "json"])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["status"] == "proposed"
    assert payload[0]["source_metadata"]["workflow_run_fingerprint"]
    assert db.get_content_ideas(status="open") == []
