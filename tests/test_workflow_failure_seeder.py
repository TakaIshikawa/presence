"""Tests for seeding content ideas from failed GitHub workflow runs."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from synthesis.workflow_failure_seeder import seed_workflow_failure_ideas, workflow_run_to_candidate


NOW = datetime(2026, 4, 23, tzinfo=timezone.utc)


def _add_workflow_run(
    db,
    *,
    repo: str = "taka/presence",
    run_id: int = 1001,
    workflow_name: str = "Tests",
    conclusion: str = "failure",
    updated_at: str = "2026-04-22T12:00:00+00:00",
    head_sha: str | None = "abc123",
    branch: str = "main",
    source_activity_id: str | None = "taka/presence#42:pull_request",
) -> int:
    metadata = {
        "workflow_name": workflow_name,
        "run_number": run_id,
        "conclusion": conclusion,
        "head_sha": head_sha,
        "head_branch": branch,
        "html_url": f"https://github.com/{repo}/actions/runs/{run_id}",
    }
    if source_activity_id:
        metadata["source_activity_id"] = source_activity_id
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
        metadata=metadata,
    )


def test_workflow_failure_creates_content_idea_with_source_metadata(db):
    _add_workflow_run(db)

    results = seed_workflow_failure_ideas(db, days=7, min_score=60, now=NOW)

    assert [(result.status, result.workflow_name, result.reason) for result in results] == [
        ("created", "Tests", "created")
    ]
    ideas = db.get_content_ideas(status="open")
    assert len(ideas) == 1
    idea = ideas[0]
    assert idea["topic"] == "taka/presence: Tests workflow failure"
    assert idea["priority"] == "high"
    assert idea["source"] == "github_workflow_failure_seed"
    assert "Failed workflow run Tests #1001" in idea["note"]
    metadata = json.loads(idea["source_metadata"])
    assert metadata["github_activity_id"] == "taka/presence#1001:workflow_run"
    assert metadata["workflow_name"] == "Tests"
    assert metadata["repo_name"] == "taka/presence"
    assert metadata["head_sha"] == "abc123"
    assert metadata["branch"] == "main"
    assert metadata["url"] == "https://github.com/taka/presence/actions/runs/1001"
    assert metadata["conclusion"] == "failure"
    assert metadata["source_activity_id"] == "taka/presence#42:pull_request"
    assert "conclusion=failure+45" in metadata["score_reasons"]


def test_workflow_failure_scoring_rewards_repeated_failures_and_context(db):
    _add_workflow_run(db, run_id=1, workflow_name="Tests", head_sha=None, source_activity_id=None)
    _add_workflow_run(db, run_id=2, workflow_name="Tests", head_sha=None, source_activity_id=None)
    _add_workflow_run(db, run_id=3, workflow_name="Tests")

    rows = db.get_github_workflow_runs(limit=3)
    candidate = workflow_run_to_candidate(rows[0], now=NOW, repeated_count=3)

    assert candidate.score == 100
    assert "repeated-failures>=3+20" in candidate.score_reasons
    assert "head-sha+8" in candidate.score_reasons
    assert "linked-source-activity+7" in candidate.score_reasons


def test_workflow_failure_skips_non_failed_stale_and_below_threshold_runs(db):
    _add_workflow_run(db, run_id=1, conclusion="success")
    _add_workflow_run(db, run_id=2, conclusion="cancelled")
    _add_workflow_run(db, run_id=3, updated_at="2026-03-01T12:00:00+00:00")
    _add_workflow_run(db, run_id=4, head_sha=None, source_activity_id=None)

    results = seed_workflow_failure_ideas(db, days=7, min_score=80, dry_run=True, now=NOW)

    reasons = {result.run_number: result.reason for result in results}
    assert reasons[1] == "conclusion success"
    assert reasons[2] == "conclusion cancelled"
    assert reasons[3] == "stale"
    assert reasons[4] == "score below 80"
    assert db.get_content_ideas(status="open") == []


def test_workflow_failure_deduplicates_by_github_activity_id(db):
    _add_workflow_run(db)

    first = seed_workflow_failure_ideas(db, days=7, now=NOW)
    second = seed_workflow_failure_ideas(db, days=7, now=NOW)

    assert first[0].status == "created"
    assert second[0].status == "skipped"
    assert second[0].reason == "open duplicate"
    assert second[0].idea_id == first[0].idea_id
    assert len(db.get_content_ideas(status="open")) == 1
