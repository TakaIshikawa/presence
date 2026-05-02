"""Tests for GitHub activity follow-up idea seeding."""

from __future__ import annotations

import importlib.util
import json
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from evaluation.github_activity_followup_ideas import (
    SOURCE_NAME,
    build_github_activity_followup_idea_candidates,
    format_github_activity_followup_ideas_json,
    seed_github_activity_followup_ideas,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "seed_github_activity_followup_ideas.py"
spec = importlib.util.spec_from_file_location("seed_github_activity_followup_ideas_script", SCRIPT_PATH)
seed_github_activity_followup_ideas_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(seed_github_activity_followup_ideas_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _activity(
    db,
    *,
    repo_name: str = "alpha/app",
    activity_type: str = "pull_request",
    number: str = "42",
    title: str = "Add export workflow",
    state: str = "closed",
    days_ago: int = 1,
    closed: bool = True,
    merged: bool = False,
) -> int:
    event_at = (NOW - timedelta(days=days_ago)).isoformat()
    return db.upsert_github_activity(
        repo_name=repo_name,
        activity_type=activity_type,
        number=number,
        title=title,
        state=state,
        author="alice",
        url=f"https://github.com/{repo_name}/{activity_type}/{number}",
        updated_at=event_at,
        created_at=(NOW - timedelta(days=days_ago, hours=2)).isoformat(),
        closed_at=event_at if closed else None,
        merged_at=event_at if merged else None,
        body=f"Body for {title}",
        labels=["release-note"],
        metadata={"activity_id": f"{repo_name}#{number}:{activity_type}"},
    )


def test_dry_run_returns_deterministic_candidates_without_writing(db):
    _activity(db, number="10", title="Close stale onboarding gap", activity_type="issue")
    _activity(db, number="11", title="Add export workflow", merged=True)

    report = seed_github_activity_followup_ideas(db, dry_run=True, now=NOW)

    assert [(item.activity_id, item.status) for item in report.candidates] == [
        ("alpha/app#11:pull_request", "candidate"),
        ("alpha/app#10:issue", "candidate"),
    ]
    assert report.candidates[0].priority == "high"
    assert report.candidates[0].suggested_topic == "Merged work in alpha/app"
    assert db.get_content_ideas(status="open") == []


def test_duplicate_suppression_from_generated_content_and_prior_ideas(db):
    generated_id = _activity(db, repo_name="alpha/app", number="20", merged=True)
    idea_id = _activity(db, repo_name="alpha/app", number="21", activity_type="issue")
    _activity(db, repo_name="alpha/app", number="22", activity_type="release", title="v1.2.0")

    db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        source_activity_ids=[str(generated_id)],
        content="Already covered",
        eval_score=8,
        eval_feedback="ok",
    )
    db.add_content_idea(
        note="Existing idea",
        topic="Existing",
        priority="normal",
        source="manual",
        source_metadata={"activity_id": f"alpha/app#21:issue"},
    )

    report = build_github_activity_followup_idea_candidates(db, now=NOW)
    reasons = {item.activity_id: item.duplicate_reason for item in report.candidates}

    assert reasons["alpha/app#20:pull_request"] == "already referenced by generated_content"
    assert reasons["alpha/app#21:issue"] == "already referenced by content_ideas"
    assert reasons["alpha/app#22:release"] is None


def test_release_handling_and_filters(db):
    _activity(db, repo_name="alpha/app", number="v1.2.0", activity_type="release", title="v1.2.0", closed=False)
    _activity(db, repo_name="beta/api", number="99", activity_type="issue")

    report = build_github_activity_followup_idea_candidates(
        db,
        repo="alpha/app",
        activity_types=("release",),
        limit=1,
        now=NOW,
    )

    assert len(report.candidates) == 1
    candidate = report.candidates[0]
    assert candidate.activity_type == "release"
    assert candidate.activity_id == "alpha/app#v1.2.0:release"
    assert candidate.suggested_topic == "Release follow-up for alpha/app"


def test_insert_mode_creates_content_ideas_with_required_metadata(db):
    _activity(db, repo_name="alpha/app", number="30", merged=True)

    report = seed_github_activity_followup_ideas(db, dry_run=False, now=NOW)

    assert [(item.status, item.idea_id) for item in report.candidates] == [("created", 1)]
    ideas = db.get_content_ideas(status="open")
    assert len(ideas) == 1
    assert ideas[0]["source"] == SOURCE_NAME
    assert ideas[0]["priority"] == "high"
    metadata = json.loads(ideas[0]["source_metadata"])
    assert metadata["source"] == SOURCE_NAME
    assert metadata["repo"] == "alpha/app"
    assert metadata["repo_name"] == "alpha/app"
    assert metadata["activity_id"] == "alpha/app#30:pull_request"
    assert metadata["activity_type"] == "pull_request"
    assert metadata["url"].endswith("/pull_request/30")

    second = seed_github_activity_followup_ideas(db, dry_run=False, now=NOW)
    assert second.candidates[0].duplicate_reason == "already referenced by content_ideas"
    assert len(db.get_content_ideas(status=None)) == 1


def test_json_formatter_and_cli_support_dry_run_and_insert(db, capsys):
    _activity(db, repo_name="alpha/app", number="40", activity_type="release", title="v2.0.0", closed=False)

    report = seed_github_activity_followup_ideas(db, dry_run=True, now=NOW)
    payload = json.loads(format_github_activity_followup_ideas_json(report))
    assert payload["summary"]["candidate"] == 1
    assert payload["candidates"][0]["source_metadata"]["activity_type"] == "release"

    with patch.object(
        seed_github_activity_followup_ideas_script,
        "script_context",
        side_effect=lambda: _script_context(db),
    ), patch.object(
        seed_github_activity_followup_ideas_script,
        "seed_github_activity_followup_ideas",
        wraps=lambda db, **kwargs: seed_github_activity_followup_ideas(db, now=NOW, **kwargs),
    ):
        dry_exit = seed_github_activity_followup_ideas_script.main(
            [
                "--days",
                "7",
                "--repo",
                "alpha/app",
                "--activity-types",
                "release",
                "--limit",
                "1",
                "--format",
                "json",
            ]
        )
        dry_payload = json.loads(capsys.readouterr().out)
        insert_exit = seed_github_activity_followup_ideas_script.main(
            [
                "--days",
                "7",
                "--repo",
                "alpha/app",
                "--activity-types",
                "release",
                "--limit",
                "1",
                "--insert",
                "--format",
                "json",
            ]
        )
        insert_payload = json.loads(capsys.readouterr().out)

    assert dry_exit == 0
    assert insert_exit == 0
    assert dry_payload["filters"]["dry_run"] is True
    assert insert_payload["summary"]["created"] == 1
    assert len(db.get_content_ideas(status="open")) == 1
