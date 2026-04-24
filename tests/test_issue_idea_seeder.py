"""Tests for stale GitHub issue idea seeding."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from synthesis.issue_idea_seeder import seed_issue_ideas


NOW = datetime(2026, 4, 23, tzinfo=timezone.utc)


def _add_issue(
    db,
    *,
    repo: str = "taka/presence",
    number: int = 7,
    title: str = "Users need clearer retry diagnostics",
    body: str = "Several users are confused when publish retries fail without a compact diagnosis.",
    state: str = "open",
    updated_at: str = "2026-03-01T12:00:00+00:00",
    labels: list[str] | None = None,
) -> int:
    if labels is None:
        labels = ["bug", "user-feedback"]
    return db.upsert_github_activity(
        repo_name=repo,
        activity_type="issue",
        number=number,
        title=title,
        state=state,
        author="taka",
        url=f"https://github.com/{repo}/issues/{number}",
        updated_at=updated_at,
        created_at="2026-02-20T10:00:00+00:00",
        body=body,
        labels=labels,
    )


def test_seed_issue_ideas_creates_content_idea_for_stale_open_issue(db):
    _add_issue(db)

    results = seed_issue_ideas(db, days_stale=30, now=NOW)

    assert [(result.status, result.repo_name, result.number) for result in results] == [
        ("created", "taka/presence", 7)
    ]
    ideas = db.get_content_ideas(status="open")
    assert len(ideas) == 1
    idea = ideas[0]
    assert idea["source"] == "github_issue_stale_seed"
    assert idea["topic"] == "taka/presence: Users need clearer retry diagnostics"
    assert "Stale open issue #7" in idea["note"]
    metadata = json.loads(idea["source_metadata"])
    assert metadata["source"] == "github_issue_stale_seed"
    assert metadata["repo_name"] == "taka/presence"
    assert metadata["number"] == 7
    assert metadata["activity_id"] == "taka/presence#7:issue"
    assert metadata["labels"] == ["bug", "user-feedback"]
    assert metadata["url"] == "https://github.com/taka/presence/issues/7"
    assert metadata["stale_days"] == 52


def test_seed_issue_ideas_filters_open_stale_issues_by_type_state_age_and_label(db):
    _add_issue(db, number=1)
    _add_issue(db, number=2, state="closed")
    _add_issue(db, number=3, updated_at="2026-04-20T12:00:00+00:00")
    _add_issue(db, number=4, labels=["maintenance"])
    db.upsert_github_activity(
        repo_name="taka/presence",
        activity_type="pull_request",
        number=5,
        title="PR should not seed",
        state="open",
        author="taka",
        url="https://github.com/taka/presence/pull/5",
        updated_at="2026-03-01T12:00:00+00:00",
        labels=["bug"],
    )

    results = seed_issue_ideas(db, days_stale=30, now=NOW)

    assert [result.number for result in results] == [1, 4]
    assert [result.status for result in results] == ["created", "skipped"]
    assert results[1].reason == "label filter"
    assert len(db.get_content_ideas(status="open")) == 1


def test_seed_issue_ideas_reuses_active_duplicates_by_source_metadata(db):
    _add_issue(db, number=11)
    first = seed_issue_ideas(db, days_stale=30, now=NOW)
    second = seed_issue_ideas(db, days_stale=30, now=NOW)

    assert first[0].status == "created"
    assert second[0].status == "duplicate"
    assert second[0].idea_id == first[0].idea_id
    assert second[0].reason == "open duplicate"
    assert len(db.get_content_ideas(status=None)) == 1

    db.promote_content_idea(first[0].idea_id, target_date="2026-05-01")
    third = seed_issue_ideas(db, days_stale=30, now=NOW)

    assert third[0].status == "duplicate"
    assert third[0].reason == "promoted duplicate"
    assert len(db.get_content_ideas(status=None)) == 1


def test_seed_issue_ideas_dry_run_returns_payloads_without_writes(db):
    _add_issue(db)

    results = seed_issue_ideas(db, days_stale=30, dry_run=True, now=NOW)

    assert len(results) == 1
    assert results[0].status == "proposed"
    assert results[0].idea_id is None
    assert results[0].source_metadata["activity_id"] == "taka/presence#7:issue"
    assert results[0].source_metadata["stale_days"] == 52
    assert "Stale open issue #7" in results[0].note
    assert db.get_content_ideas(status="open") == []


def test_seed_issue_ideas_supports_repo_label_priority_and_limit(db):
    _add_issue(db, repo="taka/presence", number=1, labels=["docs"])
    _add_issue(db, repo="taka/other", number=2, labels=["question"])
    _add_issue(db, repo="taka/other", number=3, labels=["bug"])

    results = seed_issue_ideas(
        db,
        days_stale=30,
        repo="taka/other",
        labels=["question"],
        priority="high",
        limit=2,
        now=NOW,
    )

    assert [result.number for result in results] == [2, 3]
    assert [result.status for result in results] == ["created", "skipped"]
    ideas = db.get_content_ideas(status="open", priority="high")
    assert len(ideas) == 1
    assert ideas[0]["topic"] == "taka/other: Users need clearer retry diagnostics"
