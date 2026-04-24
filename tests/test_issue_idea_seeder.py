"""Tests for stale-open and closed-issue content idea seeding."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from seed_issue_ideas import format_results_json, format_results_table, main
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
    closed_at: str | None = None,
    labels: list[str] | None = None,
    activity_type: str = "issue",
    metadata: dict | None = None,
) -> int:
    if labels is None:
        labels = ["bug", "user-feedback"]
    return db.upsert_github_activity(
        repo_name=repo,
        activity_type=activity_type,
        number=number,
        title=title,
        state=state,
        author="taka",
        url=f"https://github.com/{repo}/issues/{number}",
        updated_at=updated_at,
        created_at="2026-02-20T10:00:00+00:00",
        body=body,
        closed_at=closed_at,
        labels=labels,
        metadata=metadata or {},
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
    metadata = json.loads(idea["source_metadata"])
    assert metadata["activity_id"] == "taka/presence#7:issue"
    assert metadata["stale_days"] == 52


def test_seed_issue_ideas_filters_open_stale_issues_by_type_state_age_and_label(db):
    _add_issue(db, number=1)
    _add_issue(db, number=2, state="closed")
    _add_issue(db, number=3, updated_at="2026-04-20T12:00:00+00:00")
    _add_issue(db, number=4, labels=["maintenance"])
    _add_issue(db, number=5, activity_type="pull_request")

    results = seed_issue_ideas(db, days_stale=30, now=NOW)

    assert [result.number for result in results] == [1, 4]
    assert [result.status for result in results] == ["created", "skipped"]
    assert results[1].reason == "label filter"


def test_seed_issue_ideas_reuses_active_duplicates_by_source_metadata(db):
    _add_issue(db, number=11)
    first = seed_issue_ideas(db, days_stale=30, now=NOW)
    second = seed_issue_ideas(db, days_stale=30, now=NOW)

    assert first[0].status == "created"
    assert second[0].status == "duplicate"
    assert second[0].idea_id == first[0].idea_id


def test_seed_issue_ideas_dry_run_returns_payloads_without_writes(db):
    _add_issue(db)

    results = seed_issue_ideas(db, days_stale=30, dry_run=True, now=NOW)

    assert len(results) == 1
    assert results[0].status == "proposed"
    assert results[0].idea_id is None
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


def test_seed_issue_ideas_creates_closed_issue_idea_with_metadata(db):
    _add_issue(
        db,
        number=42,
        title="Fix confusing publish queue retry state",
        state="closed",
        updated_at="2026-04-22T12:00:00+00:00",
        closed_at="2026-04-22T11:00:00+00:00",
        labels=["bug", "customer"],
    )

    results = seed_issue_ideas(db, mode="closed", days=7, now=NOW)

    assert [(result.status, result.number, result.priority) for result in results] == [
        ("created", 42, "high")
    ]
    ideas = db.get_content_ideas(status="open")
    assert ideas[0]["source"] == "github_issue_seed"


def test_seed_issue_ideas_only_uses_closed_issue_activity(db):
    _add_issue(db, number=1, state="closed", closed_at="2026-04-22T11:00:00+00:00")
    _add_issue(db, number=2, state="open")
    _add_issue(db, number=3, activity_type="pull_request", state="closed")
    _add_issue(
        db,
        number=4,
        state="closed",
        metadata={"pull_request": {"url": "https://api.github.com/repos/taka/presence/pulls/4"}},
    )

    results = seed_issue_ideas(db, mode="closed", days=7, now=NOW)

    assert [result.number for result in results] == [1]


def test_seed_issue_ideas_closed_mode_skips_existing_active_duplicate(db):
    _add_issue(db, state="closed", closed_at="2026-04-22T11:00:00+00:00")
    first = seed_issue_ideas(db, mode="closed", days=7, now=NOW)
    second = seed_issue_ideas(db, mode="closed", days=7, now=NOW)

    assert first[0].status == "created"
    assert second[0].status == "skipped"
    assert second[0].idea_id == first[0].idea_id


def test_seed_issue_ideas_closed_mode_label_priority(db):
    _add_issue(db, number=1, title="Patch token leak in audit logs", state="closed", closed_at="2026-04-22T11:00:00+00:00", labels=["security"])
    _add_issue(db, number=2, title="Remove stale scheduler cleanup note", state="closed", closed_at="2026-04-22T11:00:00+00:00", labels=["maintenance"])
    _add_issue(db, number=3, title="Clarify install workflow docs", state="closed", closed_at="2026-04-22T11:00:00+00:00", labels=["docs"])

    results = seed_issue_ideas(db, mode="closed", days=7, now=NOW)

    priorities = {result.number: result.priority for result in results}
    assert priorities == {1: "high", 2: "low", 3: "normal"}


def test_seed_issue_ideas_closed_mode_dry_run_limit_repo_and_json_do_not_write(db):
    _add_issue(db, repo="taka/presence", number=1, state="closed", closed_at="2026-04-22T11:00:00+00:00")
    _add_issue(db, repo="taka/other", number=2, state="closed", closed_at="2026-04-22T11:00:00+00:00", labels=["maintenance"])

    results = seed_issue_ideas(
        db,
        mode="closed",
        days=7,
        repo="taka/other",
        dry_run=True,
        limit=1,
        now=NOW,
    )

    assert len(results) == 1
    assert results[0].status == "proposed"
    payload = json.loads(format_results_json(results))
    assert payload[0]["priority"] == "low"
    assert db.get_content_ideas(status="open") == []


def test_format_results_table_prints_summary(db):
    _add_issue(db, state="closed", closed_at="2026-04-22T11:00:00+00:00", labels=["security"])
    results = seed_issue_ideas(db, mode="closed", days=7, dry_run=True, now=NOW)

    output = format_results_table(results)

    assert "created=0 proposed=1 skipped=0 duplicate=0" in output
    assert "high" in output
    assert "taka/presence" in output


def test_main_prints_issue_seed_json(db, capsys):
    _add_issue(db, state="closed", closed_at="2026-04-22T11:00:00+00:00")

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("seed_issue_ideas.script_context", fake_script_context):
        assert main(["--mode", "closed", "--days", "7", "--dry-run", "--json"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["status"] == "proposed"
