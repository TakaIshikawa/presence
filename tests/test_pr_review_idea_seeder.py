"""Tests for seeding content ideas from GitHub PR review activity."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from synthesis.pr_review_idea_seeder import (
    format_pr_review_idea_results_json,
    format_pr_review_idea_results_table,
    seed_pr_review_ideas,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "seed_pr_review_ideas.py"
spec = importlib.util.spec_from_file_location("seed_pr_review_ideas_cli", SCRIPT_PATH)
seed_pr_review_ideas_cli = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(seed_pr_review_ideas_cli)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _review_activity(
    db,
    *,
    repo: str = "taka/presence",
    number: int = 601,
    pr_number: int = 8,
    activity_type: str = "github_pr_review_comment",
    title: str | None = None,
    body: str = (
        "Prefer keeping this retry branch explicit because future workflow failures "
        "need a deterministic diagnosis instead of silently falling through."
    ),
    author: str = "reviewer",
    state: str = "commented",
    updated_at: str = "2026-04-30T12:00:00+00:00",
    metadata: dict | None = None,
) -> int:
    url = f"https://github.com/{repo}/pull/{pr_number}#discussion_r{number}"
    payload = {
        "activity_id": f"{repo}#{number}:{activity_type}",
        "comment_id": number,
        "parent_pr_number": pr_number,
        "parent_number": pr_number,
        "parent_type": "pull_request",
        "path": "src/retry.py",
        "diff_hunk": "@@ -10,7 +10,9 @@",
    }
    if metadata:
        payload.update(metadata)
    return db.upsert_github_activity(
        repo_name=repo,
        activity_type=activity_type,
        number=number,
        title=title or f"PR review comment on #{pr_number} src/retry.py",
        state=state,
        author=author,
        url=url,
        updated_at=updated_at,
        created_at="2026-04-30T10:00:00+00:00",
        body=body,
        labels=[],
        metadata=payload,
    )


def test_seed_pr_review_ideas_creates_content_idea_with_stable_metadata(db):
    _review_activity(db)

    results = seed_pr_review_ideas(db, days=14, now=NOW)

    assert [(result.status, result.repo_name, result.pr_number) for result in results] == [
        ("created", "taka/presence", 8)
    ]
    ideas = db.get_content_ideas(status="open")
    assert len(ideas) == 1
    idea = ideas[0]
    assert idea["source"] == "github_pr_review"
    assert idea["priority"] == "normal"
    assert "review checklist" in idea["note"]
    metadata = json.loads(idea["source_metadata"])
    assert metadata["source"] == "github_pr_review"
    assert metadata["activity_id"] == "taka/presence#601:github_pr_review_comment"
    assert metadata["repo_name"] == "taka/presence"
    assert metadata["pr_number"] == 8
    assert metadata["review_comment_url"] == "https://github.com/taka/presence/pull/8#discussion_r601"


def test_seed_pr_review_ideas_dry_run_returns_candidates_without_writes(db):
    _review_activity(db)

    results = seed_pr_review_ideas(db, days=14, dry_run=True, now=NOW)

    assert [(result.status, result.reason) for result in results] == [
        ("proposed", "dry run")
    ]
    assert results[0].source_metadata["activity_id"] == "taka/presence#601:github_pr_review_comment"
    assert db.get_content_ideas(status="open") == []


def test_seed_pr_review_ideas_skips_duplicates_on_rerun(db):
    _review_activity(db)

    first = seed_pr_review_ideas(db, days=14, now=NOW)
    second = seed_pr_review_ideas(db, days=14, now=NOW)

    assert first[0].status == "created"
    assert second[0].status == "skipped"
    assert second[0].reason == "open duplicate"
    assert second[0].idea_id == first[0].idea_id
    assert len(db.get_content_ideas(status=None)) == 1


def test_seed_pr_review_ideas_filters_trivial_approval_empty_and_bot_activity(db):
    _review_activity(db, number=1, body="LGTM", state="approved")
    _review_activity(db, number=2, body="")
    _review_activity(
        db,
        number=3,
        author="dependabot[bot]",
        body="Prefer the typed helper because it avoids a migration edge case.",
    )
    _review_activity(
        db,
        number=4,
        body="Looks good to me",
        state="approved",
        metadata={"state": "APPROVED"},
    )
    _review_activity(
        db,
        number=5,
        body="Prefer the parser because string splitting misses escaped delimiters.",
    )

    results = seed_pr_review_ideas(db, days=14, dry_run=True, now=NOW)

    assert [(result.number, result.status, result.reason) for result in results] == [
        (5, "proposed", "dry run"),
        (4, "skipped", "approval only"),
        (3, "skipped", "bot author"),
        (2, "skipped", "empty body"),
        (1, "skipped", "approval only"),
    ]


def test_seed_pr_review_ideas_supports_repo_limit_priority_and_activity_types(db):
    _review_activity(db, repo="taka/presence", number=1, activity_type="pr_review_comment")
    _review_activity(db, repo="taka/other", number=2, activity_type="pull_request_review_comment")
    _review_activity(db, repo="taka/other", number=3, activity_type="pull_request_review")
    _review_activity(db, repo="taka/other", number=4, updated_at="2026-03-01T12:00:00+00:00")

    results = seed_pr_review_ideas(
        db,
        days=14,
        repo="taka/other",
        limit=1,
        priority="high",
        dry_run=True,
        now=NOW,
    )

    assert len(results) == 1
    assert results[0].repo_name == "taka/other"
    assert results[0].priority == "high"
    assert results[0].status == "proposed"


def test_formatters_and_cli_support_json_and_table_output(db, capsys):
    _review_activity(db)
    results = seed_pr_review_ideas(db, days=14, dry_run=True, now=NOW)

    payload = json.loads(format_pr_review_idea_results_json(results))
    assert payload[0]["status"] == "proposed"
    table = format_pr_review_idea_results_table(results)
    assert "created=0 proposed=1 skipped=0" in table
    assert "taka/presence" in table

    with patch.object(
        seed_pr_review_ideas_cli,
        "script_context",
        wraps=lambda: _script_context(db),
    ), patch.object(
        seed_pr_review_ideas_cli,
        "seed_pr_review_ideas",
        wraps=lambda db, **kwargs: seed_pr_review_ideas(db, now=NOW, **kwargs),
    ):
        assert (
            seed_pr_review_ideas_cli.main(
                [
                    "--days",
                    "14",
                    "--repo",
                    "taka/presence",
                    "--limit",
                    "1",
                    "--priority",
                    "low",
                    "--dry-run",
                    "--json",
                ]
            )
            == 0
        )

    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload[0]["status"] == "proposed"
    assert cli_payload[0]["priority"] == "low"


def test_cli_reports_invalid_repo(capsys):
    assert seed_pr_review_ideas_cli.main(["--repo", " "]) == 1
    assert "repo must not be blank" in capsys.readouterr().err
