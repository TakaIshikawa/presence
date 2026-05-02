"""Tests for GitHub PR review idea seeding."""

from __future__ import annotations

import importlib.util
import json
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from synthesis.github_pr_review_idea_seeder import (
    SOURCE_NAME,
    format_github_pr_review_seed_json,
    format_github_pr_review_seed_text,
    seed_github_pr_review_ideas,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "seed_github_pr_review_ideas.py"
spec = importlib.util.spec_from_file_location("seed_github_pr_review_ideas_script", SCRIPT_PATH)
seed_github_pr_review_ideas_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(seed_github_pr_review_ideas_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _review_activity(
    db,
    *,
    repo_name: str = "alpha/app",
    pr_number: int = 42,
    review_id: str = "9001",
    activity_type: str = "pull_request_review_comment",
    author: str = "alice",
    state: str = "changes_requested",
    labels: list[str] | None = None,
    days_ago: float = 1,
    body: str = (
        "Prefer keeping this parser branch explicit because string splitting hides "
        "a security edge case. The tradeoff is a little more code, but the diff is "
        "easier to audit and the test coverage can pin the migration behavior."
    ),
    metadata: dict | None = None,
) -> int:
    updated_at = (NOW - timedelta(days=days_ago)).isoformat()
    payload = {
        "pr_number": pr_number,
        "comment_id": review_id,
        "review_id": f"review-{review_id}",
        "path": "src/parser.py",
        "line": 88,
        "diff_hunk": "@@ -80,7 +80,12 @@",
        "review_url": f"https://github.com/{repo_name}/pull/{pr_number}#pullrequestreview-1",
    }
    if metadata:
        payload.update(metadata)
    return db.upsert_github_activity(
        repo_name=repo_name,
        activity_type=activity_type,
        number=review_id,
        title=f"Review comment on PR #{pr_number}",
        state=state,
        author=author,
        url=f"https://github.com/{repo_name}/pull/{pr_number}#discussion_r{review_id}",
        updated_at=updated_at,
        created_at=(NOW - timedelta(days=days_ago, hours=1)).isoformat(),
        body=body,
        labels=labels or ["security", "review"],
        metadata=payload,
    )


def test_seed_creates_open_content_idea_with_deterministic_review_metadata(db):
    _review_activity(db)

    results = seed_github_pr_review_ideas(db, days=7, min_score=60, now=NOW)

    assert [(result.status, result.repo_name, result.pr_number) for result in results] == [
        ("created", "alpha/app", "42")
    ]
    ideas = db.get_content_ideas(status="open")
    assert len(ideas) == 1
    assert ideas[0]["source"] == SOURCE_NAME
    assert "tradeoff" in ideas[0]["note"]
    metadata = json.loads(ideas[0]["source_metadata"])
    assert metadata["source"] == SOURCE_NAME
    assert metadata["repo"] == "alpha/app"
    assert metadata["repo_name"] == "alpha/app"
    assert metadata["pr_number"] == "42"
    assert metadata["comment_url"].endswith("#discussion_r9001")
    assert metadata["review_url"].endswith("#pullrequestreview-1")
    assert metadata["author"] == "alice"
    assert metadata["state"] == "changes_requested"
    assert metadata["labels"] == ["review", "security"]
    assert metadata["score"] >= 85
    assert "requested changes" in metadata["score_reasons"]
    assert "security/performance label" in metadata["score_reasons"]
    assert metadata["path"] == "src/parser.py"
    assert metadata["line"] == 88


def test_dry_run_returns_proposed_results_without_writing(db):
    _review_activity(db)

    results = seed_github_pr_review_ideas(db, days=7, dry_run=True, now=NOW)

    assert [(result.status, result.reason) for result in results] == [("proposed", "dry run")]
    assert results[0].idea_id is None
    assert results[0].source_metadata["repo"] == "alpha/app"
    assert db.get_content_ideas(status="open") == []


def test_skips_stale_low_score_and_duplicate_review_ideas(db):
    _review_activity(db, review_id="fresh")
    _review_activity(db, review_id="stale", days_ago=30)
    _review_activity(
        db,
        review_id="low",
        state="commented",
        labels=[],
        metadata={"path": "", "line": "", "diff_hunk": ""},
        body="Could this be clearer?",
    )

    first = seed_github_pr_review_ideas(db, days=7, min_score=60, now=NOW)
    second = seed_github_pr_review_ideas(db, days=7, min_score=60, now=NOW)

    assert [(result.review_id, result.status) for result in first] == [
        ("review-fresh", "created"),
        ("review-low", "skipped"),
    ]
    assert first[1].reason == f"score {first[1].score} below 60"
    assert first[1].score < 60
    assert all(result.review_id != "review-stale" for result in first)
    assert [(result.review_id, result.status, result.reason) for result in second] == [
        ("review-fresh", "skipped", "open duplicate"),
        ("review-low", "skipped", f"score {second[1].score} below 60"),
    ]
    assert second[0].idea_id == first[0].idea_id
    assert len(db.get_content_ideas(status=None)) == 1


def test_supports_review_activity_type_limit_and_filters_trivial_rows(db):
    _review_activity(db, review_id="1", activity_type="pull_request_review", days_ago=0.1)
    _review_activity(db, review_id="2", body="LGTM", state="approved")
    _review_activity(db, review_id="3", author="dependabot[bot]")

    results = seed_github_pr_review_ideas(
        db,
        days=7,
        limit=2,
        dry_run=True,
        now=NOW,
    )

    assert [(result.review_id, result.status) for result in results] == [
        ("review-1", "proposed")
    ]


def test_formatters_and_cli_support_table_and_json(db, capsys):
    _review_activity(db)
    results = seed_github_pr_review_ideas(db, days=7, dry_run=True, now=NOW)

    payload = json.loads(format_github_pr_review_seed_json(results))
    assert payload["summary"]["proposed"] == 1
    table = format_github_pr_review_seed_text(results)
    assert "created=0 proposed=1 skipped=0" in table
    assert "alpha/app#42/review-9001" in table

    with patch.object(
        seed_github_pr_review_ideas_script,
        "script_context",
        return_value=_script_context(db),
    ), patch.object(
        seed_github_pr_review_ideas_script,
        "seed_github_pr_review_ideas",
        wraps=lambda db, **kwargs: seed_github_pr_review_ideas(db, now=NOW, **kwargs),
    ):
        exit_code = seed_github_pr_review_ideas_script.main(
            [
                "--days",
                "7",
                "--min-score",
                "60",
                "--limit",
                "5",
                "--dry-run",
                "--json",
            ]
        )

    assert exit_code == 0
    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["summary"]["proposed"] == 1
    assert cli_payload["results"][0]["source_metadata"]["score_reasons"]
