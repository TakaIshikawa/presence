"""Tests for GitHub issue comment idea seeding."""

from __future__ import annotations

import importlib.util
import json
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from synthesis.github_issue_comment_idea_seeder import (
    SOURCE_NAME,
    format_github_issue_comment_seed_json,
    format_github_issue_comment_seed_text,
    seed_github_issue_comment_ideas,
)


NOW = datetime(2026, 4, 30, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "seed_github_issue_comment_ideas.py"
)
spec = importlib.util.spec_from_file_location("seed_github_issue_comment_ideas_script", SCRIPT_PATH)
seed_github_issue_comment_ideas_script = importlib.util.module_from_spec(spec)
spec.loader.exec_module(seed_github_issue_comment_ideas_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _issue_comment(
    db,
    *,
    repo_name: str = "alpha/app",
    issue_number: int = 42,
    comment_id: str = "1001",
    author: str = "alice",
    body: str = (
        "I cannot tell whether this setting should be enabled for hosted runners. "
        "The behavior is confusing, and the workaround in the issue thread seems "
        "to trade reliability for setup complexity."
    ),
    days_ago: float = 1,
    title: str = "Runner setup is unclear",
) -> int:
    updated_at = (NOW - timedelta(days=days_ago)).isoformat()
    return db.upsert_github_activity(
        repo_name=repo_name,
        activity_type="issue_comment",
        number=comment_id,
        title=title,
        state="open",
        author=author,
        url=f"https://github.com/{repo_name}/issues/{issue_number}#issuecomment-{comment_id}",
        updated_at=updated_at,
        created_at=(NOW - timedelta(days=days_ago, hours=1)).isoformat(),
        body=body,
        labels=[],
        metadata={
            "issue_number": issue_number,
            "comment_id": comment_id,
            "issue_title": title,
        },
    )


def test_seed_creates_content_idea_with_issue_comment_source_metadata(db):
    _issue_comment(db)

    results = seed_github_issue_comment_ideas(db, days=7, now=NOW)

    assert [result.status for result in results] == ["created"]
    assert results[0].repo_name == "alpha/app"
    assert results[0].issue_number == "42"
    assert results[0].comment_id == "1001"
    assert results[0].author == "alice"
    assert "support thread" in results[0].note or "user confusion" in results[0].note
    ideas = db.get_content_ideas(status="open")
    assert len(ideas) == 1
    assert ideas[0]["source"] == SOURCE_NAME
    metadata = json.loads(ideas[0]["source_metadata"])
    assert metadata["repo_name"] == "alpha/app"
    assert metadata["issue_number"] == "42"
    assert metadata["comment_id"] == "1001"
    assert metadata["activity_id"] == "alpha/app#1001:issue_comment"
    assert metadata["author"] == "alice"
    assert metadata["url"].endswith("#issuecomment-1001")
    assert metadata["updated_at"] == "2026-04-29T12:00:00+00:00"


def test_dry_run_returns_proposed_ideas_without_writing(db):
    _issue_comment(db, comment_id="1001")

    results = seed_github_issue_comment_ideas(db, days=7, dry_run=True, now=NOW)

    assert [result.status for result in results] == ["proposed"]
    assert results[0].idea_id is None
    assert results[0].source_metadata["comment_id"] == "1001"
    assert db.get_content_ideas(status="open") == []


def test_duplicate_source_metadata_is_skipped_deterministically(db):
    _issue_comment(db, comment_id="1001")

    first = seed_github_issue_comment_ideas(db, days=7, now=NOW)
    second = seed_github_issue_comment_ideas(db, days=7, now=NOW)

    assert first[0].status == "created"
    assert second[0].status == "skipped"
    assert second[0].reason == "open duplicate"
    assert second[0].idea_id == first[0].idea_id
    assert len(db.get_content_ideas(status="open")) == 1


def test_filters_by_repo_author_exclude_author_and_limit(db):
    _issue_comment(db, repo_name="alpha/app", comment_id="1001", author="alice", days_ago=1)
    _issue_comment(db, repo_name="alpha/app", comment_id="1002", author="bob", days_ago=2)
    _issue_comment(db, repo_name="beta/api", comment_id="1003", author="alice", days_ago=1)

    results = seed_github_issue_comment_ideas(
        db,
        days=7,
        repo="alpha/app",
        author=["alice", "bob"],
        exclude_author=["bob"],
        limit=1,
        dry_run=True,
        now=NOW,
    )

    assert len(results) == 1
    assert results[0].repo_name == "alpha/app"
    assert results[0].author == "alice"
    assert results[0].comment_id == "1001"


def test_skips_old_short_and_non_issue_comment_rows(db):
    _issue_comment(db, comment_id="old", days_ago=30)
    _issue_comment(db, comment_id="short", body="Too short to become a useful idea.")
    db.upsert_github_activity(
        repo_name="alpha/app",
        activity_type="issue",
        number=44,
        title="Regular issue",
        state="open",
        author="alice",
        url="https://github.com/alpha/app/issues/44",
        updated_at=(NOW - timedelta(days=1)).isoformat(),
        body="This issue body is long enough but should not be selected as a comment.",
        labels=[],
        metadata={},
    )

    results = seed_github_issue_comment_ideas(db, days=7, now=NOW)

    assert results == []
    assert db.get_content_ideas(status="open") == []


def test_formatters_are_deterministic(db):
    _issue_comment(db, comment_id="1001")
    results = seed_github_issue_comment_ideas(db, days=7, dry_run=True, now=NOW)

    assert format_github_issue_comment_seed_json(results) == format_github_issue_comment_seed_json(
        results
    )
    assert format_github_issue_comment_seed_text(results) == format_github_issue_comment_seed_text(
        results
    )
    payload = json.loads(format_github_issue_comment_seed_json(results))
    text = format_github_issue_comment_seed_text(results)
    assert payload["summary"]["proposed"] == 1
    assert payload["results"][0]["comment_id"] == "1001"
    assert "created=0 proposed=1 skipped=0" in text
    assert "alpha/app#42/1001" in text


def test_cli_supports_requested_filters_and_json(db, capsys):
    _issue_comment(db, repo_name="alpha/app", comment_id="1001", author="alice")
    _issue_comment(db, repo_name="alpha/app", comment_id="1002", author="bot")

    with patch.object(
        seed_github_issue_comment_ideas_script,
        "script_context",
        return_value=_script_context(db),
    ), patch.object(seed_github_issue_comment_ideas_script, "datetime") as datetime_mock:
        datetime_mock.now.return_value = NOW
        datetime_mock.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
        exit_code = seed_github_issue_comment_ideas_script.main(
            [
                "--days",
                "7",
                "--repo",
                "alpha/app",
                "--author",
                "alice",
                "--exclude-author",
                "bot",
                "--limit",
                "5",
                "--dry-run",
                "--json",
            ]
        )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["summary"]["proposed"] == 1
    assert payload["results"][0]["author"] == "alice"
    assert db.get_content_ideas(status="open") == []
