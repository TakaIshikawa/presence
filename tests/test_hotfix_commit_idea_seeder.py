"""Tests for hotfix commit content idea seeding."""

from __future__ import annotations

import importlib.util
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from synthesis.hotfix_commit_idea_seeder import (
    build_hotfix_commit_idea_candidates,
    format_hotfix_commit_idea_seed_json,
    format_hotfix_commit_idea_seed_text,
    seed_hotfix_commit_ideas,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "seed_hotfix_commit_ideas.py"
spec = importlib.util.spec_from_file_location("seed_hotfix_commit_ideas_script", SCRIPT_PATH)
seed_hotfix_commit_ideas_script = importlib.util.module_from_spec(spec)
spec.loader.exec_module(seed_hotfix_commit_ideas_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _commit(
    db,
    *,
    repo: str = "owner/repo",
    sha: str,
    message: str,
    days_ago: int = 1,
) -> int:
    return db.insert_commit(
        repo_name=repo,
        commit_sha=sha,
        commit_message=message,
        timestamp=(NOW - timedelta(days=days_ago)).isoformat(),
        author="taka",
    )


def test_recent_fix_revert_and_security_commits_create_deterministic_ideas(db):
    _commit(db, sha="fix111", message="fix: repair timeout regression in publisher")
    _commit(db, sha="rev222", message="Revert \"ship risky scheduler change\"")
    _commit(db, sha="sec333", message="security: rotate leaked webhook secret")
    _commit(db, sha="docs444", message="docs: update README troubleshooting notes")

    report = seed_hotfix_commit_ideas(db, days=7, min_score=20, now=NOW)

    assert report.summary == {"candidates": 3, "created": 3, "proposed": 0, "skipped": 0}
    ideas = db.get_content_ideas(status="open", limit=10, include_snoozed=True)
    assert len(ideas) == 3
    metadata_by_sha = {
        json.loads(idea["source_metadata"])["commit_sha"]: json.loads(idea["source_metadata"])
        for idea in ideas
    }
    assert set(metadata_by_sha) == {"fix111", "rev222", "sec333"}
    assert metadata_by_sha["fix111"]["source_type"] == "github_commit"
    assert metadata_by_sha["fix111"]["source_id"] == "fix111"
    assert metadata_by_sha["sec333"]["signals"] == ["security"]
    assert [result.commit_sha for result in report.results] == ["fix111", "rev222", "sec333"]
    text = format_hotfix_commit_idea_seed_text(report)
    assert "Hotfix commit idea seed report" in text
    assert "created=3" in text


def test_dry_run_returns_proposed_ideas_without_mutating_database(db):
    _commit(db, sha="hot123", message="hotfix: restore production webhook retries")

    report = seed_hotfix_commit_ideas(db, days=7, dry_run=True, now=NOW)

    assert report.summary == {"candidates": 1, "created": 0, "proposed": 1, "skipped": 0}
    assert report.results[0].status == "proposed"
    assert report.results[0].idea_id is None
    assert db.get_content_ideas(status="open") == []


def test_running_twice_skips_duplicate_open_idea_for_same_commit(db):
    _commit(db, sha="dup123", message="fix: stop production crash on empty payload")

    first = seed_hotfix_commit_ideas(db, days=7, now=NOW)
    second = seed_hotfix_commit_ideas(db, days=7, now=NOW)

    assert first.results[0].status == "created"
    assert second.summary == {"candidates": 1, "created": 0, "proposed": 0, "skipped": 1}
    assert second.results[0].idea_id == first.results[0].idea_id
    assert len(db.get_content_ideas(status="open", limit=10, include_snoozed=True)) == 1


def test_limit_min_score_and_old_commits_filter_candidates(db):
    _commit(db, sha="old111", message="hotfix: restore production checkout", days_ago=30)
    _commit(db, sha="prod222", message="fix: production checkout regression", days_ago=1)
    _commit(db, sha="fix333", message="fix: handle retry timeout", days_ago=1)

    candidates = build_hotfix_commit_idea_candidates(
        db,
        days=7,
        limit=1,
        min_score=30,
        now=NOW,
    )

    assert [candidate.commit_sha for candidate in candidates] == ["prod222"]
    assert candidates[0].signals == ("regression", "production", "fix")


def test_missing_optional_author_column_still_builds_candidates():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE github_commits (
            id INTEGER PRIMARY KEY,
            repo_name TEXT,
            commit_sha TEXT,
            commit_message TEXT,
            timestamp TEXT
        )"""
    )
    conn.execute(
        """INSERT INTO github_commits
           (repo_name, commit_sha, commit_message, timestamp)
           VALUES (?, ?, ?, ?)""",
        ("owner/repo", "noauthor", "revert production cache regression", NOW.isoformat()),
    )

    candidates = build_hotfix_commit_idea_candidates(conn, days=7, now=NOW)

    assert len(candidates) == 1
    assert candidates[0].author is None
    assert candidates[0].source_metadata["commit_sha"] == "noauthor"


def test_missing_required_commit_message_column_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE github_commits (
            id INTEGER PRIMARY KEY,
            repo_name TEXT,
            commit_sha TEXT,
            timestamp TEXT
        )"""
    )

    report = seed_hotfix_commit_ideas(conn, now=NOW)

    assert report.results == ()
    assert report.summary == {"candidates": 0, "created": 0, "proposed": 0, "skipped": 0}
    assert report.missing_required_columns == {"github_commits": ("commit_message",)}
    assert "Missing columns: github_commits(commit_message)" in format_hotfix_commit_idea_seed_text(report)


def test_json_formatter_is_stable(db):
    _commit(db, sha="json123", message="security: fix auth bypass regression")

    report = seed_hotfix_commit_ideas(db, days=7, dry_run=True, now=NOW)
    first = format_hotfix_commit_idea_seed_json(report)
    second = format_hotfix_commit_idea_seed_json(report)
    payload = json.loads(first)

    assert first == second
    assert payload["summary"]["proposed"] == 1
    assert payload["results"][0]["source_metadata"]["source_id"] == "json123"


def test_cli_supports_json_and_dry_run_without_mutating(db, capsys):
    _commit(db, sha="cli123", message="hotfix: mitigate production incident")

    with patch.object(
        seed_hotfix_commit_ideas_script,
        "script_context",
        return_value=_script_context(db),
    ):
        result = seed_hotfix_commit_ideas_script.main(
            ["--days", "7", "--limit", "5", "--min-score", "20", "--dry-run", "--json"]
        )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["summary"]["proposed"] == 1
    assert payload["results"][0]["commit_sha"] == "cli123"
    assert db.get_content_ideas(status="open") == []
