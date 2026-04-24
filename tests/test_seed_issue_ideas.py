"""Tests for the stale GitHub issue idea seeding CLI."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from seed_issue_ideas import format_results_table, main, seed_issue_ideas


NOW = datetime(2026, 4, 23, tzinfo=timezone.utc)


def _add_issue(
    db,
    *,
    repo: str = "taka/presence",
    number: int = 7,
    updated_at: str = "2026-03-01T12:00:00+00:00",
    labels: list[str] | None = None,
) -> int:
    return db.upsert_github_activity(
        repo_name=repo,
        activity_type="issue",
        number=number,
        title="Docs need clearer setup troubleshooting",
        state="open",
        author="taka",
        url=f"https://github.com/{repo}/issues/{number}",
        updated_at=updated_at,
        created_at="2026-02-20T10:00:00+00:00",
        body="Users keep asking how to diagnose setup failures.",
        labels=labels or ["docs"],
    )


def test_format_results_table_includes_created_skipped_and_duplicate_counts(db):
    _add_issue(db, number=1, labels=["docs"])
    _add_issue(db, number=2, labels=["maintenance"])
    first = seed_issue_ideas(db, days_stale=30, now=NOW)
    second = seed_issue_ideas(db, days_stale=30, now=NOW)

    output = format_results_table(first + second)

    assert "created=1 proposed=0 skipped=2 duplicate=1" in output
    assert "taka/presence" in output
    assert "label filter" in output
    assert "open duplicate" in output


def test_main_prints_compact_table_and_honors_options(db, capsys):
    _add_issue(db, repo="taka/presence", number=1, labels=["docs"])
    _add_issue(db, repo="taka/other", number=2, labels=["question"])

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("seed_issue_ideas.script_context", fake_script_context):
        main(
            [
                "--days-stale",
                "30",
                "--repo",
                "taka/other",
                "--label",
                "question",
                "--priority",
                "high",
                "--limit",
                "1",
            ]
        )

    output = capsys.readouterr().out
    assert "created=1 proposed=0 skipped=0 duplicate=0" in output
    assert "taka/other" in output
    ideas = db.get_content_ideas(status="open", priority="high")
    assert len(ideas) == 1
    assert "taka/other" in ideas[0]["topic"]


def test_main_dry_run_json_returns_candidate_payloads_without_writes(db, capsys):
    _add_issue(db)

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("seed_issue_ideas.script_context", fake_script_context):
        main(["--days-stale", "30", "--dry-run", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["status"] == "proposed"
    assert payload[0]["idea_id"] is None
    assert payload[0]["source_metadata"]["source"] == "github_issue_stale_seed"
    assert payload[0]["source_metadata"]["activity_id"] == "taka/presence#7:issue"
    assert payload[0]["source_metadata"]["stale_days"] >= 30
    assert db.get_content_ideas(status="open") == []
