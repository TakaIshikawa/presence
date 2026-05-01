"""Tests for GitHub pull request digest idea seeding."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from pull_request_digest import format_results_json, format_results_table, main  # noqa: E402
from synthesis.pull_request_digest import (  # noqa: E402
    build_pull_request_digest,
    seed_pull_request_ideas,
)


NOW = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)


def _add_pull_request(
    db,
    *,
    repo: str = "taka/presence",
    number: int = 12,
    title: str = "Ship pull request digest idea seeding workflow",
    body: str = (
        "This change connects merged pull request activity to content idea seeding "
        "with deterministic source metadata, duplicate checks, and reviewable notes."
    ),
    state: str = "closed",
    updated_at: str = "2026-04-22T12:00:00+00:00",
    created_at: str = "2026-04-21T10:00:00+00:00",
    closed_at: str | None = "2026-04-22T11:50:00+00:00",
    merged_at: str | None = "2026-04-22T11:55:00+00:00",
    labels: list[str] | None = None,
    metadata: dict | None = None,
) -> int:
    if labels is None:
        labels = ["enhancement", "docs"]
    if metadata is None:
        metadata = {
            "merged": True,
            "changed_files": 5,
            "additions": 180,
            "deletions": 35,
            "commits": 3,
            "files": [
                {"filename": "src/synthesis/pull_request_digest.py"},
                {"filename": "scripts/pull_request_digest.py"},
                {"filename": "tests/test_pull_request_digest.py"},
            ],
        }
    return db.upsert_github_activity(
        repo_name=repo,
        activity_type="pull_request",
        number=number,
        title=title,
        state=state,
        author="taka",
        url=f"https://github.com/{repo}/pull/{number}",
        updated_at=updated_at,
        created_at=created_at,
        closed_at=closed_at,
        merged_at=merged_at,
        labels=labels,
        body=body,
        metadata=metadata,
    )


def test_build_pull_request_digest_scores_and_extracts_file_hints(db):
    _add_pull_request(db)

    items = build_pull_request_digest(db, days=7, now=NOW)

    assert len(items) == 1
    item = items[0]
    assert item.repo_name == "taka/presence"
    assert item.number == 12
    assert item.activity_id == "taka/presence#12:pull_request"
    assert item.merged_at == "2026-04-22T11:55:00+00:00"
    assert item.changed_file_hints[:2] == [
        "src/synthesis/pull_request_digest.py",
        "scripts/pull_request_digest.py",
    ]
    assert item.score >= 42
    assert "merged+24" in item.score_reasons
    assert "Changed files: src/synthesis/pull_request_digest.py" in item.note
    assert item.source_metadata["source"] == "github_pull_request_digest"
    assert item.source_metadata["pull_request_number"] == 12


def test_build_pull_request_digest_filters_stale_low_signal_and_repo(db):
    _add_pull_request(db, repo="taka/presence", number=12)
    _add_pull_request(
        db,
        repo="taka/presence",
        number=13,
        title="Bump dependency",
        body="Routine update.",
        labels=["dependencies"],
        merged_at="2026-04-22T10:00:00+00:00",
        metadata={"merged": True, "changed_files": 1, "additions": 2, "deletions": 2},
    )
    _add_pull_request(
        db,
        repo="taka/presence",
        number=14,
        updated_at="2026-03-01T12:00:00+00:00",
        merged_at="2026-03-01T11:00:00+00:00",
    )
    _add_pull_request(db, repo="taka/other", number=15)

    items = build_pull_request_digest(db, days=7, repo="taka/presence", now=NOW)

    assert [item.number for item in items] == [12]
    assert all(item.repo_name == "taka/presence" for item in items)


def test_seed_pull_request_ideas_creates_content_ideas_with_metadata(db):
    _add_pull_request(db)

    results = seed_pull_request_ideas(db, days=7, now=NOW)

    assert [(result.status, result.repo_name, result.number) for result in results] == [
        ("created", "taka/presence", 12)
    ]
    ideas = db.get_content_ideas(status="open")
    assert len(ideas) == 1
    idea = ideas[0]
    assert idea["source"] == "github_pull_request_digest"
    assert "GitHub pull request #12 in taka/presence" in idea["note"]
    metadata = json.loads(idea["source_metadata"])
    assert metadata["source"] == "github_pull_request_digest"
    assert metadata["activity_id"] == "taka/presence#12:pull_request"
    assert metadata["repo_name"] == "taka/presence"
    assert metadata["number"] == 12
    assert metadata["changed_file_hints"][0] == "src/synthesis/pull_request_digest.py"


def test_seed_pull_request_ideas_skips_open_or_promoted_duplicates(db):
    _add_pull_request(db)
    first = seed_pull_request_ideas(db, days=7, now=NOW)
    second = seed_pull_request_ideas(db, days=7, now=NOW)

    assert [result.status for result in first] == ["created"]
    assert [result.status for result in second] == ["skipped"]
    assert second[0].idea_id == first[0].idea_id
    assert second[0].reason == "open duplicate"

    db.promote_content_idea(first[0].idea_id, target_date="2026-05-01")
    third = seed_pull_request_ideas(db, days=7, now=NOW)

    assert [result.status for result in third] == ["skipped"]
    assert third[0].idea_id == first[0].idea_id
    assert third[0].reason == "promoted duplicate"
    assert len(db.get_content_ideas(status=None)) == 1


def test_seed_pull_request_ideas_dry_run_json_limit_and_repo_filter_do_not_write(db):
    _add_pull_request(db, repo="taka/presence", number=12)
    _add_pull_request(db, repo="taka/other", number=21)

    results = seed_pull_request_ideas(
        db,
        days=7,
        repo="taka/other",
        dry_run=True,
        limit=1,
        now=NOW,
    )
    payload = json.loads(
        format_results_json(
            build_pull_request_digest(db, days=7, repo="taka/other", now=NOW),
            results,
        )
    )

    assert len(payload["seed_results"]) == 1
    assert payload["seed_results"][0]["status"] == "proposed"
    assert payload["seed_results"][0]["repo_name"] == "taka/other"
    assert payload["seed_results"][0]["reason"] == "dry run"
    assert db.get_content_ideas(status="open") == []


def test_format_results_table_prints_digest_and_seed_summary(db):
    _add_pull_request(db)
    items = build_pull_request_digest(db, days=7, now=NOW)
    results = seed_pull_request_ideas(db, days=7, dry_run=True, now=NOW)

    output = format_results_table(items, results)

    assert "pull_requests=1" in output
    assert "seed_results created=0 proposed=1 skipped=0" in output
    assert "taka/presence#12" in output


def test_main_prints_dry_run_json_without_persisting(db, capsys):
    _add_pull_request(db)

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("pull_request_digest.script_context", fake_script_context):
        main(["--days", "36500", "--repo", "taka/presence", "--dry-run", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert payload["seed_results"][0]["status"] == "proposed"
    assert payload["seed_results"][0]["repo_name"] == "taka/presence"
    assert db.get_content_ideas(status="open") == []
