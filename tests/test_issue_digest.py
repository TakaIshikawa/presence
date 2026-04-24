"""Tests for GitHub issue digest generation and idea seeding."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from issue_digest import format_digest_json, format_digest_table, main
from synthesis.issue_digest import build_issue_digest, seed_issue_ideas


NOW = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)


def _add_issue(
    db,
    *,
    repo: str = "taka/presence",
    number: int = 1,
    title: str = "Fix customer workflow regression in issue digest",
    body: str = (
        "Customers hit a workflow regression when the digest pipeline tries to "
        "summarize issue activity with labels, comments, and source metadata for "
        "later review."
    ),
    state: str = "open",
    updated_at: str = "2026-04-22T12:00:00+00:00",
    created_at: str = "2026-04-22T11:00:00+00:00",
    closed_at: str | None = None,
    labels: list[str] | None = None,
    metadata: dict | None = None,
) -> int:
    if labels is None:
        labels = ["bug", "customer"]
    if metadata is None:
        metadata = {"comments_count": 3, "issue_event_type": "commented"}
    return db.upsert_github_activity(
        repo_name=repo,
        activity_type="issue",
        number=number,
        title=title,
        state=state,
        author="taka",
        url=f"https://github.com/{repo}/issues/{number}",
        updated_at=updated_at,
        created_at=created_at,
        closed_at=closed_at,
        labels=labels,
        body=body,
        metadata=metadata,
    )


def test_build_issue_digest_groups_by_repo_and_label_with_stable_order(db):
    _add_issue(db, repo="zeta/app", number=2, labels=["enhancement"])
    _add_issue(db, repo="alpha/app", number=1, labels=["bug", "customer"])
    _add_issue(
        db,
        repo="alpha/app",
        number=3,
        title="Close old login bug",
        state="closed",
        labels=["bug"],
        closed_at="2026-04-22T13:00:00+00:00",
        metadata={"comments_count": 0},
    )

    digest = build_issue_digest(db, days=7, now=NOW)

    assert [(group.repo_name, group.label) for group in digest.groups] == [
        ("alpha/app", "bug"),
        ("alpha/app", "customer"),
        ("zeta/app", "enhancement"),
    ]
    bug_group = digest.groups[0]
    assert [issue.number for issue in bug_group.issues] == [3, 1]
    assert bug_group.closed_count == 1
    assert bug_group.high_signal_count == 1
    assert "alpha/app / bug: 2 recent issues" in bug_group.summary
    payload = digest.to_dict()
    assert payload["groups"][0]["issues"][0]["activity_id"] == "alpha/app#3:issue"


def test_build_issue_digest_filters_repo_and_label(db):
    _add_issue(db, repo="taka/presence", number=1, labels=["bug"])
    _add_issue(db, repo="taka/presence", number=2, labels=["docs"])
    _add_issue(db, repo="taka/other", number=3, labels=["bug"])

    digest = build_issue_digest(db, days=7, repo="taka/presence", label="bug", now=NOW)

    assert [(group.repo_name, group.label) for group in digest.groups] == [
        ("taka/presence", "bug")
    ]
    assert [issue.number for issue in digest.groups[0].issues] == [1]


def test_seed_issue_ideas_creates_and_skips_duplicate_active_ideas(db):
    _add_issue(db)
    digest = build_issue_digest(db, days=7, now=NOW)

    first = seed_issue_ideas(db, digest)
    second = seed_issue_ideas(db, digest)

    assert [result.status for result in first] == ["created"]
    assert [result.status for result in second] == ["skipped"]
    assert second[0].idea_id == first[0].idea_id
    assert second[0].reason == "open duplicate"
    ideas = db.get_content_ideas(status="open")
    assert len(ideas) == 1
    metadata = json.loads(ideas[0]["source_metadata"])
    assert metadata["source"] == "github_issue_digest"
    assert metadata["activity_id"] == "taka/presence#1:issue"
    assert metadata["github_activity_id"] is not None
    assert metadata["digest_fingerprint"]


def test_seed_issue_ideas_dry_run_does_not_write_low_signal_skipped(db):
    _add_issue(db)
    _add_issue(
        db,
        number=2,
        title="Tiny chore",
        body="Small update.",
        labels=["chore"],
        metadata={"comments_count": 0},
    )
    digest = build_issue_digest(db, days=7, now=NOW)

    results = seed_issue_ideas(db, digest, dry_run=True)

    assert [(result.status, result.number) for result in results] == [("proposed", 1)]
    assert db.get_content_ideas(status="open") == []


def test_formatters_emit_reviewable_table_and_json(db):
    _add_issue(db)
    digest = build_issue_digest(db, days=7, now=NOW)

    table = format_digest_table(digest, seed_issue_ideas(db, digest, dry_run=True))
    payload = json.loads(format_digest_json(digest))

    assert "groups=2 issues=2 high_signal=2" in table
    assert "seed_results created=0 proposed=1 skipped=0" in table
    assert payload["digest"]["groups"][0]["repo_name"] == "taka/presence"


def test_cli_prints_json_and_can_write_output(file_db, capsys, tmp_path):
    _add_issue(file_db)
    output_path = tmp_path / "digest.json"

    main(["--db", str(file_db.db_path), "--days", "36500", "--json"])
    stdout_payload = json.loads(capsys.readouterr().out)

    main(
        [
            "--db",
            str(file_db.db_path),
            "--days",
            "36500",
            "--repo",
            "taka/presence",
            "--seed-ideas",
            "--json",
            "--output",
            str(output_path),
        ]
    )
    file_payload = json.loads(output_path.read_text())

    assert stdout_payload["digest"]["groups"][0]["repo_name"] == "taka/presence"
    assert file_payload["seed_results"][0]["status"] == "created"
    assert len(file_db.get_content_ideas(status="open")) == 1
