"""Tests for GitHub Discussion digest content idea seeding."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from discussion_digest import format_results_json, format_results_table, main
from synthesis.discussion_digest import (
    build_discussion_candidates,
    seed_discussion_ideas,
)


NOW = datetime(2026, 4, 23, tzinfo=timezone.utc)


def _add_discussion(
    db,
    *,
    repo: str = "taka/presence",
    number: int = 4,
    title: str = "How should discussion digests handle unanswered questions?",
    body: str = "Users need a clearer path from unanswered discussions to durable content ideas.",
    state: str = "open",
    category: str = "Q&A",
    comments: int = 3,
    updated_at: str = "2026-04-22T12:00:00+00:00",
    labels: list[str] | None = None,
) -> int:
    metadata = {
        "category": {"name": category, "slug": category.lower().replace("&", "and")},
        "comments_count": comments,
    }
    if state == "answered":
        metadata["answer"] = {"body": "Use the digest workflow.", "chosen_by": "taka"}
    return db.upsert_github_activity(
        repo_name=repo,
        activity_type="discussion",
        number=number,
        title=title,
        state=state,
        author="octo",
        url=f"https://github.com/{repo}/discussions/{number}",
        updated_at=updated_at,
        created_at="2026-04-22T10:00:00+00:00",
        body=body,
        labels=labels or ["question"],
        metadata=metadata,
    )


def test_build_discussion_candidates_groups_by_repo_category_and_ranks(db):
    _add_discussion(db, number=4, comments=6)
    _add_discussion(
        db,
        number=5,
        title="How can discussion digests find repeated docs confusion?",
        body="The same docs confusion appears in discussions and deserves a practical answer.",
        comments=2,
        labels=["docs"],
    )
    _add_discussion(
        db,
        repo="taka/other",
        number=8,
        title="General announcement",
        body="FYI only.",
        state="answered",
        category="Announcements",
        comments=0,
        labels=[],
    )
    rows = db.get_recent_github_discussions(days=7, now=NOW, limit=None)

    candidates = build_discussion_candidates(rows, min_score=35, now=NOW)

    assert [candidate.number for candidate in candidates][:2] == [4, 5]
    assert candidates[0].category == "Q&A"
    assert candidates[0].source_metadata["theme_terms"]
    assert "unanswered+24" in candidates[0].score_reasons
    assert "Suggested angle:" in candidates[0].note


def test_seed_discussion_ideas_creates_content_ideas_with_source_metadata(db):
    _add_discussion(db)

    results = seed_discussion_ideas(db, days=7, now=NOW)

    assert [(result.status, result.repo_name, result.number) for result in results] == [
        ("created", "taka/presence", 4)
    ]
    ideas = db.get_content_ideas(status="open")
    assert len(ideas) == 1
    idea = ideas[0]
    assert idea["source"] == "github_discussion_digest"
    assert "GitHub Discussion #4 in taka/presence" in idea["note"]
    metadata = json.loads(idea["source_metadata"])
    assert metadata["source"] == "github_discussion_digest"
    assert metadata["activity_id"] == "taka/presence#4:discussion"
    assert metadata["category"] == "Q&A"
    assert metadata["comments_count"] == 3
    assert metadata["answered"] is False


def test_seed_discussion_ideas_skips_open_or_promoted_duplicates(db):
    _add_discussion(db)
    first = seed_discussion_ideas(db, days=7, now=NOW)
    second = seed_discussion_ideas(db, days=7, now=NOW)

    assert [result.status for result in first] == ["created"]
    assert [result.status for result in second] == ["skipped"]
    assert second[0].idea_id == first[0].idea_id
    assert second[0].reason == "open duplicate"

    db.promote_content_idea(first[0].idea_id, target_date="2026-05-01")
    third = seed_discussion_ideas(db, days=7, now=NOW)

    assert [result.status for result in third] == ["skipped"]
    assert third[0].idea_id == first[0].idea_id
    assert third[0].reason == "promoted duplicate"
    assert len(db.get_content_ideas(status=None)) == 1


def test_seed_discussion_ideas_dry_run_json_limit_and_repo_filter_do_not_write(db):
    _add_discussion(db, repo="taka/presence", number=4)
    _add_discussion(db, repo="taka/other", number=9, comments=5)

    results = seed_discussion_ideas(
        db,
        days=7,
        repo="taka/other",
        dry_run=True,
        limit=1,
        now=NOW,
    )
    payload = json.loads(format_results_json(results))

    assert len(payload) == 1
    assert payload[0]["status"] == "proposed"
    assert payload[0]["repo_name"] == "taka/other"
    assert payload[0]["reason"] == "dry run"
    assert db.get_content_ideas(status="open") == []


def test_seed_discussion_ideas_ignores_old_or_low_score_discussions(db):
    _add_discussion(db, updated_at="2026-03-01T12:00:00+00:00")
    _add_discussion(
        db,
        number=10,
        title="FYI",
        body="Small note.",
        state="answered",
        comments=0,
        labels=[],
    )

    assert seed_discussion_ideas(db, days=7, min_score=80, now=NOW) == []


def test_format_results_table_prints_summary(db):
    _add_discussion(db)
    results = seed_discussion_ideas(db, days=7, now=NOW)

    output = format_results_table(results)

    assert "created=1 proposed=0 skipped=0" in output
    assert "taka/presence" in output
    assert "Q&A" in output


def test_main_prints_dry_run_json_without_persisting(db, capsys):
    _add_discussion(db)

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("discussion_digest.script_context", fake_script_context):
        main(["--days", "7", "--repo", "taka/presence", "--dry-run", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["status"] == "proposed"
    assert payload[0]["repo_name"] == "taka/presence"
    assert db.get_content_ideas(status="open") == []
