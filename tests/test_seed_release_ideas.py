"""Tests for seeding content ideas from GitHub releases."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from seed_release_ideas import format_results_table, main, seed_release_ideas


NOW = datetime(2026, 4, 23, tzinfo=timezone.utc)


def _add_release(
    db,
    *,
    repo: str = "taka/presence",
    release_id: int = 101,
    tag: str = "v1.0.0",
    updated_at: str = "2026-04-22T12:00:00+00:00",
    state: str = "published",
    body: str = "Added release-focused idea seeding and cleaner summaries.",
) -> int:
    return db.upsert_github_activity(
        repo_name=repo,
        activity_type="release",
        number=release_id,
        title=f"Release {tag}",
        state=state,
        author="taka",
        url=f"https://github.com/{repo}/releases/tag/{tag}",
        updated_at=updated_at,
        created_at="2026-04-22T10:00:00+00:00",
        body=body,
        metadata={
            "release_id": release_id,
            "tag_name": tag,
            "published_at": updated_at,
            "draft": False,
            "prerelease": state == "prerelease",
        },
    )


def test_seed_release_ideas_creates_release_ideas(db):
    _add_release(db)

    results = seed_release_ideas(db, days=7, now=NOW)

    assert [(result.status, result.repo_name, result.tag_name) for result in results] == [
        ("created", "taka/presence", "v1.0.0")
    ]
    ideas = db.get_content_ideas(status="open")
    assert len(ideas) == 1
    idea = ideas[0]
    assert idea["source"] == "github_release_seed"
    assert "Release v1.0.0 in taka/presence" in idea["note"]
    assert "Suggested angle:" in idea["note"]
    metadata = json.loads(idea["source_metadata"])
    assert metadata["source"] == "github_release_seed"
    assert metadata["release_id"] == 101
    assert metadata["repo_name"] == "taka/presence"
    assert metadata["tag_name"] == "v1.0.0"
    assert metadata["url"] == "https://github.com/taka/presence/releases/tag/v1.0.0"
    assert metadata["body_excerpt"] == "Added release-focused idea seeding and cleaner summaries."


def test_seed_release_ideas_skips_open_or_promoted_duplicates(db):
    _add_release(db)
    first = seed_release_ideas(db, days=7, now=NOW)
    second = seed_release_ideas(db, days=7, now=NOW)

    assert [result.status for result in first] == ["created"]
    assert [result.status for result in second] == ["skipped"]
    assert second[0].idea_id == first[0].idea_id
    assert second[0].reason == "open duplicate"

    db.promote_content_idea(first[0].idea_id, target_date="2026-05-01")
    third = seed_release_ideas(db, days=7, now=NOW)

    assert [result.status for result in third] == ["skipped"]
    assert third[0].idea_id == first[0].idea_id
    assert third[0].reason == "promoted duplicate"
    assert len(db.get_content_ideas(status=None)) == 1


def test_seed_release_ideas_dry_run_limit_and_repo_filter_do_not_write(db):
    _add_release(db, repo="taka/presence", release_id=101, tag="v1.0.0")
    _add_release(db, repo="taka/other", release_id=202, tag="v2.0.0")

    results = seed_release_ideas(
        db,
        days=7,
        repo="taka/other",
        dry_run=True,
        limit=1,
        now=NOW,
    )

    assert len(results) == 1
    assert results[0].status == "skipped"
    assert results[0].repo_name == "taka/other"
    assert results[0].reason == "dry run"
    assert db.get_content_ideas(status="open") == []


def test_seed_release_ideas_ignores_old_releases(db):
    _add_release(db, updated_at="2026-03-01T12:00:00+00:00")

    assert seed_release_ideas(db, days=7, now=NOW) == []


def test_format_results_table_prints_summary(db):
    _add_release(db)
    results = seed_release_ideas(db, days=7, now=NOW)

    output = format_results_table(results)

    assert "created=1 skipped=0" in output
    assert "taka/presence" in output
    assert "v1.0.0" in output


def test_main_prints_release_seed_summary(db, capsys):
    _add_release(db)

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("seed_release_ideas.script_context", fake_script_context), patch(
        "seed_release_ideas.datetime"
    ) as mock_datetime:
        mock_datetime.now.return_value = NOW
        mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
        main(["--days", "7", "--repo", "taka/presence"])

    output = capsys.readouterr().out
    assert "created=1 skipped=0" in output
    assert "taka/presence" in output
    assert "v1.0.0" in output
