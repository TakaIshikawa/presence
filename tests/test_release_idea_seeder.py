"""Tests for GitHub release idea seeding."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json

from synthesis.release_idea_seeder import (
    SOURCE_NAME,
    seed_release_ideas,
)


NOW = datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)


def _release(
    db,
    *,
    repo_name: str = "taka/presence",
    tag: str = "v2.0.0",
    title: str = "v2.0.0",
    body: str = "",
    prerelease: bool = False,
    draft: bool = False,
    days_ago: int = 1,
) -> int:
    """Insert a release row into github_activity via the db fixture."""
    updated_at = (NOW - timedelta(days=days_ago)).isoformat()
    metadata = {
        "tag_name": tag,
        "activity_id": f"{repo_name}#{tag}:release",
        "prerelease": prerelease,
        "draft": draft,
        "published_at": updated_at,
    }
    return db.upsert_github_activity(
        repo_name=repo_name,
        activity_type="release",
        number=tag,
        title=title,
        state="published" if not draft else "draft",
        author="releaser",
        url=f"https://github.com/{repo_name}/releases/tag/{tag}",
        updated_at=updated_at,
        created_at=(NOW - timedelta(days=days_ago, hours=1)).isoformat(),
        body=body,
        labels=[],
        metadata=metadata,
    )


def test_empty_db_returns_empty_report(db):
    report = seed_release_ideas(db, now=NOW)
    assert report.summary["candidates"] == 0
    assert report.results == ()
    assert report.to_dict()["generated_at"] == NOW.isoformat()


def test_single_major_release_scores_high(db):
    _release(db, tag="v3.0.0", title="Version 3.0.0")
    report = seed_release_ideas(db, dry_run=True, now=NOW)

    assert report.summary["candidates"] == 1
    assert report.summary["dry_run"] == 1
    assert report.results[0].status == "dry-run"
    assert report.results[0].score >= 40
    assert "major_version" in report.results[0].signals
    assert report.results[0].priority == "high"


def test_patch_release_scores_lower_than_major(db):
    _release(db, tag="v1.2.3", title="Patch fix")
    _release(db, tag="v2.0.0", title="Major release")

    report = seed_release_ideas(db, dry_run=True, min_score=0, now=NOW)

    # Major should sort first (higher score)
    assert report.results[0].tag == "v2.0.0"
    assert report.results[1].tag == "v1.2.3"
    assert report.results[0].score > report.results[1].score


def test_prerelease_scores_lower(db):
    _release(db, tag="v2.0.0", title="Stable", prerelease=False)
    _release(db, tag="v2.1.0-beta.1", title="Beta", prerelease=True)

    report = seed_release_ideas(db, dry_run=True, min_score=0, now=NOW)

    stable = next(r for r in report.results if r.tag == "v2.0.0")
    beta = next(r for r in report.results if r.tag == "v2.1.0-beta.1")
    assert stable.score > beta.score


def test_breaking_changes_boost_score(db):
    body_with_breaking = "## BREAKING CHANGE\nRemoved deprecated API endpoint."
    _release(db, tag="v4.0.0", title="Big update", body=body_with_breaking)

    report = seed_release_ideas(db, dry_run=True, now=NOW)

    assert "breaking_changes" in report.results[0].signals
    # Major + breaking = high score
    assert report.results[0].score >= 50


def test_detailed_changelog_boosts_score(db):
    long_body = "Changelog:\n" + "\n".join(f"- Fix #{i}" for i in range(50))
    _release(db, tag="v1.5.0", title="Feature release", body=long_body)

    report = seed_release_ideas(db, dry_run=True, now=NOW)

    assert any("changelog" in s for s in report.results[0].signals)


def test_multiple_releases_ranked_and_limited(db):
    _release(db, tag="v5.0.0", title="Major")
    _release(db, tag="v4.1.0", title="Minor")
    _release(db, tag="v4.0.1", title="Patch")

    report = seed_release_ideas(db, dry_run=True, limit=2, min_score=0, now=NOW)

    assert len(report.results) == 2
    assert report.summary["candidates"] == 2
    # Major version should be first
    assert report.results[0].tag == "v5.0.0"


def test_dedup_skips_already_seeded_release(db):
    _release(db, tag="v1.0.0", title="Initial")

    first = seed_release_ideas(db, dry_run=False, now=NOW)
    assert first.summary["created"] == 1
    assert first.results[0].idea_id is not None

    second = seed_release_ideas(db, dry_run=False, now=NOW)
    assert second.summary["skipped"] == 1
    assert second.results[0].status == "skipped"
    assert second.results[0].reason == "active duplicate"

    # Only one idea in DB
    ideas = db.get_content_ideas(status="open")
    assert len(ideas) == 1


def test_below_threshold_filtered_out(db):
    # Draft + prerelease + patch => very low score
    _release(db, tag="v0.0.1", title="Tiny", prerelease=True, draft=True)

    report = seed_release_ideas(db, dry_run=True, min_score=30, now=NOW)
    assert report.summary["candidates"] == 0
    assert len(report.results) == 0


def test_insert_mode_stores_correct_metadata(db):
    _release(db, tag="v2.0.0", title="V2 Release", repo_name="acme/lib")

    report = seed_release_ideas(db, dry_run=False, now=NOW)

    assert report.summary["created"] == 1
    ideas = db.get_content_ideas(status="open")
    assert len(ideas) == 1
    assert ideas[0]["source"] == SOURCE_NAME
    metadata = json.loads(ideas[0]["source_metadata"])
    assert metadata["source"] == SOURCE_NAME
    assert metadata["source_type"] == "github_release"
    assert metadata["repo_name"] == "acme/lib"
    assert metadata["tag"] == "v2.0.0"
    assert metadata["release_fingerprint"] == metadata["source_id"]


def test_old_releases_outside_window_excluded(db):
    _release(db, tag="v1.0.0", title="Old", days_ago=60)

    report = seed_release_ideas(db, days=30, dry_run=True, now=NOW)
    assert report.summary["candidates"] == 0


def test_report_to_dict_structure(db):
    _release(db, tag="v3.0.0", title="Major")

    report = seed_release_ideas(db, dry_run=True, now=NOW)
    payload = report.to_dict()

    assert "generated_at" in payload
    assert "filters" in payload
    assert "summary" in payload
    assert "results" in payload
    assert isinstance(payload["results"], list)
    assert payload["results"][0]["tag"] == "v3.0.0"
    assert isinstance(payload["results"][0]["signals"], list)


def test_contributors_signal_detected(db):
    body = "Thanks to @alice @bob @charlie @dave @eve for contributing!"
    _release(db, tag="v1.1.0", title="Community", body=body)

    report = seed_release_ideas(db, dry_run=True, min_score=0, now=NOW)
    assert "many_contributors" in report.results[0].signals


def test_frozen_dataclass_immutability(db):
    _release(db, tag="v1.0.0", title="Test")
    report = seed_release_ideas(db, dry_run=True, now=NOW)

    import dataclasses
    assert dataclasses.is_dataclass(report)
    assert dataclasses.is_dataclass(report.results[0])
