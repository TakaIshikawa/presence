"""Tests for GitHub release digest building."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from release_digest import main
from synthesis.release_digest import (
    build_release_digest,
    format_release_digest_json,
    format_release_digest_text,
    parse_release_body,
    seed_digest_content_ideas,
)


NOW = datetime(2026, 4, 23, tzinfo=timezone.utc)


def _add_release(
    db,
    *,
    repo: str = "taka/presence",
    release_id: int = 101,
    tag: str = "v1.0.0",
    title: str | None = None,
    updated_at: str = "2026-04-22T12:00:00+00:00",
    body: str = "## What's Changed\n- Added digest builder in #12\n- Fixed export via https://github.com/taka/presence/issues/13",
) -> int:
    return db.upsert_github_activity(
        repo_name=repo,
        activity_type="release",
        number=release_id,
        title=title or f"Release {tag}",
        state="published",
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
            "prerelease": False,
        },
    )


def test_parse_release_body_extracts_highlights_and_references():
    highlights, references = parse_release_body(
        """## What's Changed
- Add release digest support (#44)
- Fix newsletter export https://github.com/taka/presence/pull/45

```text
#999 ignored from code block
```
"""
    )

    assert highlights == [
        "Add release digest support (#44)",
        "Fix newsletter export https://github.com/taka/presence/pull/45",
    ]
    assert references == ["#44", "taka/presence#45"]


def test_build_release_digest_groups_releases_by_repository_and_preserves_sources(db):
    _add_release(db, repo="taka/presence", release_id=101, tag="v1.0.0")
    _add_release(db, repo="taka/other", release_id=202, tag="v2.0.0")
    db.upsert_github_activity(
        repo_name="taka/presence",
        activity_type="issue",
        number=7,
        title="Ignored issue",
        state="open",
        author="taka",
        url="https://github.com/taka/presence/issues/7",
        updated_at="2026-04-22T12:00:00+00:00",
        created_at="2026-04-22T10:00:00+00:00",
    )

    digest = build_release_digest(db, days=7, now=NOW)

    assert [repo.repo_name for repo in digest.repositories] == ["taka/other", "taka/presence"]
    assert digest.source_activity_ids == [
        "taka/other#202:release",
        "taka/presence#101:release",
    ]
    presence = digest.repositories[1].releases[0]
    assert presence.url == "https://github.com/taka/presence/releases/tag/v1.0.0"
    assert presence.source_activity_ids == ["taka/presence#101:release"]
    assert presence.references == ["#12", "taka/presence#13"]


def test_build_release_digest_filters_repo_and_lookback(db):
    _add_release(db, repo="taka/presence", release_id=101, tag="v1.0.0")
    _add_release(db, repo="taka/other", release_id=202, tag="v2.0.0")
    _add_release(
        db,
        repo="taka/other",
        release_id=303,
        tag="v0.1.0",
        updated_at="2026-03-01T12:00:00+00:00",
    )

    digest = build_release_digest(db, days=7, repo="taka/other", now=NOW)

    assert len(digest.repositories) == 1
    assert digest.repositories[0].repo_name == "taka/other"
    assert [release.tag_name for release in digest.repositories[0].releases] == ["v2.0.0"]


def test_release_digest_json_and_text_output(db):
    _add_release(db)
    digest = build_release_digest(db, days=7, now=NOW)

    json_output = format_release_digest_json(digest)
    text_output = format_release_digest_text(digest)

    payload = json.loads(json_output)
    assert payload["repositories"][0]["releases"][0]["tag_name"] == "v1.0.0"
    assert payload["repositories"][0]["releases"][0]["source_activity_ids"] == [
        "taka/presence#101:release"
    ]
    assert "## taka/presence" in text_output
    assert "Link: https://github.com/taka/presence/releases/tag/v1.0.0" in text_output
    assert "Source activity IDs: taka/presence#101:release" in text_output


def test_seed_digest_content_ideas_is_idempotent(db):
    _add_release(db)
    digest = build_release_digest(db, days=7, now=NOW)

    first = seed_digest_content_ideas(db, digest)
    second = seed_digest_content_ideas(db, digest)

    assert [result.status for result in first] == ["created"]
    assert [result.status for result in second] == ["skipped"]
    assert second[0].idea_id == first[0].idea_id
    ideas = db.get_content_ideas(status=None)
    assert len(ideas) == 1
    metadata = json.loads(ideas[0]["source_metadata"])
    assert metadata["release_id"] == 101
    assert metadata["source_activity_ids"] == ["taka/presence#101:release"]


def test_seed_digest_content_ideas_skips_existing_release_seed(db):
    _add_release(db)
    db.add_content_idea(
        note="Existing release idea",
        topic="taka/presence v1.0.0 release",
        source="github_release_seed",
        source_metadata={
            "source": "github_release_seed",
            "release_id": 101,
            "repo_name": "taka/presence",
        },
    )
    digest = build_release_digest(db, days=7, now=NOW)

    results = seed_digest_content_ideas(db, digest)

    assert [result.status for result in results] == ["skipped"]
    assert len(db.get_content_ideas(status=None)) == 1


def test_cli_supports_text_and_json_output(db, capsys):
    _add_release(db)

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("release_digest.script_context", fake_script_context), patch(
        "synthesis.release_digest.datetime"
    ) as mock_datetime:
        mock_datetime.now.return_value = NOW
        mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
        main(["--days", "7", "--repo", "taka/presence", "--format", "text"])

    text_output = capsys.readouterr().out
    assert "Release digest:" in text_output
    assert "taka/presence#101:release" in text_output

    with patch("release_digest.script_context", fake_script_context), patch(
        "synthesis.release_digest.datetime"
    ) as mock_datetime:
        mock_datetime.now.return_value = NOW
        mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
        main(["--days", "7", "--repo", "taka/presence", "--format", "json"])

    payload = json.loads(capsys.readouterr().out)
    assert payload["repositories"][0]["repo_name"] == "taka/presence"
    assert payload["repositories"][0]["releases"][0]["url"].endswith("/v1.0.0")
