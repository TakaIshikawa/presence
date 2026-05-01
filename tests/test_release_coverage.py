"""Tests for GitHub release coverage reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.release_coverage import (
    build_release_coverage_report,
    format_json_report,
    format_text_report,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "release_coverage.py"
spec = importlib.util.spec_from_file_location("release_coverage_script", SCRIPT_PATH)
release_coverage_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(release_coverage_script)

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _add_release(
    db,
    *,
    repo: str = "taka/presence",
    release_id: int = 101,
    tag: str = "v1.0.0",
    title: str | None = None,
    updated_at: str = "2026-04-30T12:00:00+00:00",
) -> int:
    return db.upsert_github_activity(
        repo_name=repo,
        activity_type="release",
        number=tag,
        title=title or f"Release {tag}",
        state="published",
        author="taka",
        url=f"https://github.com/{repo}/releases/tag/{tag}",
        updated_at=updated_at,
        created_at="2026-04-30T10:00:00+00:00",
        body="## What's Changed\n- Added release coverage",
        metadata={
            "release_id": release_id,
            "tag_name": tag,
            "published_at": updated_at,
            "draft": False,
            "prerelease": False,
        },
    )


def _add_content(
    db,
    *,
    content_type: str = "x_post",
    content: str = "Generated follow-up",
    source_activity_ids: list[str] | None = None,
    published: bool = False,
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        source_activity_ids=source_activity_ids or [],
        content=content,
        eval_score=8,
        eval_feedback="ok",
    )
    if published:
        db.conn.execute(
            """UPDATE generated_content
               SET published = 1, published_at = ?, published_url = ?
               WHERE id = ?""",
            (
                "2026-05-01T08:00:00+00:00",
                f"https://example.com/posts/{content_id}",
                content_id,
            ),
        )
        db.conn.commit()
    return content_id


def test_uncovered_recent_release_appears_as_gap(db):
    release_id = _add_release(db, tag="v1.2.3", title="Release v1.2.3")

    report = build_release_coverage_report(db, days=7, now=NOW)

    assert report["counts"] == {"total_releases": 1, "covered": 0, "uncovered": 1}
    assert report["items"][0]["id"] == release_id
    assert report["items"][0]["coverage_status"] == "uncovered"
    assert report["items"][0]["matched_content"] == []


def test_covered_release_includes_content_ids_and_publication_status(db):
    release_id = _add_release(db, tag="v2.0.0")
    content_id = _add_content(
        db,
        content_type="x_thread",
        source_activity_ids=[f"taka/presence#v2.0.0:release"],
        published=True,
    )
    db.conn.execute(
        """INSERT INTO content_publications
           (content_id, platform, status, platform_url, published_at)
           VALUES (?, ?, ?, ?, ?)""",
        (
            content_id,
            "bluesky",
            "published",
            "https://bsky.app/profile/example/post/1",
            "2026-05-01T09:00:00+00:00",
        ),
    )
    db.conn.commit()

    report = build_release_coverage_report(db, days=7, now=NOW)

    assert report["counts"] == {"total_releases": 1, "covered": 1, "uncovered": 0}
    item = report["items"][0]
    assert item["id"] == release_id
    assert item["coverage_status"] == "covered"
    assert item["matched_content"][0]["content_id"] == content_id
    assert item["matched_content"][0]["content_type"] == "x_thread"
    assert item["matched_content"][0]["publication_status"] == "published"
    assert item["matched_content"][0]["publications"][0]["platform"] == "bluesky"


def test_matches_by_numeric_activity_id_and_content_text_fallback(db):
    numeric_release_id = _add_release(db, repo="taka/api", tag="v3.0.0")
    _add_release(db, repo="taka/web", tag="v4.0.0", title="Release v4.0.0")
    numeric_content = _add_content(db, source_activity_ids=[str(numeric_release_id)])
    text_content = _add_content(
        db,
        content_type="blog_seed",
        content="Blog seed for the taka/web v4.0.0 release.",
    )

    report = build_release_coverage_report(db, days=7, now=NOW)
    by_repo = {item["repo_name"]: item for item in report["items"]}

    assert by_repo["taka/api"]["matched_content"][0]["content_id"] == numeric_content
    assert by_repo["taka/api"]["matched_content"][0]["match_reasons"] == [
        "source_activity_ids"
    ]
    assert by_repo["taka/web"]["matched_content"][0]["content_id"] == text_content
    assert by_repo["taka/web"]["matched_content"][0]["match_reasons"] == [
        "content_tag_reference"
    ]


def test_repo_filter_and_min_age_apply(db):
    _add_release(repo="taka/presence", db=db, tag="v1.0.0")
    _add_release(repo="taka/other", db=db, tag="v2.0.0")
    _add_release(
        repo="taka/presence",
        db=db,
        tag="v1.1.0",
        updated_at="2026-05-01T06:30:00+00:00",
    )

    report = build_release_coverage_report(
        db,
        days=7,
        repo="taka/presence",
        min_age_hours=12,
        now=NOW,
    )

    assert [item["tag_name"] for item in report["items"]] == ["v1.0.0"]
    assert report["filters"] == {
        "days": 7,
        "repo": "taka/presence",
        "min_age_hours": 12.0,
    }


def test_text_and_json_output_are_deterministic(db):
    _add_release(db, tag="v1.0.0")
    report = build_release_coverage_report(db, days=7, now=NOW)

    assert json.loads(format_json_report(report))["artifact_type"] == "release_coverage"
    assert format_text_report(report) == "\n".join(
        [
            "Release Coverage Report",
            "Generated: 2026-05-01T12:00:00+00:00",
            "Filters: days=7 repo=all min_age_hours=12",
            "Counts: releases=1 covered=0 uncovered=1",
            "",
            "Uncovered Releases",
            "  - taka/presence v1.0.0 [published] released=2026-04-30T12:00:00+00:00 title=Release v1.0.0 url=https://github.com/taka/presence/releases/tag/v1.0.0",
            "",
            "Covered Releases",
            "  none",
        ]
    )


def test_defensive_fallback_query_with_partial_schema():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE github_activity (
            id INTEGER PRIMARY KEY,
            repo_name TEXT,
            activity_type TEXT,
            number TEXT,
            title TEXT,
            state TEXT,
            url TEXT,
            updated_at TEXT,
            metadata TEXT
        );
        CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            content_type TEXT,
            content TEXT,
            source_metadata TEXT
        );
        INSERT INTO github_activity
          (id, repo_name, activity_type, number, title, state, url, updated_at, metadata)
        VALUES
          (1, 'taka/presence', 'release', 'v5.0.0', 'Release v5', 'published',
           'https://github.com/taka/presence/releases/tag/v5.0.0',
           '2026-04-30T12:00:00+00:00',
           '{"tag_name": "v5.0.0", "release_id": 500}');
        INSERT INTO generated_content
          (id, content_type, content, source_metadata)
        VALUES
          (10, 'newsletter_section', 'Newsletter section',
           '{"repo_name": "taka/presence", "release_id": 500}');
        """
    )

    report = build_release_coverage_report(conn, days=7, now=NOW)

    assert report["counts"] == {"total_releases": 1, "covered": 1, "uncovered": 0}
    assert report["items"][0]["matched_content"][0]["content_id"] == 10
    assert report["items"][0]["matched_content"][0]["publication_status"] == "draft"


def test_missing_required_tables_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE github_activity (id INTEGER PRIMARY KEY)")

    report = build_release_coverage_report(conn, now=NOW)

    assert report["missing_required_tables"] == ["generated_content"]
    assert report["items"] == []


def test_invalid_arguments_raise():
    with pytest.raises(ValueError, match="days"):
        build_release_coverage_report(sqlite3.connect(":memory:"), days=0)
    with pytest.raises(ValueError, match="min_age_hours"):
        build_release_coverage_report(sqlite3.connect(":memory:"), min_age_hours=-1)


def test_cli_outputs_json(db, capsys, monkeypatch):
    _add_release(db, tag="v9.0.0")
    monkeypatch.setattr(release_coverage_script, "script_context", lambda: _script_context(db))

    assert release_coverage_script.main(["--format", "json", "--days", "7"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "release_coverage"
    assert payload["items"][0]["tag_name"] == "v9.0.0"
