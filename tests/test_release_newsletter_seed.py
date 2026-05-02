"""Tests for exporting release newsletter seed candidates."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

from synthesis.release_newsletter_seed import (
    build_release_newsletter_seed_report,
    format_release_newsletter_seed_json,
    format_release_newsletter_seed_text,
)


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "export_release_newsletter_seeds.py"
)
spec = importlib.util.spec_from_file_location("export_release_newsletter_seeds", SCRIPT_PATH)
export_release_newsletter_seeds = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(export_release_newsletter_seeds)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _release(
    db,
    *,
    repo: str = "taka/presence",
    tag: str = "v1.0.0",
    title: str | None = None,
    body: str = "## What's Changed\n- Added release newsletter seeds with deterministic JSON output.\n- Improved planning handoff with source activity metadata.",
    updated_at: str = "2026-04-30T12:00:00+00:00",
    activity_type: str = "release",
) -> int:
    return db.upsert_github_activity(
        repo_name=repo,
        activity_type=activity_type,
        number=tag,
        title=title or f"Release {tag}",
        state="published",
        author="taka",
        url=f"https://github.com/{repo}/releases/tag/{tag}",
        updated_at=updated_at,
        created_at="2026-04-30T10:00:00+00:00",
        body=body,
        metadata={
            "tag_name": tag,
            "published_at": updated_at,
            "activity_id": f"{repo}#{tag}:{activity_type}",
        },
    )


def test_export_ranks_by_freshness_and_content_richness(db):
    _release(
        db,
        tag="v1.0.0",
        body="Short but useful release notes for operators.",
        updated_at="2026-04-29T12:00:00+00:00",
    )
    _release(
        db,
        tag="v1.1.0",
        title="Release planner bridge",
        body=(
            "## What's Changed\n"
            "- Added a release newsletter seed exporter for timely project updates.\n"
            "- Included repo, URL, source activity id, and concise summary text.\n"
            "- Ranked candidates using freshness and release-note richness."
        ),
        updated_at="2026-04-30T12:00:00+00:00",
    )

    report = build_release_newsletter_seed_report(
        db,
        days=14,
        min_body_length=0,
        now=NOW,
    )

    assert [seed.tag_name for seed in report.seeds] == ["v1.1.0", "v1.0.0"]
    seed = report.seeds[0]
    assert seed.rank == 1
    assert seed.repo == "taka/presence"
    assert seed.release_title == "Release planner bridge"
    assert seed.url == "https://github.com/taka/presence/releases/tag/v1.1.0"
    assert seed.source_activity_id == "taka/presence#v1.1.0:release"
    assert "release newsletter seed exporter" in seed.summary_text
    assert report.totals == {"scanned": 2, "eligible": 2, "excluded_by_body_length": 0}


def test_export_filters_by_repo_days_and_minimum_body_length(db):
    _release(db, repo="taka/presence", tag="v1.0.0")
    _release(db, repo="taka/other", tag="v2.0.0")
    _release(
        db,
        repo="taka/presence",
        tag="v0.9.0",
        updated_at="2026-03-01T12:00:00+00:00",
    )
    _release(db, repo="taka/presence", tag="v1.0.1", body="too short")

    report = build_release_newsletter_seed_report(
        db,
        days=14,
        repo="taka/presence",
        min_body_length=40,
        now=NOW,
    )

    assert [seed.tag_name for seed in report.seeds] == ["v1.0.0"]
    assert report.totals["scanned"] == 2
    assert report.totals["excluded_by_body_length"] == 1


def test_empty_result_and_missing_table_are_human_readable():
    conn = sqlite3.connect(":memory:")
    try:
        report = build_release_newsletter_seed_report(conn, days=14, now=NOW)
    finally:
        conn.close()

    assert report.seeds == ()
    assert report.availability["github_activity"] is False
    assert report.missing_tables == ("github_activity",)
    text = format_release_newsletter_seed_text(report)
    assert "Release Newsletter Seeds" in text
    assert "No release newsletter seed candidates found." in text


def test_malformed_metadata_and_body_values_do_not_break_export(db):
    activity_id = _release(
        db,
        tag="v-bad",
        title="Malformed metadata release",
        body="",
    )
    db.conn.execute(
        "UPDATE github_activity SET metadata = ?, body = ? WHERE id = ?",
        ("{not-json", 12345, activity_id),
    )
    db.conn.commit()

    report = build_release_newsletter_seed_report(
        db,
        days=14,
        min_body_length=0,
        now=NOW,
    )

    assert len(report.seeds) == 1
    seed = report.seeds[0]
    assert seed.tag_name == "v-bad"
    assert seed.summary_text == "12345"
    assert seed.source_activity_id == "taka/presence#v-bad:release"


def test_json_text_and_cli_outputs_are_deterministic(db, capsys):
    _release(db)
    report = build_release_newsletter_seed_report(
        db,
        days=14,
        min_body_length=0,
        now=NOW,
    )

    assert format_release_newsletter_seed_json(report) == (
        format_release_newsletter_seed_json(report)
    )
    payload = json.loads(format_release_newsletter_seed_json(report))
    assert sorted(payload) == [
        "artifact_type",
        "availability",
        "filters",
        "generated_at",
        "missing_columns",
        "missing_tables",
        "seeds",
        "totals",
    ]
    assert payload["seeds"][0]["repo"] == "taka/presence"
    text = format_release_newsletter_seed_text(report)
    assert "Release Newsletter Seeds" in text
    assert "source: taka/presence#v1.0.0:release" in text

    with patch.object(
        export_release_newsletter_seeds,
        "script_context",
        wraps=lambda: _script_context(db),
    ), patch.object(
        export_release_newsletter_seeds,
        "build_release_newsletter_seed_report",
        wraps=lambda db, **kwargs: build_release_newsletter_seed_report(db, now=NOW, **kwargs),
    ):
        assert (
            export_release_newsletter_seeds.main(
                [
                    "--days",
                    "14",
                    "--repo",
                    "taka/presence",
                    "--min-body-length",
                    "0",
                    "--format",
                    "json",
                ]
            )
            == 0
        )

    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["seeds"][0]["source_activity_id"] == "taka/presence#v1.0.0:release"


def test_invalid_filters_return_cli_errors(capsys):
    assert export_release_newsletter_seeds.main(["--repo", " "]) == 1
    assert "repo must not be blank" in capsys.readouterr().err
