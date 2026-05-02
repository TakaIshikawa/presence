"""Tests for seeding content ideas from stored GitHub discussions."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from synthesis.github_discussion_idea_seeder import (
    format_github_discussion_idea_seed_json,
    format_github_discussion_idea_seed_text,
    seed_github_discussion_ideas,
)


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "seed_github_discussion_ideas.py"
)
spec = importlib.util.spec_from_file_location("seed_github_discussion_ideas_cli", SCRIPT_PATH)
seed_github_discussion_ideas_cli = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(seed_github_discussion_ideas_cli)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _discussion(
    db,
    *,
    repo: str = "taka/presence",
    number: int | str = 12,
    activity_type: str = "discussion",
    title: str = "How should discussion ideas preserve product reasoning?",
    body: str = (
        "The discussion explains why users prefer evidence-backed content ideas "
        "that carry the original GitHub URL, repo context, and the product tradeoff."
    ),
    updated_at: str = "2026-04-30T12:00:00+00:00",
) -> int:
    return db.upsert_github_activity(
        repo_name=repo,
        activity_type=activity_type,
        number=number,
        title=title,
        state="open",
        author="taka",
        url=f"https://github.com/{repo}/discussions/{number}",
        updated_at=updated_at,
        created_at="2026-04-30T10:00:00+00:00",
        body=body,
        labels=["question", "product"],
        metadata={"activity_id": f"{repo}#{number}:{activity_type}"},
    )


def test_seeder_creates_deterministic_content_ideas_from_discussion_activity(db):
    _discussion(db)
    _discussion(
        db,
        number=13,
        title="Discussion comment captures a launch positioning tradeoff",
        activity_type="discussion_comment",
        updated_at="2026-04-29T12:00:00+00:00",
    )

    report = seed_github_discussion_ideas(
        db,
        days=14,
        min_body_length=0,
        now=NOW,
    )

    assert [result.status for result in report.results] == ["created", "created"]
    assert [result.number for result in report.results] == ["12", "13"]
    assert report.totals == {
        "scanned": 2,
        "eligible": 2,
        "processed": 2,
        "created": 2,
        "proposed": 0,
        "skipped": 0,
    }
    ideas = db.get_content_ideas(status="open")
    assert len(ideas) == 2
    first = ideas[0]
    assert first["source"] == "github_discussion_idea_seeder"
    assert "Evidence: https://github.com/taka/presence/discussions/12" in first["note"]
    metadata = json.loads(first["source_metadata"])
    assert metadata["activity_id"] == "taka/presence#12:discussion"
    assert metadata["repo_name"] == "taka/presence"
    assert metadata["evidence_urls"] == ["https://github.com/taka/presence/discussions/12"]


def test_dry_run_returns_candidates_without_inserting_rows(db):
    _discussion(db)

    report = seed_github_discussion_ideas(
        db,
        days=14,
        dry_run=True,
        min_body_length=0,
        now=NOW,
    )

    assert [(result.status, result.reason) for result in report.results] == [
        ("proposed", "dry_run")
    ]
    assert report.totals["proposed"] == 1
    assert db.get_content_ideas(status="open") == []


def test_repeated_runs_skip_existing_equivalent_ideas(db):
    _discussion(db)

    first = seed_github_discussion_ideas(db, days=14, min_body_length=0, now=NOW)
    second = seed_github_discussion_ideas(db, days=14, min_body_length=0, now=NOW)

    assert [result.status for result in first.results] == ["created"]
    assert [result.status for result in second.results] == ["skipped"]
    assert second.results[0].idea_id == first.results[0].idea_id
    assert second.results[0].reason == "open duplicate"
    assert second.skipped_reasons == {"open duplicate": 1}
    assert len(db.get_content_ideas(status=None)) == 1


def test_deduplicates_by_normalized_title_and_body_metadata(db):
    _discussion(db)
    db.add_content_idea(
        note="Existing manual idea",
        topic="Manual topic",
        source="manual",
        source_metadata={
            "normalized_title": "how should discussion ideas preserve product reasoning?",
            "normalized_body": (
                "the discussion explains why users prefer evidence-backed content ideas "
                "that carry the original github url, repo context, and the product tradeoff."
            ),
        },
    )

    report = seed_github_discussion_ideas(db, days=14, min_body_length=0, now=NOW)

    assert [result.status for result in report.results] == ["skipped"]
    assert report.results[0].reason == "normalized title/body duplicate"
    assert len(db.get_content_ideas(status=None)) == 1


def test_filters_by_repo_days_limit_and_body_length(db):
    _discussion(db, repo="taka/presence", number=12)
    _discussion(db, repo="taka/other", number=14)
    _discussion(db, number=15, updated_at="2026-03-01T12:00:00+00:00")
    _discussion(db, number=16, body="too short")

    report = seed_github_discussion_ideas(
        db,
        days=14,
        repo="taka/presence",
        limit=1,
        min_body_length=40,
        dry_run=True,
        now=NOW,
    )

    assert [result.number for result in report.results] == ["12"]
    assert report.totals["scanned"] == 2
    assert report.totals["eligible"] == 1
    assert report.skipped_reasons == {"body_too_short": 1}


def test_json_text_and_cli_outputs_include_counts_and_skipped_reasons(db, capsys):
    _discussion(db)
    _discussion(db, number=16, body="too short")
    report = seed_github_discussion_ideas(
        db,
        days=14,
        dry_run=True,
        min_body_length=40,
        now=NOW,
    )

    payload = json.loads(format_github_discussion_idea_seed_json(report))
    assert payload["artifact_type"] == "github_discussion_idea_seed"
    assert payload["totals"]["proposed"] == 1
    assert payload["skipped_reasons"] == {"body_too_short": 1}
    text = format_github_discussion_idea_seed_text(report)
    assert "GitHub Discussion Idea Seeder" in text
    assert "Skipped reasons: body_too_short=1" in text

    with patch.object(
        seed_github_discussion_ideas_cli,
        "script_context",
        wraps=lambda: _script_context(db),
    ), patch.object(
        seed_github_discussion_ideas_cli,
        "seed_github_discussion_ideas",
        wraps=lambda db, **kwargs: seed_github_discussion_ideas(db, now=NOW, **kwargs),
    ):
        assert (
            seed_github_discussion_ideas_cli.main(
                [
                    "--days",
                    "14",
                    "--repo",
                    "taka/presence",
                    "--min-body-length",
                    "40",
                    "--dry-run",
                    "--format",
                    "json",
                ]
            )
            == 0
        )

    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["totals"]["proposed"] == 1
    assert cli_payload["results"][0]["source_activity_id"] == "taka/presence#12:discussion"


def test_invalid_filters_return_cli_errors(capsys):
    assert seed_github_discussion_ideas_cli.main(["--repo", " "]) == 1
    assert "repo must not be blank" in capsys.readouterr().err
