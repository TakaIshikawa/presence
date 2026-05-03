"""Tests for GitHub discussion follow-through reporting."""

from __future__ import annotations

import csv
import importlib.util
import io
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from evaluation.github_discussion_followthrough import (
    build_github_discussion_followthrough_report,
    format_github_discussion_followthrough_csv,
    format_github_discussion_followthrough_json,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "github_discussion_followthrough.py"
spec = importlib.util.spec_from_file_location("github_discussion_followthrough_script", SCRIPT_PATH)
github_discussion_followthrough_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(github_discussion_followthrough_script)


def _discussion(
    db,
    *,
    repo_name: str = "alpha/app",
    number: int = 1,
    title: str = "Discussion",
    days_ago: int = 1,
    labels: list[str] | str | None = None,
) -> int:
    return db.upsert_github_activity(
        repo_name=repo_name,
        activity_type="discussion",
        number=number,
        title=title,
        state="open",
        author="taka",
        url=f"https://github.com/{repo_name}/discussions/{number}",
        updated_at=(NOW - timedelta(days=days_ago)).isoformat(),
        created_at=(NOW - timedelta(days=days_ago + 1)).isoformat(),
        labels=labels or [],
    )


def _content(db, source_activity_ids: list[str]) -> int:
    return db.insert_generated_content(
        "x_post",
        [],
        [],
        "Generated from discussion",
        8.0,
        "ok",
        source_activity_ids=source_activity_ids,
    )


def test_covered_discussions_are_linked_through_source_activity_ids(db):
    covered_activity_id = _discussion(db, number=10, title="Covered", labels=["idea"])
    uncovered_id = _discussion(db, number=11, title="Uncovered", labels=["idea"])
    content_id = _content(db, ["alpha/app#10:discussion", str(covered_activity_id)])

    report = build_github_discussion_followthrough_report(db, days_stale=7, now=NOW)
    by_id = {item.id: item for item in report.items if item.id is not None}

    assert by_id[covered_activity_id].status == "covered"
    assert by_id[covered_activity_id].linked_content_ids == (content_id,)
    assert by_id[covered_activity_id].activity_id == "alpha/app#10:discussion"
    assert by_id[uncovered_id].status == "fresh_uncovered"


def test_uncovered_discussions_are_split_into_fresh_and_stale(db):
    stale_id = _discussion(db, number=1, days_ago=15)
    fresh_id = _discussion(db, number=2, days_ago=6)

    report = build_github_discussion_followthrough_report(db, days_stale=7, now=NOW)
    by_id = {item.id: item for item in report.items}

    assert by_id[stale_id].status == "stale_uncovered"
    assert by_id[stale_id].age_days == 15
    assert by_id[fresh_id].status == "fresh_uncovered"
    assert by_id[fresh_id].age_days == 6


def test_repo_and_label_filters_apply_to_discussions(db):
    _discussion(db, repo_name="alpha/app", number=1, labels=["Idea", "docs"])
    _discussion(db, repo_name="alpha/app", number=2, labels=["bug"])
    _discussion(db, repo_name="beta/api", number=3, labels=["idea"])

    report = build_github_discussion_followthrough_report(
        db,
        repo="alpha/app",
        label="idea",
        now=NOW,
    )

    discussion_items = [item for item in report.items if item.status != "invalid_linkage"]
    assert [item.activity_id for item in discussion_items] == ["alpha/app#1:discussion"]
    assert discussion_items[0].labels == ("Idea", "docs")


def test_malformed_source_activity_ids_are_reported_without_aborting(db):
    discussion_id = _discussion(db, number=1)
    good_content_id = _content(db, ["alpha/app#1:discussion"])
    bad_content_id = _content(db, [])
    db.conn.execute(
        "UPDATE generated_content SET source_activity_ids = ? WHERE id = ?",
        ("not-json", bad_content_id),
    )
    db.conn.commit()

    report = build_github_discussion_followthrough_report(db, now=NOW)
    payload = json.loads(format_github_discussion_followthrough_json(report))

    assert payload["summary"]["covered"] == 1
    assert payload["summary"]["invalid_linkage"] == 1
    covered = next(item for item in report.items if item.id == discussion_id)
    invalid = next(item for item in report.items if item.status == "invalid_linkage")
    assert covered.linked_content_ids == (good_content_id,)
    assert invalid.content_id == bad_content_id
    assert invalid.linked_content_ids == (bad_content_id,)
    assert invalid.error.startswith("invalid_json")


def test_non_list_source_activity_ids_are_reported(db):
    _discussion(db, number=1)
    content_id = _content(db, [])
    db.conn.execute(
        "UPDATE generated_content SET source_activity_ids = ? WHERE id = ?",
        (json.dumps({"activity": "alpha/app#1:discussion"}), content_id),
    )
    db.conn.commit()

    report = build_github_discussion_followthrough_report(db, now=NOW)

    invalid = next(item for item in report.items if item.status == "invalid_linkage")
    assert invalid.error == "non_list_json: dict"


def test_csv_output_contains_required_fields(db):
    content_id = _content(db, ["alpha/app#1:discussion"])
    _discussion(db, number=1, title="Worth a post", labels=["idea"])

    report = build_github_discussion_followthrough_report(db, now=NOW)
    rows = list(csv.DictReader(io.StringIO(format_github_discussion_followthrough_csv(report))))

    assert rows[0]["status"] == "covered"
    assert rows[0]["title"] == "Worth a post"
    assert rows[0]["repo_name"] == "alpha/app"
    assert rows[0]["number"] == "1"
    assert rows[0]["url"] == "https://github.com/alpha/app/discussions/1"
    assert json.loads(rows[0]["labels"]) == ["idea"]
    assert json.loads(rows[0]["linked_content_ids"]) == [content_id]


def test_cli_supports_db_days_stale_repo_label_and_formats(file_db, capsys):
    _discussion(file_db, repo_name="alpha/app", number=1, days_ago=10, labels=["idea"])
    _discussion(file_db, repo_name="alpha/app", number=2, days_ago=10, labels=["bug"])

    assert (
        github_discussion_followthrough_script.main(
            [
                "--db",
                str(file_db.db_path),
                "--days-stale",
                "7",
                "--repo",
                "alpha/app",
                "--label",
                "idea",
                "--format",
                "json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"]["days_stale"] == 7
    assert payload["filters"]["repo"] == "alpha/app"
    assert payload["filters"]["label"] == "idea"
    assert payload["summary"]["stale_uncovered"] == 1
    assert payload["items"][0]["activity_id"] == "alpha/app#1:discussion"

    assert (
        github_discussion_followthrough_script.main(
            ["--db", str(file_db.db_path), "--format", "csv"]
        )
        == 0
    )
    assert "status,id,activity_id,title,repo_name" in capsys.readouterr().out


def test_invalid_days_stale_is_rejected(db):
    with pytest.raises(ValueError, match="days_stale must be positive"):
        build_github_discussion_followthrough_report(db, days_stale=0, now=NOW)
