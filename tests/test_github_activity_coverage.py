"""Tests for uncovered GitHub activity reporting."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.github_activity_coverage import (  # noqa: E402
    format_github_activity_coverage_text,
    uncovered_github_activity_report,
)
from github_activity_coverage import main  # noqa: E402

NOW = datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc)


def _insert_activity(
    db,
    *,
    repo_name: str = "repo",
    activity_type: str = "issue",
    number: int = 1,
    title: str = "Issue",
    state: str = "open",
    updated_at: str = "2026-04-24T12:00:00+00:00",
) -> int:
    return db.upsert_github_activity(
        repo_name=repo_name,
        activity_type=activity_type,
        number=number,
        title=title,
        state=state,
        author="taka",
        url=f"https://github.com/taka/{repo_name}/{activity_type}/{number}",
        updated_at=updated_at,
        created_at="2026-04-01T12:00:00+00:00",
    )


def _insert_content(db, source_activity_ids: list[str]) -> int:
    return db.insert_generated_content(
        "x_post",
        [],
        [],
        "Generated content",
        8.0,
        "ok",
        source_activity_ids=source_activity_ids,
    )


def test_report_excludes_activities_referenced_by_source_activity_ids(db):
    _insert_activity(db, activity_type="issue", number=1, title="Covered")
    uncovered_id = _insert_activity(
        db,
        activity_type="pull_request",
        number=2,
        title="Uncovered PR",
        state="closed",
        updated_at="2026-04-25T10:00:00+00:00",
    )
    _insert_content(db, ["repo#1:issue"])

    report = uncovered_github_activity_report(db, now=NOW)

    assert [item["id"] for item in report["items"]] == [uncovered_id]
    assert report["items"][0] == {
        "id": uncovered_id,
        "activity_id": "repo#2:pull_request",
        "repo": "repo",
        "repo_name": "repo",
        "activity_type": "pull_request",
        "number": 2,
        "title": "Uncovered PR",
        "state": "closed",
        "url": "https://github.com/taka/repo/pull_request/2",
        "updated_at": "2026-04-25T10:00:00+00:00",
    }


def test_report_also_excludes_numeric_github_activity_ids(db):
    covered_id = _insert_activity(db, activity_type="release", number=100, title="Release")
    _insert_activity(db, activity_type="issue", number=2, title="Uncovered")
    _insert_content(db, [str(covered_id)])

    report = uncovered_github_activity_report(db, now=NOW)

    assert [item["activity_id"] for item in report["items"]] == ["repo#2:issue"]


def test_summary_counts_by_activity_type_and_state(db):
    _insert_activity(db, activity_type="issue", number=1, state="open")
    _insert_activity(db, activity_type="pull_request", number=2, state="closed")
    _insert_activity(db, activity_type="release", number=3, state="published")

    report = uncovered_github_activity_report(db, now=NOW)

    assert report["summary"]["total"] == 3
    assert report["summary"]["by_activity_type"] == {
        "issue": 1,
        "pull_request": 1,
        "release": 1,
    }
    assert report["summary"]["by_state"] == {
        "closed": 1,
        "open": 1,
        "published": 1,
    }


def test_filters_work_independently_and_together(db):
    _insert_activity(
        db,
        repo_name="alpha",
        activity_type="issue",
        number=1,
        state="open",
        updated_at="2026-04-24T12:00:00+00:00",
    )
    _insert_activity(
        db,
        repo_name="alpha",
        activity_type="pull_request",
        number=2,
        state="closed",
        updated_at="2026-04-20T12:00:00+00:00",
    )
    _insert_activity(
        db,
        repo_name="beta",
        activity_type="pull_request",
        number=3,
        state="open",
        updated_at="2026-04-25T11:00:00+00:00",
    )

    assert [i["activity_id"] for i in uncovered_github_activity_report(db, repo="alpha", now=NOW)["items"]] == [
        "alpha#1:issue",
        "alpha#2:pull_request",
    ]
    assert [
        i["activity_id"]
        for i in uncovered_github_activity_report(db, activity_type="pull_request", now=NOW)["items"]
    ] == ["beta#3:pull_request", "alpha#2:pull_request"]
    assert [i["activity_id"] for i in uncovered_github_activity_report(db, state="open", now=NOW)["items"]] == [
        "beta#3:pull_request",
        "alpha#1:issue",
    ]
    assert [i["activity_id"] for i in uncovered_github_activity_report(db, days=2, now=NOW)["items"]] == [
        "beta#3:pull_request",
        "alpha#1:issue",
    ]
    assert [
        i["activity_id"]
        for i in uncovered_github_activity_report(
            db,
            repo="alpha",
            activity_type="issue",
            state="open",
            days=2,
            now=NOW,
        )["items"]
    ] == ["alpha#1:issue"]


def test_report_rejects_invalid_filters(db):
    with pytest.raises(ValueError, match="activity_type"):
        uncovered_github_activity_report(db, activity_type="discussion")

    with pytest.raises(ValueError, match="days"):
        uncovered_github_activity_report(db, days=0)


def test_json_output_has_summary_and_items_top_level(db, capsys):
    _insert_activity(db, activity_type="release", number=4, state="prerelease")

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("github_activity_coverage.script_context", fake_script_context):
        main(["--json"])

    payload = json.loads(capsys.readouterr().out)
    assert sorted(payload.keys()) == ["items", "summary"]
    assert payload["summary"]["by_activity_type"] == {"release": 1}
    assert payload["items"][0]["activity_id"] == "repo#4:release"


def test_text_output_includes_required_columns(db):
    _insert_activity(db, activity_type="issue", number=7, title="Needs coverage")

    output = format_github_activity_coverage_text(uncovered_github_activity_report(db, now=NOW))

    assert "repo issue #7 [open]" in output
    assert "Needs coverage" in output
    assert "https://github.com/taka/repo/issue/7" in output
    assert "2026-04-24T12:00:00+00:00" in output
