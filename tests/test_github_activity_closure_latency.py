"""Tests for GitHub activity closure latency reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.github_activity_closure_latency import (
    build_github_activity_closure_latency_report,
    format_github_activity_closure_latency_json,
    format_github_activity_closure_latency_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "github_activity_closure_latency.py"
)
spec = importlib.util.spec_from_file_location(
    "github_activity_closure_latency_script",
    SCRIPT_PATH,
)
github_activity_closure_latency_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(github_activity_closure_latency_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _activity(
    db,
    *,
    repo_name: str = "alpha/app",
    activity_type: str = "issue",
    number: int = 1,
    title: str | None = None,
    state: str = "open",
    updated_at: str = "2026-05-01T10:00:00+00:00",
    created_at: str | None = "2026-04-01T12:00:00+00:00",
    closed_at: str | None = None,
    merged_at: str | None = None,
) -> int:
    return db.upsert_github_activity(
        repo_name=repo_name,
        activity_type=activity_type,
        number=number,
        title=title or f"{activity_type} {number}",
        body="",
        state=state,
        author="taka",
        url=f"https://github.com/{repo_name}/{activity_type}/{number}",
        updated_at=updated_at,
        created_at=created_at,
        closed_at=closed_at,
        merged_at=merged_at,
        labels=[],
        metadata={},
    )


def _metric(report, repo: str, activity_type: str):
    return next(
        item
        for item in report.metrics
        if item.repo == repo and item.activity_type == activity_type
    )


def test_report_separates_open_and_closed_issue_metrics_by_repo(db):
    _activity(
        db,
        repo_name="alpha/app",
        number=1,
        title="Old open issue",
        state="open",
        created_at="2026-04-01T12:00:00+00:00",
    )
    _activity(
        db,
        repo_name="alpha/app",
        number=2,
        state="closed",
        created_at="2026-04-21T12:00:00+00:00",
        closed_at="2026-04-25T12:00:00+00:00",
    )
    _activity(
        db,
        repo_name="alpha/app",
        number=3,
        state="closed",
        created_at="2026-04-11T12:00:00+00:00",
        closed_at="2026-04-21T12:00:00+00:00",
    )
    _activity(
        db,
        repo_name="beta/app",
        number=4,
        state="open",
        created_at="2026-04-26T12:00:00+00:00",
    )

    report = build_github_activity_closure_latency_report(db, days=30, now=NOW)

    alpha = _metric(report, "alpha/app", "issue")
    beta = _metric(report, "beta/app", "issue")
    assert alpha.total_count == 3
    assert alpha.open_count == 1
    assert alpha.closed_count == 2
    assert alpha.open_age_days_median == 30.0
    assert alpha.open_age_days_p90 is None
    assert alpha.close_latency_days_median == 7.0
    assert alpha.close_latency_days_p90 == 9.4
    assert beta.open_count == 1
    assert beta.open_age_days_median == 5.0
    assert report.stale_open_items[0].number == "1"
    assert report.stale_open_items[0].title == "Old open issue"


def test_report_separates_merged_and_open_pull_request_metrics(db):
    _activity(
        db,
        activity_type="pull_request",
        number=10,
        title="Open PR",
        state="open",
        created_at="2026-04-29T12:00:00+00:00",
    )
    _activity(
        db,
        activity_type="pull_request",
        number=11,
        state="closed",
        created_at="2026-04-25T12:00:00+00:00",
        merged_at="2026-04-30T12:00:00+00:00",
    )
    _activity(
        db,
        activity_type="pull_request",
        number=12,
        state="closed",
        created_at="2026-04-20T12:00:00+00:00",
        merged_at="2026-04-30T12:00:00+00:00",
    )

    report = build_github_activity_closure_latency_report(db, days=30, now=NOW)
    metric = _metric(report, "alpha/app", "pull_request")

    assert metric.open_count == 1
    assert metric.closed_count == 0
    assert metric.merged_count == 2
    assert metric.open_age_days_median == 2.0
    assert metric.merge_latency_days_median == 7.5
    assert metric.merge_latency_days_p90 == 9.5
    assert metric.close_latency_days_median is None


def test_repo_and_activity_type_filters_limit_report_scope(db):
    _activity(db, repo_name="alpha/app", number=1)
    _activity(db, repo_name="beta/app", number=2)
    _activity(
        db,
        repo_name="alpha/app",
        activity_type="pull_request",
        number=3,
        state="open",
    )

    report = build_github_activity_closure_latency_report(
        db,
        repo="alpha/app",
        activity_type="issue",
        days=30,
        now=NOW,
    )

    assert [(item.repo, item.activity_type) for item in report.metrics] == [
        ("alpha/app", "issue")
    ]
    assert report.filters["repo"] == "alpha/app"
    assert report.filters["activity_type"] == "issue"


def test_missing_timestamps_do_not_break_counts_or_metrics(db):
    _activity(
        db,
        number=1,
        state="open",
        created_at=None,
    )
    _activity(
        db,
        number=2,
        state="closed",
        created_at=None,
        closed_at="2026-04-30T12:00:00+00:00",
    )

    report = build_github_activity_closure_latency_report(db, days=30, now=NOW)
    metric = _metric(report, "alpha/app", "issue")

    assert metric.total_count == 2
    assert metric.open_count == 1
    assert metric.closed_count == 1
    assert metric.missing_created_at_count == 2
    assert metric.open_age_days_median is None
    assert metric.close_latency_days_median is None
    assert report.stale_open_items[0].age_days is None


def test_text_and_json_rendering_include_metrics_and_representatives(db):
    _activity(
        db,
        activity_type="pull_request",
        number=8,
        title="Long-running PR",
        state="open",
        created_at="2026-04-01T12:00:00+00:00",
    )

    report = build_github_activity_closure_latency_report(db, days=30, now=NOW)
    text = format_github_activity_closure_latency_text(report)
    payload = json.loads(format_github_activity_closure_latency_json(report))

    assert "GitHub Activity Closure Latency" in text
    assert "alpha/app pull_request" in text
    assert "Long-running PR" in text
    assert payload["artifact_type"] == "github_activity_closure_latency"
    assert payload["stale_open_items"][0]["number"] == "8"
    assert payload["metrics"][0]["open_age_days_median"] == 30.0


def test_cli_json_output(db, capsys, monkeypatch):
    _activity(
        db,
        activity_type="pull_request",
        number=9,
        state="closed",
        created_at="2026-04-28T12:00:00+00:00",
        merged_at="2026-05-01T12:00:00+00:00",
    )

    monkeypatch.setattr(
        github_activity_closure_latency_script,
        "script_context",
        lambda: _script_context(db),
    )

    assert github_activity_closure_latency_script.main(
        ["--format", "json", "--activity-type", "pull_request", "--limit", "5"]
    ) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "github_activity_closure_latency"
    assert payload["metrics"][0]["activity_type"] == "pull_request"
    assert payload["metrics"][0]["merged_count"] == 1


def test_cli_text_output(db, capsys, monkeypatch):
    _activity(db, number=5, title="Stale issue")
    monkeypatch.setattr(
        github_activity_closure_latency_script,
        "script_context",
        lambda: _script_context(db),
    )

    assert github_activity_closure_latency_script.main(["--format", "text"]) == 0

    output = capsys.readouterr().out
    assert "GitHub Activity Closure Latency" in output
    assert "Stale issue" in output
