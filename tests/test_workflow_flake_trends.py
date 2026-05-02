"""Tests for GitHub Actions workflow flake trend reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.workflow_flake_trends import (
    build_workflow_flake_trends_report,
    format_workflow_flake_trends_json,
    format_workflow_flake_trends_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "workflow_flake_trends.py"
spec = importlib.util.spec_from_file_location("workflow_flake_trends_script", SCRIPT_PATH)
workflow_flake_trends_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(workflow_flake_trends_script)

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _add_workflow_run(
    db,
    *,
    repo: str = "taka/presence",
    run_id: int = 1001,
    run_number: int | None = None,
    workflow_name: str = "Tests",
    conclusion: str = "failure",
    updated_at: datetime | None = None,
    branch: str = "main",
    run_attempt: int = 1,
    source_activity_id: str | None = None,
) -> int:
    updated_at = updated_at or NOW - timedelta(hours=1)
    run_number = run_number or run_id
    metadata = {
        "workflow_name": workflow_name,
        "run_number": run_number,
        "conclusion": conclusion,
        "branch": branch,
        "head_branch": branch,
        "run_attempt": run_attempt,
        "run_url": f"https://github.com/{repo}/actions/runs/{run_id}",
        "html_url": f"https://github.com/{repo}/actions/runs/{run_id}",
    }
    if source_activity_id:
        metadata["source_activity_id"] = source_activity_id
    return db.upsert_github_activity(
        repo_name=repo,
        activity_type="workflow_run",
        number=run_id,
        title=f"{workflow_name} #{run_number}: {conclusion}",
        state=conclusion,
        author="github-actions",
        url=f"https://github.com/{repo}/actions/runs/{run_id}",
        updated_at=updated_at.isoformat(),
        created_at=(updated_at - timedelta(minutes=5)).isoformat(),
        metadata=metadata,
    )


def test_flags_mixed_success_failure_group_with_counts_and_latest_url(db):
    _add_workflow_run(
        db,
        run_id=1001,
        conclusion="failure",
        updated_at=NOW - timedelta(hours=3),
    )
    _add_workflow_run(
        db,
        run_id=1002,
        conclusion="success",
        updated_at=NOW - timedelta(hours=1),
    )
    _add_workflow_run(db, run_id=1003, workflow_name="Lint", conclusion="success")

    report = build_workflow_flake_trends_report(db, days=7, min_runs=2, now=NOW)

    assert len(report.trends) == 1
    trend = report.trends[0]
    assert trend.repo_name == "taka/presence"
    assert trend.workflow_name == "Tests"
    assert trend.branch == "main"
    assert trend.run_count == 2
    assert trend.failure_count == 1
    assert trend.success_count == 1
    assert trend.latest_url == "https://github.com/taka/presence/actions/runs/1002"
    assert trend.conclusions == ("failure", "success")
    assert trend.run_numbers == (1001, 1002)
    assert "mixed success/failure conclusions" in trend.reasons
    assert "failure followed by success" in trend.reasons
    assert "stabilization fix" in trend.recommended_action


def test_flags_repeated_reruns_and_keeps_source_activity_groups_separate(db):
    _add_workflow_run(
        db,
        run_id=1001,
        run_number=77,
        conclusion="success",
        run_attempt=1,
        source_activity_id="taka/presence#12:pull_request",
        updated_at=NOW - timedelta(hours=3),
    )
    _add_workflow_run(
        db,
        run_id=1002,
        run_number=77,
        conclusion="success",
        run_attempt=2,
        source_activity_id="taka/presence#12:pull_request",
        updated_at=NOW - timedelta(hours=2),
    )
    _add_workflow_run(
        db,
        run_id=1003,
        conclusion="failure",
        source_activity_id="taka/presence#13:pull_request",
        updated_at=NOW - timedelta(hours=1),
    )

    report = build_workflow_flake_trends_report(db, days=7, min_runs=2, now=NOW)

    assert len(report.trends) == 1
    trend = report.trends[0]
    assert trend.source_activity_id == "taka/presence#12:pull_request"
    assert trend.rerun_count == 1
    assert trend.failure_count == 0
    assert trend.success_count == 2
    assert trend.run_numbers == (77,)
    assert trend.reasons == ("repeated reruns",)


def test_reads_workflow_fields_from_columns_when_metadata_is_empty():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE github_activity (
            id INTEGER PRIMARY KEY,
            repo_name TEXT NOT NULL,
            activity_type TEXT NOT NULL,
            number TEXT NOT NULL,
            title TEXT NOT NULL,
            state TEXT,
            url TEXT,
            updated_at TEXT NOT NULL,
            metadata TEXT,
            workflow_name TEXT,
            branch TEXT,
            conclusion TEXT,
            run_attempt INTEGER,
            run_url TEXT,
            source_activity_id TEXT
        );
        """
    )
    rows = [
        (1, "failure", "2026-05-01T08:00:00+00:00", 1),
        (2, "success", "2026-05-01T09:00:00+00:00", 2),
    ]
    for run_id, conclusion, updated_at, attempt in rows:
        conn.execute(
            """INSERT INTO github_activity
               (repo_name, activity_type, number, title, state, url, updated_at,
                metadata, workflow_name, branch, conclusion, run_attempt, run_url,
                source_activity_id)
               VALUES (?, 'workflow_run', ?, 'column run', ?, '', ?, NULL,
                       'Deploy', 'main', ?, ?, ?, 'release#1')""",
            (
                "acme/widget",
                str(run_id),
                conclusion,
                updated_at,
                conclusion,
                attempt,
                f"https://github.com/acme/widget/actions/runs/{run_id}",
            ),
        )
    conn.commit()

    report = build_workflow_flake_trends_report(conn, days=7, min_runs=2, now=NOW)

    assert len(report.trends) == 1
    trend = report.trends[0]
    assert trend.repo_name == "acme/widget"
    assert trend.workflow_name == "Deploy"
    assert trend.branch == "main"
    assert trend.source_activity_id == "release#1"
    assert trend.latest_url.endswith("/2")
    assert trend.rerun_count == 1


def test_empty_or_missing_workflow_rows_return_empty_report(db):
    report = build_workflow_flake_trends_report(db, days=7, min_runs=2, now=NOW)

    assert report.trends == ()
    assert report.missing_tables == ()
    assert "No likely flaky workflow groups found." in format_workflow_flake_trends_text(report)


def test_json_and_text_output_are_deterministic_and_compact(db):
    _add_workflow_run(db, run_id=1001, conclusion="failure", updated_at=NOW - timedelta(hours=2))
    _add_workflow_run(db, run_id=1002, conclusion="success", updated_at=NOW - timedelta(hours=1))

    report = build_workflow_flake_trends_report(db, days=7, min_runs=2, now=NOW)
    payload = json.loads(format_workflow_flake_trends_json(report))
    text = format_workflow_flake_trends_text(report)

    assert payload["artifact_type"] == "workflow_flake_trends"
    assert payload["trend_count"] == 1
    assert payload["trends"][0]["failure_count"] == 1
    assert payload["trends"][0]["success_count"] == 1
    assert "- taka/presence | Tests | main:" in text
    assert "2 runs, 1 failures, 1 successes" in text
    assert "latest=https://github.com/taka/presence/actions/runs/1002" in text


def test_cli_supports_requested_flags(db, monkeypatch, capsys):
    _add_workflow_run(db, repo="acme/widget", run_id=1001, conclusion="failure")
    _add_workflow_run(
        db,
        repo="acme/widget",
        run_id=1002,
        conclusion="success",
        updated_at=NOW - timedelta(minutes=30),
    )
    _add_workflow_run(db, repo="other/repo", run_id=2001, conclusion="failure")
    _add_workflow_run(db, repo="other/repo", run_id=2002, conclusion="success")
    monkeypatch.setattr(
        workflow_flake_trends_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        workflow_flake_trends_script,
        "build_workflow_flake_trends_report",
        lambda db, **kwargs: build_workflow_flake_trends_report(db, now=NOW, **kwargs),
    )

    exit_code = workflow_flake_trends_script.main(
        ["--days", "7", "--min-runs", "2", "--repo", "acme/widget", "--json"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["repo"] == "acme/widget"
    assert payload["trend_count"] == 1
    assert payload["trends"][0]["repo_name"] == "acme/widget"
