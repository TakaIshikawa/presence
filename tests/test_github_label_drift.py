"""Tests for GitHub label drift reporting."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from evaluation.github_label_drift import (  # noqa: E402
    build_github_label_drift_report,
    format_github_label_drift_json,
    format_github_label_drift_text,
)
from github_label_drift import main  # noqa: E402


NOW = datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc)


def _insert_activity(
    db,
    *,
    repo_name: str = "alpha/app",
    activity_type: str = "issue",
    number: int = 1,
    title: str = "Issue",
    state: str = "open",
    days_ago: float = 1,
    labels: list[str] | str | None = None,
) -> int:
    return db.upsert_github_activity(
        repo_name=repo_name,
        activity_type=activity_type,
        number=number,
        title=title,
        state=state,
        author="taka",
        url=f"https://github.com/{repo_name}/{activity_type}/{number}",
        updated_at=(NOW - timedelta(days=days_ago)).isoformat(),
        created_at=(NOW - timedelta(days=days_ago + 1)).isoformat(),
        labels=labels or [],
    )


def test_report_returns_totals_by_repo_activity_type_and_top_labels(db):
    _insert_activity(db, number=1, labels=["bug", "customer"])
    _insert_activity(db, number=2, labels=["bug"])
    _insert_activity(
        db,
        repo_name="beta/api",
        activity_type="pull_request",
        number=3,
        state="closed",
        labels=["docs"],
    )
    _insert_activity(
        db,
        repo_name="beta/api",
        activity_type="pull_request",
        number=4,
        title="Open without labels",
        state="open",
        labels=[],
    )
    malformed_id = _insert_activity(db, repo_name="beta/api", number=5, labels=["security"])
    db.conn.execute(
        "UPDATE github_activity SET labels = ? WHERE id = ?",
        ("not-json", malformed_id),
    )
    db.conn.commit()

    report = build_github_label_drift_report(db, days=7, compare_days=7, now=NOW)

    assert report["totals"] == {
        "recent_items": 5,
        "comparison_items": 0,
        "label_total": 4,
        "unlabeled_open_items": 1,
        "malformed_label_rows": 1,
    }
    assert report["top_labels"] == [
        {"label": "bug", "count": 2, "share": 0.5},
        {"label": "customer", "count": 1, "share": 0.25},
        {"label": "docs", "count": 1, "share": 0.25},
    ]

    by_repo = {entry["repo"]: entry for entry in report["by_repo"]}
    assert by_repo["alpha/app"]["label_total"] == 3
    assert by_repo["alpha/app"]["concentration"] == {
        "top_label": "bug",
        "top_label_share": 0.6667,
    }
    assert by_repo["beta/api"]["unlabeled_open_items"] == 1
    assert by_repo["beta/api"]["malformed_label_rows"] == 1

    by_type = {entry["activity_type"]: entry for entry in report["by_activity_type"]}
    assert by_type["issue"]["items"] == 3
    assert by_type["issue"]["malformed_label_rows"] == 1
    assert by_type["pull_request"]["top_labels"] == [
        {"label": "docs", "count": 1, "share": 1.0}
    ]

    assert report["malformed_items"][0]["reason"] == "invalid_json"
    assert report["unlabeled_open_items"][0]["title"] == "Open without labels"


def test_report_identifies_labels_absent_from_recent_window(db):
    _insert_activity(db, number=1, labels=["bug"], days_ago=1)
    _insert_activity(db, number=2, labels=["security"], days_ago=8)
    _insert_activity(db, number=3, labels=["security", "docs"], days_ago=9)
    _insert_activity(db, number=4, labels=["outside"], days_ago=20)

    report = build_github_label_drift_report(db, days=7, compare_days=7, now=NOW)

    assert report["disappeared_labels"] == [
        {"label": "docs", "comparison_count": 1},
        {"label": "security", "comparison_count": 2},
    ]
    assert report["newly_dominant_labels"][0] == {
        "label": "bug",
        "recent_count": 1,
        "comparison_count": 0,
        "delta": 1,
        "recent_share": 1.0,
        "comparison_share": 0.0,
    }


def test_repo_filter_and_activity_scope(db):
    _insert_activity(db, repo_name="alpha/app", number=1, labels=["bug"])
    _insert_activity(db, repo_name="beta/api", number=2, labels=["docs"])
    _insert_activity(db, repo_name="alpha/app", activity_type="release", number=3, labels=["ignored"])

    report = build_github_label_drift_report(
        db,
        days=7,
        compare_days=7,
        repo="alpha/app",
        now=NOW,
    )

    assert report["totals"]["recent_items"] == 1
    assert report["top_labels"] == [{"label": "bug", "count": 1, "share": 1.0}]
    assert [entry["repo"] for entry in report["by_repo"]] == ["alpha/app"]


def test_formatters_are_deterministic_for_fixed_input(db):
    _insert_activity(db, repo_name="alpha/app", number=1, labels=["bug", "docs"])
    _insert_activity(db, repo_name="alpha/app", number=2, labels=[])

    report = build_github_label_drift_report(db, days=7, compare_days=7, now=NOW)

    assert format_github_label_drift_json(report) == format_github_label_drift_json(report)
    assert format_github_label_drift_text(report) == format_github_label_drift_text(report)
    payload = json.loads(format_github_label_drift_json(report))
    assert payload["artifact_type"] == "github_label_drift"
    text = format_github_label_drift_text(report)
    assert "GitHub label drift report" in text
    assert "alpha/app: items=2 labels=2 unlabeled_open=1" in text


def test_cli_supports_days_compare_days_repo_and_json(db, capsys):
    _insert_activity(db, repo_name="alpha/app", number=1, labels=["bug"])
    fixed_report = build_github_label_drift_report(
        db,
        days=3,
        compare_days=4,
        repo="alpha/app",
        now=NOW,
    )

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("github_label_drift.script_context", fake_script_context), patch(
        "github_label_drift.build_github_label_drift_report",
        return_value=fixed_report,
    ) as build_report:
        result = main(
            [
                "--days",
                "3",
                "--compare-days",
                "4",
                "--repo",
                "alpha/app",
                "--json",
            ]
        )

    assert result == 0
    build_report.assert_called_once()
    assert build_report.call_args.kwargs["days"] == 3
    assert build_report.call_args.kwargs["compare_days"] == 4
    assert build_report.call_args.kwargs["repo"] == "alpha/app"
    payload = json.loads(capsys.readouterr().out)
    assert payload["days"] == 3
    assert payload["compare_days"] == 4
    assert payload["filters"]["repo"] == "alpha/app"
