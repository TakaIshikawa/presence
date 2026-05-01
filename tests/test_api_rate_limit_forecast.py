"""Tests for API rate limit exhaustion forecasting."""

from __future__ import annotations

import json
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from api_rate_limit_forecast import main  # noqa: E402
from evaluation.api_rate_limit_forecast import (  # noqa: E402
    build_api_rate_limit_forecast_report,
    format_api_rate_limit_forecast_text,
)


BASE_TIME = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)


def _snapshot(
    db,
    *,
    provider: str = "x",
    resource: str = "GET /2/tweets",
    remaining: int,
    limit: int | None = 100,
    reset_in_hours: float | None = 1,
    fetched_hours_ago: float,
) -> int:
    return db.insert_api_rate_limit_snapshot(
        provider=provider,
        endpoint=resource,
        remaining=remaining,
        limit=limit,
        reset_at=(
            BASE_TIME + timedelta(hours=reset_in_hours)
            if reset_in_hours is not None
            else None
        ),
        fetched_at=BASE_TIME - timedelta(hours=fetched_hours_ago),
    )


def test_empty_snapshots_report_empty_state_without_division_errors():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_api_rate_limit_forecast_report(conn, now=BASE_TIME)
    text = format_api_rate_limit_forecast_text(report)

    assert report["totals"] == {"resources": 0, "warnings": 0}
    assert report["resources"] == []
    assert report["empty_state"]["schema_present"] is False
    assert "No API rate limit snapshots found" in text


def test_single_snapshot_resource_has_no_consumption_rate_or_projection(db):
    _snapshot(
        db,
        provider="github",
        resource="/user/repos",
        remaining=80,
        fetched_hours_ago=0.25,
    )

    report = build_api_rate_limit_forecast_report(db, now=BASE_TIME)

    resource = report["resources"][0]
    assert resource["provider"] == "github"
    assert resource["resource"] == "/user/repos"
    assert resource["limit"] == 100
    assert resource["remaining"] == 80
    assert resource["remaining_percent"] == 80.0
    assert resource["seconds_until_reset"] == 3600.0
    assert resource["consumption_rate_per_second"] is None
    assert resource["projected_exhaustion_at"] is None
    assert resource["warnings"] == []


def test_forecast_uses_latest_snapshot_and_projects_exhaustion_before_reset(db):
    _snapshot(db, remaining=100, fetched_hours_ago=1.0, reset_in_hours=2)
    _snapshot(db, remaining=40, fetched_hours_ago=0.5, reset_in_hours=2)
    _snapshot(db, remaining=10, fetched_hours_ago=0.0, reset_in_hours=2)

    report = build_api_rate_limit_forecast_report(
        db,
        hours=3,
        remaining_warning_percent=15,
        now=BASE_TIME,
    )

    resource = report["resources"][0]
    assert resource["remaining"] == 10
    assert resource["remaining_percent"] == 10.0
    assert resource["snapshots_used"] == 3
    assert resource["consumption_rate_per_second"] == 0.025
    assert resource["projected_exhaustion_at"] == (
        BASE_TIME + timedelta(seconds=400)
    ).isoformat()
    assert {
        warning["label"] for warning in resource["warnings"]
    } == {"low_remaining", "exhaustion_before_reset"}
    assert {
        warning["label"] for warning in report["warnings"]
    } == {"low_remaining", "exhaustion_before_reset"}


def test_increases_and_old_snapshots_do_not_create_false_burn_rate(db):
    _snapshot(db, remaining=1, fetched_hours_ago=10, reset_in_hours=1)
    _snapshot(db, remaining=90, fetched_hours_ago=1, reset_in_hours=1)
    _snapshot(db, remaining=95, fetched_hours_ago=0, reset_in_hours=1)

    report = build_api_rate_limit_forecast_report(db, hours=2, now=BASE_TIME)

    resource = report["resources"][0]
    assert resource["snapshots_used"] == 2
    assert resource["consumption_rate_per_second"] is None
    assert resource["projected_exhaustion_at"] is None
    assert resource["warnings"] == []


def test_text_output_includes_required_fields_and_warnings(db):
    _snapshot(db, remaining=50, fetched_hours_ago=1, reset_in_hours=2)
    _snapshot(db, remaining=5, fetched_hours_ago=0, reset_in_hours=2)

    report = build_api_rate_limit_forecast_report(
        db,
        remaining_warning_percent=10,
        now=BASE_TIME,
    )
    text = format_api_rate_limit_forecast_text(report)

    assert "API rate limit forecast" in text
    assert "PROVIDER" in text
    assert "RESOURCE" in text
    assert "LIMIT" in text
    assert "REMAIN%" in text
    assert "RESET_AT" in text
    assert "EXHAUST_AT" in text
    assert "low_remaining" in text
    assert "exhaustion_before_reset" in text


def test_cli_supports_json_format_and_threshold_flags(db, capsys):
    _snapshot(db, remaining=80, fetched_hours_ago=0)
    fixed_report = build_api_rate_limit_forecast_report(
        db,
        hours=6,
        remaining_warning_percent=25,
        now=BASE_TIME,
    )

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("api_rate_limit_forecast.script_context", fake_script_context), patch(
        "api_rate_limit_forecast.build_api_rate_limit_forecast_report",
        return_value=fixed_report,
    ):
        result = main(
            [
                "--hours",
                "6",
                "--remaining-warning-percent",
                "25",
                "--format",
                "json",
            ]
        )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["lookback_hours"] == 6
    assert payload["thresholds"]["remaining_warning_percent"] == 25
