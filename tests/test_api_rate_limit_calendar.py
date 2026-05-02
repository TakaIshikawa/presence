"""Tests for API rate-limit reset calendar exports."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.api_rate_limit_calendar import (
    build_api_rate_limit_calendar,
    format_api_rate_limit_calendar_json,
    format_api_rate_limit_calendar_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "export_api_rate_limit_calendar.py"
)
spec = importlib.util.spec_from_file_location(
    "export_api_rate_limit_calendar_script",
    SCRIPT_PATH,
)
export_api_rate_limit_calendar_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(export_api_rate_limit_calendar_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _snapshot(
    db,
    *,
    provider: str = "x",
    endpoint: str = "GET /2/tweets",
    remaining: int = 50,
    limit: int | None = 100,
    reset_minutes_from_now: float | None = 60,
    fetched_minutes_ago: float = 5,
) -> int:
    return db.insert_api_rate_limit_snapshot(
        provider=provider,
        endpoint=endpoint,
        remaining=remaining,
        limit=limit,
        reset_at=(
            NOW + timedelta(minutes=reset_minutes_from_now)
            if reset_minutes_from_now is not None
            else None
        ),
        fetched_at=NOW - timedelta(minutes=fetched_minutes_ago),
    )


def test_calendar_uses_latest_snapshot_per_provider_endpoint_deterministically(db):
    _snapshot(db, provider="x", endpoint="/tweets", remaining=80, fetched_minutes_ago=30)
    low_id = _snapshot(
        db,
        provider="x",
        endpoint="/tweets",
        remaining=25,
        fetched_minutes_ago=10,
    )
    latest_id = _snapshot(
        db,
        provider="x",
        endpoint="/tweets",
        remaining=20,
        fetched_minutes_ago=10,
    )
    _snapshot(db, provider="github", endpoint="/user/repos", remaining=90)

    report = build_api_rate_limit_calendar(db, now=NOW)
    payload = json.loads(format_api_rate_limit_calendar_json(report))

    by_key = {(row.provider, row.endpoint): row for row in report.rows}
    assert len(report.rows) == 2
    assert by_key[("x", "/tweets")].remaining == 20
    assert by_key[("x", "/tweets")].remaining_ratio == 0.2
    assert by_key[("x", "/tweets")].minutes_until_reset == 60.0
    assert by_key[("x", "/tweets")].snapshot_status == "fresh"
    assert by_key[("x", "/tweets")].depletion_status == "available"
    assert payload["artifact_type"] == "api_rate_limit_calendar"
    assert payload["totals"]["row_count"] == 2

    rows = db.conn.execute(
        """SELECT id, remaining FROM api_rate_limit_snapshots
           WHERE provider = 'x' AND endpoint = '/tweets'
           ORDER BY fetched_at DESC, id DESC"""
    ).fetchall()
    assert rows[0]["id"] == latest_id
    assert rows[1]["id"] == low_id


def test_calendar_marks_stale_depleted_and_unknown_reset_rows(db):
    _snapshot(
        db,
        provider="x",
        endpoint="/stale",
        remaining=20,
        fetched_minutes_ago=120,
    )
    _snapshot(
        db,
        provider="github",
        endpoint="/depleted",
        remaining=0,
        reset_minutes_from_now=45,
    )
    _snapshot(
        db,
        provider="anthropic",
        endpoint="/messages",
        remaining=10,
        limit=None,
        reset_minutes_from_now=None,
    )

    report = build_api_rate_limit_calendar(
        db,
        stale_after_minutes=60,
        now=NOW,
    )
    by_endpoint = {row.endpoint: row for row in report.rows}

    assert by_endpoint["/stale"].snapshot_status == "stale"
    assert by_endpoint["/stale"].recommended_action == "refresh_snapshot"
    assert by_endpoint["/stale"].recommended_next_poll_at == NOW.isoformat()
    assert by_endpoint["/depleted"].depletion_status == "depleted"
    assert by_endpoint["/depleted"].recommended_action == "wait_for_reset"
    assert by_endpoint["/depleted"].recommended_next_poll_at == (
        NOW + timedelta(minutes=45)
    ).isoformat()
    assert by_endpoint["/messages"].remaining_ratio is None
    assert by_endpoint["/messages"].depletion_status == "unknown_limit"
    assert by_endpoint["/messages"].recommended_action == "unknown_reset"
    assert by_endpoint["/messages"].recommended_next_poll_at is None
    assert report.totals["stale_count"] == 1
    assert report.totals["unknown_reset_count"] == 1


def test_filters_limit_and_low_remaining_recommendation(db):
    _snapshot(db, provider="x", endpoint="/tweets", remaining=5, limit=100)
    _snapshot(db, provider="x", endpoint="/users", remaining=50, limit=100)
    _snapshot(db, provider="github", endpoint="/user/repos", remaining=90)

    report = build_api_rate_limit_calendar(
        db,
        provider="x",
        endpoint="/tweets",
        limit=1,
        now=NOW,
    )

    assert [(row.provider, row.endpoint) for row in report.rows] == [("x", "/tweets")]
    assert report.rows[0].remaining_ratio == 0.05
    assert report.rows[0].depletion_status == "low"
    assert report.rows[0].recommended_action == "poll_after_reset"
    assert report.filters["limit"] == 1


def test_missing_schema_and_optional_columns_are_tolerated():
    empty = sqlite3.connect(":memory:")
    empty.row_factory = sqlite3.Row
    missing = build_api_rate_limit_calendar(empty, now=NOW)

    assert missing.rows == ()
    assert missing.missing_tables == ("api_rate_limit_snapshots",)
    assert "Missing tables: api_rate_limit_snapshots" in (
        format_api_rate_limit_calendar_text(missing)
    )

    partial = sqlite3.connect(":memory:")
    partial.row_factory = sqlite3.Row
    partial.executescript(
        """
        CREATE TABLE api_rate_limit_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            provider TEXT NOT NULL,
            endpoint TEXT NOT NULL,
            remaining INTEGER NOT NULL,
            fetched_at TEXT NOT NULL
        );
        INSERT INTO api_rate_limit_snapshots
            (provider, endpoint, remaining, fetched_at)
        VALUES
            ('x', '/tweets', 9, '2026-05-02T11:55:00+00:00');
        """
    )

    report = build_api_rate_limit_calendar(partial, now=NOW)

    assert report.missing_columns["api_rate_limit_snapshots"] == (
        "limit_value",
        "reset_at",
    )
    assert report.rows[0].limit is None
    assert report.rows[0].reset_at is None
    assert report.rows[0].recommended_action == "unknown_reset"


def test_invalid_args_raise_value_error(db):
    with pytest.raises(ValueError, match="stale_after_minutes must be positive"):
        build_api_rate_limit_calendar(db, stale_after_minutes=0, now=NOW)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_api_rate_limit_calendar(db, limit=0, now=NOW)


def test_cli_outputs_text_and_json_for_valid_input(db, monkeypatch, capsys):
    _snapshot(db, provider="x", endpoint="/tweets", remaining=25)
    monkeypatch.setattr(
        export_api_rate_limit_calendar_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        export_api_rate_limit_calendar_script,
        "build_api_rate_limit_calendar",
        lambda db, **kwargs: build_api_rate_limit_calendar(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    assert (
        export_api_rate_limit_calendar_script.main(
            [
                "--provider",
                "x",
                "--endpoint",
                "/tweets",
                "--stale-after-minutes",
                "30",
                "--limit",
                "5",
            ]
        )
        == 0
    )
    text = capsys.readouterr().out
    assert "API Rate-limit Reset Calendar" in text
    assert "NEXT_POLL_AT" in text

    assert export_api_rate_limit_calendar_script.main(["--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["totals"]["row_count"] == 1
    assert payload["rows"][0]["provider"] == "x"
