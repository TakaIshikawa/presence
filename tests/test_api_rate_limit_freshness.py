"""Tests for API rate-limit freshness reports."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.api_rate_limit_freshness import (
    build_api_rate_limit_freshness_report,
    format_api_rate_limit_freshness_json,
    format_api_rate_limit_freshness_text,
)


NOW = datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "api_rate_limit_freshness.py"
spec = importlib.util.spec_from_file_location("api_rate_limit_freshness_script", SCRIPT_PATH)
api_rate_limit_freshness_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(api_rate_limit_freshness_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _snapshot(
    db,
    *,
    provider: str = "x",
    endpoint: str = "/tweets",
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


def test_report_uses_latest_snapshot_per_provider_endpoint(db):
    _snapshot(db, provider="x", endpoint="/tweets", remaining=2, fetched_minutes_ago=30)
    latest_id = _snapshot(db, provider="x", endpoint="/tweets", remaining=20, fetched_minutes_ago=5)
    _snapshot(db, provider="github", endpoint="/user/repos", remaining=90)

    report = build_api_rate_limit_freshness_report(db, now=NOW)
    payload = json.loads(format_api_rate_limit_freshness_json(report))

    assert payload["artifact_type"] == "api_rate_limit_freshness"
    assert payload["filters"]["low_remaining"] == 10
    assert payload["totals"]["snapshot_count"] == 2
    assert payload["totals"]["finding_count"] == 0
    by_key = {(row["provider"], row["endpoint"]): row for row in payload["latest_snapshots"]}
    assert by_key[("x", "/tweets")]["remaining"] == 20
    assert by_key[("x", "/tweets")]["finding_labels"] == []

    rows = db.conn.execute(
        """SELECT id FROM api_rate_limit_snapshots
           WHERE provider = 'x' AND endpoint = '/tweets'
           ORDER BY fetched_at DESC, id DESC"""
    ).fetchall()
    assert rows[0]["id"] == latest_id


def test_report_flags_low_stale_overdue_and_missing_reset(db):
    _snapshot(
        db,
        provider="x",
        endpoint="/low",
        remaining=3,
        fetched_minutes_ago=10,
    )
    _snapshot(
        db,
        provider="github",
        endpoint="/stale",
        remaining=80,
        fetched_minutes_ago=120,
    )
    _snapshot(
        db,
        provider="openai",
        endpoint="/overdue",
        remaining=40,
        reset_minutes_from_now=-20,
        fetched_minutes_ago=2,
    )
    _snapshot(
        db,
        provider="anthropic",
        endpoint="/messages",
        remaining=40,
        reset_minutes_from_now=None,
        fetched_minutes_ago=2,
    )

    report = build_api_rate_limit_freshness_report(
        db,
        low_remaining=5,
        stale_after_minutes=60,
        reset_overdue_minutes=10,
        now=NOW,
    )
    labels = [finding["label"] for finding in report.to_dict()["findings"]]
    by_endpoint = {snapshot.endpoint: snapshot for snapshot in report.latest_snapshots}

    assert labels == [
        "low_remaining",
        "stale_snapshot",
        "reset_overdue",
        "missing_reset_at",
    ]
    assert by_endpoint["/low"].finding_labels == ("low_remaining",)
    assert by_endpoint["/stale"].snapshot_age_minutes == 120.0
    assert by_endpoint["/overdue"].minutes_until_reset == -20.0
    assert report.totals == {
        "finding_count": 4,
        "low_remaining_count": 1,
        "missing_reset_at_count": 1,
        "reset_overdue_count": 1,
        "snapshot_count": 4,
        "stale_snapshot_count": 1,
    }


def test_text_formatter_and_missing_schema_are_stable():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    missing = build_api_rate_limit_freshness_report(conn, now=NOW)

    assert missing.missing_tables == ("api_rate_limit_snapshots",)
    text = format_api_rate_limit_freshness_text(missing)
    assert "API Rate-limit Freshness" in text
    assert "Missing tables: api_rate_limit_snapshots" in text
    assert "No API rate-limit snapshots found." in text

    partial = sqlite3.connect(":memory:")
    partial.row_factory = sqlite3.Row
    partial.executescript(
        """
        CREATE TABLE api_rate_limit_snapshots (
            provider TEXT NOT NULL,
            endpoint TEXT NOT NULL,
            remaining INTEGER NOT NULL,
            fetched_at TEXT NOT NULL
        );
        INSERT INTO api_rate_limit_snapshots
            (provider, endpoint, remaining, fetched_at)
        VALUES
            ('x', '/tweets', 1, '2026-05-13T11:55:00+00:00');
        """
    )
    report = build_api_rate_limit_freshness_report(partial, now=NOW)

    assert report.missing_columns["api_rate_limit_snapshots"] == (
        "id",
        "limit_value",
        "reset_at",
    )
    assert report.latest_snapshots[0].finding_labels == (
        "low_remaining",
        "missing_reset_at",
    )


def test_invalid_thresholds_raise_value_error(db):
    with pytest.raises(ValueError, match="low_remaining must be positive"):
        build_api_rate_limit_freshness_report(db, low_remaining=0, now=NOW)
    with pytest.raises(ValueError, match="stale_after_minutes must be positive"):
        build_api_rate_limit_freshness_report(db, stale_after_minutes=0, now=NOW)
    with pytest.raises(ValueError, match="reset_overdue_minutes must be positive"):
        build_api_rate_limit_freshness_report(db, reset_overdue_minutes=0, now=NOW)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_api_rate_limit_freshness_report(db, limit=0, now=NOW)


def test_cli_supports_json_format_and_positive_integer_validation(db, monkeypatch, capsys):
    action_id = _snapshot(db, provider="x", endpoint="/tweets", remaining=1)
    monkeypatch.setattr(
        api_rate_limit_freshness_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        api_rate_limit_freshness_script,
        "build_api_rate_limit_freshness_report",
        lambda db, **kwargs: build_api_rate_limit_freshness_report(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    exit_code = api_rate_limit_freshness_script.main(
        [
            "--format",
            "json",
            "--low-remaining",
            "2",
            "--stale-after-minutes",
            "30",
            "--reset-overdue-minutes",
            "3",
            "--limit",
            "5",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["filters"] == {
        "limit": 5,
        "low_remaining": 2,
        "reset_overdue_minutes": 3,
        "stale_after_minutes": 30,
    }
    assert payload["latest_snapshots"][0]["remaining"] == 1
    assert action_id

    invalid = api_rate_limit_freshness_script.main(["--limit", "0"])
    captured = capsys.readouterr()
    assert invalid == 2
    assert "value must be positive" in captured.err
