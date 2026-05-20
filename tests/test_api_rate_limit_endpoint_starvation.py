"""Tests for API rate-limit endpoint starvation reports."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.api_rate_limit_endpoint_starvation import (
    build_api_rate_limit_endpoint_starvation_report,
    build_api_rate_limit_endpoint_starvation_report_from_db,
    format_api_rate_limit_endpoint_starvation_json,
    format_api_rate_limit_endpoint_starvation_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "api_rate_limit_endpoint_starvation.py"
spec = importlib.util.spec_from_file_location("api_rate_limit_endpoint_starvation_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE api_rate_limit_snapshots (
            id INTEGER PRIMARY KEY,
            provider TEXT,
            endpoint TEXT,
            remaining INTEGER,
            limit_value INTEGER,
            reset_at TEXT,
            fetched_at TEXT
        )"""
    )
    return conn


def _insert(
    conn: sqlite3.Connection,
    *,
    provider: str = "x",
    endpoint: str = "/tweets",
    remaining: int,
    reset_minutes: int | None = 30,
    fetched_minutes_ago: int,
) -> None:
    conn.execute(
        """INSERT INTO api_rate_limit_snapshots
           (provider, endpoint, remaining, limit_value, reset_at, fetched_at)
           VALUES (?, ?, ?, 100, ?, ?)""",
        (
            provider,
            endpoint,
            remaining,
            (NOW + timedelta(minutes=reset_minutes)).isoformat() if reset_minutes is not None else None,
            (NOW - timedelta(minutes=fetched_minutes_ago)).isoformat(),
        ),
    )


def test_builder_groups_snapshots_and_flags_starvation_windows():
    rows = [
        {"provider": "x", "endpoint": "/tweets", "remaining": 2, "reset_at": (NOW + timedelta(minutes=10)).isoformat(), "fetched_at": (NOW - timedelta(minutes=30)).isoformat()},
        {"provider": "x", "endpoint": "/tweets", "remaining": 0, "reset_at": (NOW + timedelta(minutes=10)).isoformat(), "fetched_at": (NOW - timedelta(minutes=5)).isoformat()},
        {"provider": "github", "endpoint": "/repos", "remaining": 1, "reset_at": (NOW - timedelta(minutes=20)).isoformat(), "fetched_at": (NOW - timedelta(minutes=1)).isoformat()},
        {"provider": "openai", "endpoint": "/models", "remaining": 80, "reset_at": (NOW + timedelta(minutes=20)).isoformat(), "fetched_at": (NOW - timedelta(minutes=1)).isoformat()},
    ]

    report = build_api_rate_limit_endpoint_starvation_report(rows, low_remaining=5, now=NOW)
    payload = json.loads(format_api_rate_limit_endpoint_starvation_json(report))

    assert payload["artifact_type"] == "api_rate_limit_endpoint_starvation"
    assert payload["summary"]["provider_endpoint_count"] == 3
    assert payload["summary"]["issue_count"] == 2
    assert [item["issue_type"] for item in payload["issue_items"]] == [
        "sustained_low_remaining",
        "reset_without_recovery",
    ]
    sustained = payload["issue_items"][0]
    assert sustained["first_seen_at"] == (NOW - timedelta(minutes=30)).isoformat()
    assert sustained["last_seen_at"] == (NOW - timedelta(minutes=5)).isoformat()
    assert sustained["min_remaining"] == 0
    assert sustained["latest_remaining"] == 0
    assert sustained["reset_at"] == (NOW + timedelta(minutes=10)).isoformat()


def test_from_db_cli_and_missing_table(tmp_path, capsys):
    conn = _conn()
    _insert(conn, remaining=2, fetched_minutes_ago=30)
    _insert(conn, remaining=0, fetched_minutes_ago=5)
    _insert(conn, provider="github", endpoint="/repos", remaining=70, fetched_minutes_ago=5)
    conn.commit()

    report = build_api_rate_limit_endpoint_starvation_report_from_db(conn, low_remaining=5, now=NOW)
    assert report["summary"]["issue_count"] == 1
    assert report["issue_items"][0]["provider"] == "x"
    assert "API Rate-limit Endpoint Starvation" in format_api_rate_limit_endpoint_starvation_text(report)

    db_path = tmp_path / "snapshots.sqlite"
    conn.backup(sqlite3.connect(db_path))
    original_builder = script.build_api_rate_limit_endpoint_starvation_report_from_db

    def build_report_with_fixed_now(conn, **kwargs):
        return original_builder(conn, now=NOW, **kwargs)

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            script,
            "build_api_rate_limit_endpoint_starvation_report_from_db",
            build_report_with_fixed_now,
        )
        assert script.main(["--db", str(db_path), "--low-remaining", "5", "--limit", "5"]) == 0
        assert json.loads(capsys.readouterr().out)["summary"]["issue_count"] == 1
        assert script.main(["--db", str(db_path), "--format", "text"]) == 0
        assert "API Rate-limit Endpoint Starvation" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])

    missing = build_api_rate_limit_endpoint_starvation_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["api_rate_limit_snapshots"]
