"""Tests for API rate-limit recovery window reporting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.api_rate_limit_recovery_windows import (
    build_api_rate_limit_recovery_windows_report,
    build_api_rate_limit_recovery_windows_report_from_db,
    format_api_rate_limit_recovery_windows_json,
    format_api_rate_limit_recovery_windows_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "api_rate_limit_recovery_windows.py"
spec = importlib.util.spec_from_file_location("api_rate_limit_recovery_windows_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _ts(minutes_ago: int) -> str:
    return (NOW - timedelta(minutes=minutes_ago)).isoformat()


def _conn(path: Path | str = ":memory:") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE api_rate_limit_snapshots (
            id INTEGER PRIMARY KEY,
            provider TEXT,
            platform TEXT,
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
    provider: str | None = "x",
    platform: str | None = "x",
    endpoint: str = "/tweets",
    remaining: int,
    reset_minutes_ago: int | None = None,
    fetched_minutes_ago: int,
) -> None:
    conn.execute(
        """INSERT INTO api_rate_limit_snapshots
           (provider, platform, endpoint, remaining, limit_value, reset_at, fetched_at)
           VALUES (?, ?, ?, ?, 100, ?, ?)""",
        (
            provider,
            platform,
            endpoint,
            remaining,
            _ts(reset_minutes_ago) if reset_minutes_ago is not None else (NOW + timedelta(minutes=15)).isoformat(),
            _ts(fetched_minutes_ago),
        ),
    )


def test_builder_flags_low_windows_stale_resets_and_metadata():
    rows = [
        {"provider": "x", "platform": "x", "endpoint": "/tweets", "remaining": 3, "reset_at": _ts(120), "fetched_at": _ts(90)},
        {"provider": "x", "platform": "x", "endpoint": "/tweets", "remaining": 2, "reset_at": _ts(120), "fetched_at": _ts(30)},
        {"provider": "x", "platform": "x", "endpoint": "/tweets", "remaining": 40, "reset_at": _ts(120), "fetched_at": _ts(5)},
        {"provider": "github", "platform": "github", "endpoint": "/repos", "remaining": 70, "reset_at": _ts(60), "fetched_at": _ts(5)},
        {"provider": "", "platform": "", "endpoint": "/models", "remaining": 1, "reset_at": _ts(10), "fetched_at": _ts(5)},
    ]

    report = build_api_rate_limit_recovery_windows_report(rows, threshold=5, stale_minutes=30, now=NOW)
    payload = json.loads(format_api_rate_limit_recovery_windows_json(report))

    assert payload["artifact_type"] == "api_rate_limit_recovery_windows"
    assert payload["summary"]["finding_count"] == 3
    assert payload["summary"]["by_issue_type"] == {
        "low_quota_window": 1,
        "missing_provider_platform_metadata": 1,
        "stale_reset_at": 1,
    }
    x_group = next(group for group in payload["findings"] if group["provider"] == "x")
    assert [item["issue_type"] for item in x_group["issues"]] == ["low_quota_window", "stale_reset_at"]
    assert x_group["issues"][0]["severity"] == "critical"
    assert x_group["issues"][0]["duration_minutes"] == 60
    assert x_group["issues"][1]["severity"] == "warning"
    unknown_group = next(group for group in payload["findings"] if group["provider"] == "unknown")
    assert unknown_group["issues"][0]["missing_fields"] == ["provider", "platform"]


def test_db_loader_schema_gaps_empty_state_and_text():
    missing = build_api_rate_limit_recovery_windows_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["api_rate_limit_snapshots"]

    bad = sqlite3.connect(":memory:")
    bad.execute("CREATE TABLE api_rate_limit_snapshots (provider TEXT)")
    gaps = build_api_rate_limit_recovery_windows_report_from_db(bad, now=NOW)
    assert gaps["missing_columns"] == {"api_rate_limit_snapshots": ["endpoint", "fetched_at", "remaining"]}

    conn = _conn()
    _insert(conn, remaining=80, fetched_minutes_ago=5)
    clean = build_api_rate_limit_recovery_windows_report_from_db(conn, now=NOW)
    assert clean["summary"]["finding_count"] == 0
    assert "No API rate-limit recovery window issues found" in format_api_rate_limit_recovery_windows_text(clean)


def test_cli_json_text_and_validation(tmp_path, capsys):
    db_path = tmp_path / "snapshots.sqlite"
    conn = _conn(db_path)
    _insert(conn, remaining=2, reset_minutes_ago=120, fetched_minutes_ago=90)
    _insert(conn, remaining=1, reset_minutes_ago=120, fetched_minutes_ago=30)
    conn.commit()
    conn.close()

    original_builder = script.build_api_rate_limit_recovery_windows_report_from_db

    def build_report_with_fixed_now(conn, **kwargs):
        return original_builder(conn, now=NOW, **kwargs)

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(script, "build_api_rate_limit_recovery_windows_report_from_db", build_report_with_fixed_now)
        assert script.main(["--db", str(db_path), "--format", "json", "--threshold", "5", "--stale-minutes", "30"]) == 0
        assert json.loads(capsys.readouterr().out)["summary"]["finding_count"] == 2
        assert script.main(["--db", str(db_path), "--format", "text"]) == 0
        assert "API Rate-limit Recovery Windows" in capsys.readouterr().out

    with pytest.raises(ValueError, match="threshold must be non-negative"):
        build_api_rate_limit_recovery_windows_report([], threshold=-1)
    with pytest.raises(SystemExit):
        script.parse_args(["--stale-minutes", "0"])
