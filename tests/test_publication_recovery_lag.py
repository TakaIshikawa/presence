"""Tests for publication recovery lag reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.publication_recovery_lag import (
    build_publication_recovery_lag_report,
    format_publication_recovery_lag_json,
    format_publication_recovery_lag_text,
)


NOW = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_recovery_lag.py"
spec = importlib.util.spec_from_file_location("publication_recovery_lag_script", SCRIPT_PATH)
publication_recovery_lag_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(publication_recovery_lag_script)


@contextmanager
def _script_context(conn: sqlite3.Connection):
    yield SimpleNamespace(), SimpleNamespace(conn=conn)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE publication_attempts (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            platform TEXT,
            attempted_at TEXT,
            success INTEGER
        )"""
    )
    return conn


def _attempt(conn: sqlite3.Connection, content_id: int, platform: str, attempted_at: str, success: int) -> None:
    conn.execute(
        "INSERT INTO publication_attempts (content_id, platform, attempted_at, success) VALUES (?, ?, ?, ?)",
        (content_id, platform, attempted_at, success),
    )
    conn.commit()


def test_report_matches_failures_to_subsequent_successes_and_unrecovered():
    conn = _conn()
    _attempt(conn, 1, "x", "2026-05-18T00:00:00+00:00", 0)
    _attempt(conn, 1, "x", "2026-05-18T06:00:00+00:00", 1)
    _attempt(conn, 2, "bluesky", "2026-05-17T00:00:00+00:00", 0)
    _attempt(conn, 3, "x", "2026-05-17T00:00:00+00:00", 1)

    report = build_publication_recovery_lag_report(conn, now=NOW)

    assert report["summary"] == {
        "recovered_count": 1,
        "unrecovered_count": 1,
        "average_recovery_lag_hours": 6.0,
    }
    recovered = next(row for row in report["rows"] if row["content_id"] == 1)
    assert recovered["lag_hours"] == 6.0
    assert recovered["unrecovered"] is False
    unrecovered = next(row for row in report["rows"] if row["content_id"] == 2)
    assert unrecovered["recovery_at"] is None
    assert unrecovered["unrecovered"] is True


def test_multiple_failure_recovery_windows_are_reported():
    conn = _conn()
    _attempt(conn, 4, "x", "2026-05-16T00:00:00+00:00", 0)
    _attempt(conn, 4, "x", "2026-05-16T02:00:00+00:00", 1)
    _attempt(conn, 4, "x", "2026-05-17T00:00:00+00:00", 0)
    _attempt(conn, 4, "x", "2026-05-17T03:00:00+00:00", 1)

    report = build_publication_recovery_lag_report(conn, now=NOW)

    assert [row["lag_hours"] for row in report["rows"]] == [2.0, 3.0]
    assert report["summary"]["average_recovery_lag_hours"] == 2.5


def test_json_text_and_cli_output(monkeypatch, capsys):
    conn = _conn()
    _attempt(conn, 1, "x", "2026-05-18T00:00:00+00:00", 0)
    monkeypatch.setattr(publication_recovery_lag_script, "script_context", lambda: _script_context(conn))
    monkeypatch.setattr(
        publication_recovery_lag_script,
        "build_publication_recovery_lag_report",
        lambda db, **kwargs: build_publication_recovery_lag_report(db, now=NOW, **kwargs),
    )

    report = build_publication_recovery_lag_report(conn, now=NOW)
    payload = json.loads(format_publication_recovery_lag_json(report))
    text = format_publication_recovery_lag_text(report)
    exit_code = publication_recovery_lag_script.main(["--format", "json"])
    cli_payload = json.loads(capsys.readouterr().out)

    assert payload["artifact_type"] == "publication_recovery_lag"
    assert "Publication Recovery Lag" in text
    assert cli_payload["summary"]["unrecovered_count"] == 1
    assert exit_code == 0
