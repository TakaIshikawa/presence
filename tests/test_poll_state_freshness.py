"""Tests for poll-state freshness reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from evaluation.poll_state_freshness import (
    build_poll_state_freshness_report,
    format_poll_state_freshness_json,
    format_poll_state_freshness_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "poll_state_freshness.py"
spec = importlib.util.spec_from_file_location("poll_state_freshness_script", SCRIPT_PATH)
poll_state_freshness_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(poll_state_freshness_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE poll_state (
            id INTEGER PRIMARY KEY,
            source TEXT NOT NULL,
            last_success_at TEXT,
            updated_at TEXT,
            last_cursor TEXT
        )"""
    )
    conn.commit()
    return conn


def _ts(hours_ago: float) -> str:
    return (NOW - timedelta(hours=hours_ago)).isoformat()


def _insert(
    conn: sqlite3.Connection,
    source: str,
    *,
    hours_ago: float | None,
    cursor: str = "cursor-1",
    updated_hours_ago: float | None = None,
) -> None:
    conn.execute(
        """INSERT INTO poll_state (source, last_success_at, updated_at, last_cursor)
           VALUES (?, ?, ?, ?)""",
        (
            source,
            _ts(hours_ago) if hours_ago is not None else None,
            _ts(updated_hours_ago) if updated_hours_ago is not None else None,
            cursor,
        ),
    )
    conn.commit()


def test_classifies_threshold_boundaries():
    conn = _conn()
    _insert(conn, "healthy", hours_ago=1.99)
    _insert(conn, "warning", hours_ago=2)
    _insert(conn, "stale", hours_ago=6)

    report = build_poll_state_freshness_report(
        conn,
        warning_hours=2,
        stale_hours=6,
        now=NOW,
    )
    by_source = {row["source"]: row for row in report["rows"]}

    assert by_source["healthy"]["status"] == "healthy"
    assert by_source["warning"]["status"] == "warning"
    assert by_source["stale"]["status"] == "stale"
    assert report["counts"]["by_status"] == {"healthy": 1, "warning": 1, "stale": 1}


def test_missing_metadata_is_stale_with_clear_reason():
    conn = _conn()
    _insert(conn, "missing", hours_ago=None, cursor="")

    report = build_poll_state_freshness_report(conn, warning_hours=2, stale_hours=6, now=NOW)
    row = report["rows"][0]

    assert row["status"] == "stale"
    assert row["age_hours"] is None
    assert row["cursor_summary"] == "none"
    assert row["reason"] == "missing freshness timestamp"


def test_source_filtering():
    conn = _conn()
    _insert(conn, "github", hours_ago=1)
    _insert(conn, "mastodon", hours_ago=9)

    report = build_poll_state_freshness_report(
        conn,
        warning_hours=2,
        stale_hours=6,
        sources=["mastodon"],
        now=NOW,
    )

    assert [row["source"] for row in report["rows"]] == ["mastodon"]
    assert report["rows"][0]["status"] == "stale"
    assert report["filters"]["sources"] == ["mastodon"]


def test_empty_database_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_poll_state_freshness_report(conn, now=NOW)

    assert report["missing_tables"] == ["poll_state"]
    assert report["counts"]["pollers_scanned"] == 0
    assert report["rows"] == []
    assert "No poll_state rows found." in format_poll_state_freshness_text(report)


def test_current_singleton_schema_uses_updated_at_and_last_poll_time_cursor():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE poll_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            last_poll_time TEXT NOT NULL,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )"""
    )
    conn.execute(
        "INSERT INTO poll_state (id, last_poll_time, updated_at) VALUES (1, ?, ?)",
        ("2026-05-01T11:00:00+00:00", _ts(1)),
    )
    conn.commit()

    report = build_poll_state_freshness_report(conn, warning_hours=2, stale_hours=6, now=NOW)
    row = report["rows"][0]

    assert row["source"] == "poll_state"
    assert row["status"] == "healthy"
    assert row["cursor_summary"] == "2026-05-01T11:00:00+00:00"


def test_formatters_are_deterministic_and_concise():
    conn = _conn()
    _insert(conn, "github", hours_ago=3, cursor="abcdef")

    report = build_poll_state_freshness_report(conn, warning_hours=2, stale_hours=6, now=NOW)
    payload = json.loads(format_poll_state_freshness_json(report))
    text = format_poll_state_freshness_text(report)

    assert list(payload) == sorted(payload)
    assert payload["rows"][0]["source"] == "github"
    assert "source=github status=warning age=3h cursor=abcdef" in text
    assert "action=Watch the next scheduled poll and verify the cursor advances." in text


def test_cli_supports_json_output_source_filters_and_validation(db, monkeypatch, capsys):
    db.conn.execute(
        """INSERT OR REPLACE INTO poll_state (id, last_poll_time, updated_at)
           VALUES (1, ?, ?)""",
        (_ts(8), _ts(8)),
    )
    db.conn.commit()
    monkeypatch.setattr(
        poll_state_freshness_script,
        "script_context",
        lambda: _script_context(db),
    )

    result = poll_state_freshness_script.main(
        ["--format", "json", "--warning-hours", "2", "--stale-hours", "6", "--source", "poll_state"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert result == 0
    assert payload["filters"]["sources"] == ["poll_state"]
    assert payload["rows"][0]["status"] == "stale"

    result = poll_state_freshness_script.main(["--warning-hours", "-1"])
    captured = capsys.readouterr()
    assert result == 2
    assert "value must be non-negative" in captured.err
