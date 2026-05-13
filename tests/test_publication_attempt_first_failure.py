"""Tests for publication attempt first failure reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.publication_attempt_first_failure import (
    build_publication_attempt_first_failure_report,
    format_publication_attempt_first_failure_json,
    format_publication_attempt_first_failure_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_attempt_first_failure.py"
spec = importlib.util.spec_from_file_location("publication_attempt_first_failure_script", SCRIPT_PATH)
publication_attempt_first_failure_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(publication_attempt_first_failure_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db) -> int:
    cursor = db.conn.execute(
        "INSERT INTO generated_content (content_type, content) VALUES ('x_post', 'Post')"
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def _attempt(db, content_id: int, platform: str, when: str, success: int, error: str | None = None) -> int:
    cursor = db.conn.execute(
        """INSERT INTO publication_attempts
           (content_id, platform, attempted_at, success, error_category)
           VALUES (?, ?, ?, ?, ?)""",
        (content_id, platform, when, success, error),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def test_identifies_first_failure_per_content_platform_and_recovery(db):
    recovered = _content(db)
    _attempt(db, recovered, "x", "2026-05-10T10:00:00+00:00", 0, "rate_limit")
    _attempt(db, recovered, "x", "2026-05-10T11:00:00+00:00", 0, "rate_limit")
    _attempt(db, recovered, "x", "2026-05-10T12:00:00+00:00", 1)
    unrecovered = _content(db)
    _attempt(db, unrecovered, "bluesky", "2026-05-11T10:00:00+00:00", 0, "network")
    _attempt(db, unrecovered, "bluesky", "2026-05-11T12:00:00+00:00", 0, "network")

    report = build_publication_attempt_first_failure_report(db, now=NOW)

    by_key = {(item["content_id"], item["platform"]): item for item in report["items"]}
    assert by_key[(recovered, "x")]["error_category"] == "rate_limit"
    assert by_key[(recovered, "x")]["attempts_until_recovery"] == 2
    assert by_key[(recovered, "x")]["recovered_at"] == "2026-05-10T12:00:00+00:00"
    assert by_key[(recovered, "x")]["latest_status"] == "succeeded"
    assert by_key[(unrecovered, "bluesky")]["recovery_status"] == "unrecovered"
    assert by_key[(unrecovered, "bluesky")]["attempts_until_recovery"] is None
    assert by_key[(unrecovered, "bluesky")]["latest_status"] == "failed"


def test_lookback_filters_first_failures_and_totals_group_by_platform_error_and_recovery(db):
    old = _content(db)
    _attempt(db, old, "x", "2026-03-01T00:00:00+00:00", 0, "auth")
    recent = _content(db)
    _attempt(db, recent, "x", "2026-05-15T00:00:00+00:00", 0, "auth")
    _attempt(db, recent, "x", "2026-05-15T01:00:00+00:00", 1)
    other = _content(db)
    _attempt(db, other, "bluesky", "2026-05-16T00:00:00+00:00", 0, "media")

    report = build_publication_attempt_first_failure_report(db, lookback_days=30, now=NOW)

    assert report["totals"]["total"] == 2
    assert report["totals"]["by_platform"] == {"bluesky": 1, "x": 1}
    assert report["totals"]["by_error_category"] == {"auth": 1, "media": 1}
    assert report["totals"]["by_recovery_status"] == {"recovered": 1, "unrecovered": 1}
    assert old not in {item["content_id"] for item in report["items"]}


def test_json_text_and_cli(db, monkeypatch, capsys):
    content_id = _content(db)
    _attempt(db, content_id, "x", "2026-05-15T00:00:00+00:00", 0, "auth")
    report = build_publication_attempt_first_failure_report(db, limit=1, now=NOW)
    assert len(report["items"]) == 1
    assert list(json.loads(format_publication_attempt_first_failure_json(report)).keys()) == sorted(report.keys())
    assert "Publication Attempt First Failure" in format_publication_attempt_first_failure_text(report)

    monkeypatch.setattr(publication_attempt_first_failure_script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        publication_attempt_first_failure_script,
        "build_publication_attempt_first_failure_report",
        lambda db, **kwargs: build_publication_attempt_first_failure_report(db, now=NOW, **kwargs),
    )
    assert publication_attempt_first_failure_script.main(["--lookback-days", "30", "--limit", "5", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["filters"]["lookback_days"] == 30
    assert publication_attempt_first_failure_script.main(["--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_missing_schema_and_invalid_args_are_reported():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    assert build_publication_attempt_first_failure_report(conn, now=NOW)["missing_tables"] == ["publication_attempts"]
    with pytest.raises(ValueError, match="lookback_days must be positive"):
        build_publication_attempt_first_failure_report(conn, lookback_days=0, now=NOW)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_publication_attempt_first_failure_report(conn, limit=0, now=NOW)
    conn.close()
