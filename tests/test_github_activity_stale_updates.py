"""Tests for GitHub activity stale update reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.github_activity_stale_updates import (
    build_github_activity_stale_updates_report,
    format_github_activity_stale_updates_json,
    format_github_activity_stale_updates_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "github_activity_stale_updates.py"
spec = importlib.util.spec_from_file_location("github_activity_stale_updates_script", SCRIPT_PATH)
github_activity_stale_updates_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(github_activity_stale_updates_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _activity(db, *, repo: str, number: str, updated: str | None, labels: str | None, state: str = "open", ingested: str = "2026-05-20T00:00:00+00:00") -> int:
    cursor = db.conn.execute(
        """INSERT INTO github_activity
           (repo_name, activity_type, number, title, state, updated_at, labels, ingested_at)
           VALUES (?, 'issue', ?, 'Title', ?, ?, ?, ?)""",
        (repo, number, state, updated, labels, ingested),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def test_classifies_open_activities_as_fresh_stale_or_missing_updated_at():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE github_activity (
            id INTEGER PRIMARY KEY,
            repo_name TEXT,
            activity_type TEXT,
            number TEXT,
            title TEXT,
            state TEXT,
            updated_at TEXT,
            labels TEXT,
            ingested_at TEXT
        )"""
    )
    conn.execute("INSERT INTO github_activity VALUES (1, 'acme/app', 'issue', '1', 'Fresh', 'open', '2026-05-19T00:00:00+00:00', '[]', '2026-05-20T00:00:00+00:00')")
    conn.execute("INSERT INTO github_activity VALUES (2, 'acme/app', 'issue', '2', 'Stale', 'open', '2026-05-01T00:00:00+00:00', '[\"bug\"]', '2026-05-20T00:00:00+00:00')")
    conn.execute("INSERT INTO github_activity VALUES (3, 'acme/app', 'issue', '3', 'Missing', 'open', NULL, '[]', '2026-05-20T00:00:00+00:00')")
    conn.execute("INSERT INTO github_activity VALUES (4, 'acme/app', 'issue', '4', 'Closed', 'closed', '2026-04-01T00:00:00+00:00', '[]', '2026-05-20T00:00:00+00:00')")

    report = build_github_activity_stale_updates_report(conn, stale_days=14, now=NOW)

    by_id = {item["activity_id"]: item for item in report["items"]}
    assert by_id[1]["freshness_status"] == "fresh"
    assert by_id[2]["freshness_status"] == "stale"
    assert by_id[2]["age_days"] == 19
    assert by_id[3]["freshness_status"] == "missing_updated_at"
    assert 4 not in by_id
    conn.close()


def test_items_and_totals_include_grouping_and_malformed_label_counts(db):
    first = _activity(db, repo="acme/app", number="1", updated="2026-05-01T00:00:00+00:00", labels='["bug", {"name": "help wanted"}]')
    _activity(db, repo="acme/lib", number="2", updated="2026-05-01T00:00:00+00:00", labels="{bad")

    report = build_github_activity_stale_updates_report(db, stale_days=7, now=NOW)

    item = {row["activity_id"]: row for row in report["items"]}[first]
    assert item["repo"] == "acme/app"
    assert item["activity_type"] == "issue"
    assert item["number"] == "1"
    assert item["title"] == "Title"
    assert item["labels"] == ["bug", "help wanted"]
    assert report["totals"]["by_repo_name"] == {"acme/app": 1, "acme/lib": 1}
    assert report["totals"]["by_activity_type"] == {"issue": 2}
    assert report["totals"]["by_state"] == {"open": 2}
    assert report["totals"]["by_label"] == {"bug": 1, "help wanted": 1}
    assert report["totals"]["malformed_labels_count"] == 1


def test_json_text_and_cli(db, monkeypatch, capsys):
    _activity(db, repo="acme/app", number="1", updated="2026-05-01T00:00:00+00:00", labels="[]")
    report = build_github_activity_stale_updates_report(db, limit=1, now=NOW)
    assert len(report["items"]) == 1
    assert list(json.loads(format_github_activity_stale_updates_json(report)).keys()) == sorted(report.keys())
    assert "GitHub Activity Stale Updates" in format_github_activity_stale_updates_text(report)

    monkeypatch.setattr(github_activity_stale_updates_script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        github_activity_stale_updates_script,
        "build_github_activity_stale_updates_report",
        lambda db, **kwargs: build_github_activity_stale_updates_report(db, now=NOW, **kwargs),
    )
    assert github_activity_stale_updates_script.main(["--stale-days", "7", "--limit", "5", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["filters"]["stale_days"] == 7
    assert github_activity_stale_updates_script.main(["--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_missing_schema_and_invalid_args_are_reported():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    assert build_github_activity_stale_updates_report(conn, now=NOW)["missing_tables"] == ["github_activity"]
    with pytest.raises(ValueError, match="stale_days must be positive"):
        build_github_activity_stale_updates_report(conn, stale_days=0, now=NOW)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_github_activity_stale_updates_report(conn, limit=0, now=NOW)
    conn.close()
