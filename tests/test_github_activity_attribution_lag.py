"""Tests for GitHub activity attribution lag reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.github_activity_attribution_lag import (
    build_github_activity_attribution_lag_report,
    format_github_activity_attribution_lag_json,
    format_github_activity_attribution_lag_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "github_activity_attribution_lag.py"
spec = importlib.util.spec_from_file_location("github_activity_attribution_lag_script", SCRIPT_PATH)
github_activity_attribution_lag_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(github_activity_attribution_lag_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _activity(db, *, repo="acme/app", number="1", activity_type="issue", labels=None, updated_at=None) -> int:
    return db.upsert_github_activity(
        repo_name=repo,
        activity_type=activity_type,
        number=number,
        title=f"{activity_type} {number}",
        body="body",
        state="open",
        author="dev",
        url=f"https://github.com/{repo}/issues/{number}",
        updated_at=(updated_at or NOW).isoformat(),
        labels=labels or [],
    )


def _content(db, source_activity_ids) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="copy",
        eval_score=7,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET source_activity_ids = ? WHERE id = ?",
        (json.dumps(source_activity_ids), content_id),
    )
    db.conn.commit()
    return content_id


def test_counts_used_and_unused_activity_with_labels_and_groups(db):
    used_numeric = _activity(db, number="10", labels=["bug"], updated_at=NOW - timedelta(days=2))
    used_logical = _activity(db, number="11", activity_type="pull_request", labels=["enhancement"], updated_at=NOW)
    unused_old = _activity(db, number="12", labels=["bug", "urgent"], updated_at=NOW - timedelta(days=12))
    unused_new = _activity(db, repo="acme/api", number="2", labels=[], updated_at=NOW - timedelta(days=1))
    _content(db, [used_numeric, "acme/app#11:pull_request"])

    report = build_github_activity_attribution_lag_report(db, days=30, now=NOW)
    payload = json.loads(format_github_activity_attribution_lag_json(report))
    text = format_github_activity_attribution_lag_text(report)

    assert payload["totals"]["used_count"] == 2
    assert payload["totals"]["unused_count"] == 2
    assert payload["label_counts"] == {"bug": 2, "enhancement": 1, "unlabeled": 1, "urgent": 1}
    assert payload["activity_type_counts"] == {"issue": 3, "pull_request": 1}
    assert [item["id"] for item in payload["oldest_unused"]] == [unused_old, unused_new]
    assert any(group["label"] == "bug" and group["unused_count"] == 1 for group in payload["groups"])
    assert f"id={unused_old}" in text


def test_malformed_source_activity_arrays_are_safe(db):
    _activity(db, number="1", labels=["bug"])
    content_id = _content(db, [])
    db.conn.execute("UPDATE generated_content SET source_activity_ids = ? WHERE id = ?", ("not-json", content_id))
    db.conn.commit()

    report = build_github_activity_attribution_lag_report(db, now=NOW)

    assert report["totals"]["malformed_source_activity_rows"] == 1
    assert report["totals"]["unused_count"] == 1


def test_malformed_labels_are_reported_as_label_bucket(db):
    activity_id = _activity(db, labels=["bug"])
    db.conn.execute("UPDATE github_activity SET labels = ? WHERE id = ?", ("not-json", activity_id))
    db.conn.commit()

    report = build_github_activity_attribution_lag_report(db, now=NOW)

    assert report["totals"]["malformed_label_rows"] == 1
    assert report["label_counts"] == {"malformed": 1}
    assert report["groups"][0]["label"] == "malformed"


def test_partial_schema_without_generated_content_reports_unused():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """CREATE TABLE github_activity (
            id INTEGER PRIMARY KEY,
            repo_name TEXT,
            activity_type TEXT,
            number TEXT,
            url TEXT,
            labels TEXT,
            updated_at TEXT
        )"""
    )
    conn.execute(
        "INSERT INTO github_activity VALUES (?, ?, ?, ?, ?, ?, ?)",
        (1, "acme/app", "issue", "1", "https://example.test", '["bug"]', NOW.isoformat()),
    )
    conn.commit()
    try:
        report = build_github_activity_attribution_lag_report(conn, now=NOW)
    finally:
        conn.close()

    assert report["missing_tables"] == ["generated_content"]
    assert report["totals"]["unused_count"] == 1


def test_cli_supports_json_output(db, monkeypatch, capsys):
    activity_id = _activity(db)
    monkeypatch.setattr(github_activity_attribution_lag_script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        github_activity_attribution_lag_script,
        "build_github_activity_attribution_lag_report",
        lambda db, **kwargs: build_github_activity_attribution_lag_report(db, now=NOW, **kwargs),
    )

    exit_code = github_activity_attribution_lag_script.main(["--format", "json"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["oldest_unused"][0]["id"] == activity_id


def test_cli_returns_nonzero_on_database_error(monkeypatch, capsys):
    monkeypatch.setattr(github_activity_attribution_lag_script, "script_context", lambda: _script_context(SimpleNamespace()))
    monkeypatch.setattr(
        github_activity_attribution_lag_script,
        "build_github_activity_attribution_lag_report",
        lambda *args, **kwargs: (_ for _ in ()).throw(sqlite3.Error("db failed")),
    )

    assert github_activity_attribution_lag_script.main([]) == 1
    assert "error: db failed" in capsys.readouterr().err
