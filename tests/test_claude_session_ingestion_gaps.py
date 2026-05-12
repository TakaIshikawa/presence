"""Tests for Claude session ingestion gaps reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from ingestion.session_ingestion_gaps import (
    build_session_ingestion_gaps_report,
    format_session_ingestion_gaps_json,
)


NOW = datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "claude_session_ingestion_gaps.py"
spec = importlib.util.spec_from_file_location("claude_session_ingestion_gaps_script", SCRIPT_PATH)
claude_session_ingestion_gaps_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_session_ingestion_gaps_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _commit(db, repo: str, days_ago: int, sha: str) -> None:
    db.insert_commit(repo, sha, "commit", (NOW - timedelta(days=days_ago)).isoformat(), "dev")


def _session(db, project: str, days_ago: int, session_id: str) -> None:
    db.insert_claude_message(session_id, f"{session_id}-msg", f"/work/{project}", (NOW - timedelta(days=days_ago)).isoformat(), "prompt")


def test_normal_activity_has_no_gaps(db):
    _commit(db, "acme/widget", 1, "a")
    _session(db, "widget", 1, "s1")

    report = build_session_ingestion_gaps_report(db, now=NOW)

    assert report.gaps == ()


def test_commit_only_gaps_are_reported(db):
    _commit(db, "acme/widget", 1, "a")
    _commit(db, "acme/widget", 1, "b")

    report = build_session_ingestion_gaps_report(db, min_commits=2, now=NOW)

    assert report.gaps[0].gap_reason_code == "commit_activity_without_sessions"
    assert report.gaps[0].commit_count == 2


def test_session_only_days_are_reported(db):
    _session(db, "widget", 1, "s1")

    report = build_session_ingestion_gaps_report(db, now=NOW)

    assert report.gaps[0].gap_reason_code == "session_activity_without_commits"
    assert report.gaps[0].session_count == 1


def test_stable_sorting_and_cli_json(db, monkeypatch, capsys):
    _commit(db, "acme/zeta", 1, "a")
    _commit(db, "acme/alpha", 1, "b")
    monkeypatch.setattr(claude_session_ingestion_gaps_script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        claude_session_ingestion_gaps_script,
        "build_session_ingestion_gaps_report",
        lambda db, **kwargs: build_session_ingestion_gaps_report(db, now=NOW, **kwargs),
    )

    report = build_session_ingestion_gaps_report(db, now=NOW)
    payload = json.loads(format_session_ingestion_gaps_json(report))
    exit_code = claude_session_ingestion_gaps_script.main(["--format", "json", "--min-commits", "1"])
    cli_payload = json.loads(capsys.readouterr().out)

    assert [gap.repo_or_project for gap in report.gaps] == ["alpha", "zeta"]
    assert payload["artifact_type"] == "claude_session_ingestion_gaps"
    assert cli_payload["gap_count"] == 2
    assert exit_code == 0
