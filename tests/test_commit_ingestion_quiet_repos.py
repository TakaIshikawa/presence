"""Tests for commit ingestion quiet repository reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.commit_ingestion_quiet_repos import (
    build_commit_ingestion_quiet_repos_report,
    build_commit_ingestion_quiet_repos_report_from_db,
    format_commit_ingestion_quiet_repos_text,
)


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "commit_ingestion_quiet_repos.py"
spec = importlib.util.spec_from_file_location("commit_ingestion_quiet_repos_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db, repositories=None):
    yield SimpleNamespace(github=SimpleNamespace(repositories=repositories or [])), db


def _commit(repo: str, days_ago: int, sha: str) -> dict:
    return {
        "repo_name": repo,
        "commit_sha": sha,
        "commit_message": f"{repo} change",
        "timestamp": (NOW - timedelta(days=days_ago)).isoformat(),
        "author": "taka",
    }


def test_reports_configured_repo_with_no_historical_commits():
    report = build_commit_ingestion_quiet_repos_report([], repositories=["acme/new"], days=7, now=NOW)

    assert report["rows"] == [
        {
            "repository": "acme/new",
            "configured": True,
            "status": "no_history",
            "ingested_commit_count": 0,
            "historical_commit_count": 0,
            "last_commit_timestamp": None,
            "quiet_days": None,
            "last_commit_sha": None,
            "last_commit_message": None,
            "last_commit_author": None,
        }
    ]


def test_reports_historically_active_repo_with_no_recent_commits():
    report = build_commit_ingestion_quiet_repos_report(
        [_commit("acme/quiet", 20, "oldsha")],
        repositories=[],
        days=7,
        now=NOW,
    )

    row = report["rows"][0]
    assert row["repository"] == "acme/quiet"
    assert row["status"] == "quiet"
    assert row["ingested_commit_count"] == 0
    assert row["historical_commit_count"] == 1
    assert row["quiet_days"] == 20.0
    assert row["last_commit_sha"] == "oldsha"


def test_recently_resumed_repos_are_not_reported_as_quiet():
    report = build_commit_ingestion_quiet_repos_report(
        [
            _commit("acme/resumed", 30, "oldsha"),
            _commit("acme/resumed", 1, "newsha"),
            _commit("acme/active", 1, "activesha"),
        ],
        repositories=["acme/resumed", "acme/active"],
        days=7,
        now=NOW,
    )

    assert report["rows"] == []
    assert report["summary"]["repository_count"] == 2


def test_db_loader_uses_configured_and_historical_repositories(db):
    db.insert_commit("acme/quiet", "quietsha", "old", (NOW - timedelta(days=15)).isoformat(), "taka")
    db.insert_commit("acme/current", "currentsha", "new", (NOW - timedelta(days=1)).isoformat(), "taka")

    report = build_commit_ingestion_quiet_repos_report_from_db(
        db,
        repositories=[{"owner": "acme", "name": "empty"}],
        days=7,
        now=NOW,
    )
    by_repo = {row["repository"]: row for row in report["rows"]}

    assert set(by_repo) == {"acme/empty", "acme/quiet"}
    assert by_repo["acme/empty"]["status"] == "no_history"
    assert by_repo["acme/quiet"]["last_commit_timestamp"] == (NOW - timedelta(days=15)).isoformat()
    assert "Commit Ingestion Quiet Repos" in format_commit_ingestion_quiet_repos_text(report)


def test_cli_supports_json_and_table_output(db, monkeypatch, capsys):
    db.insert_commit("acme/quiet", "quietsha", "old", (NOW - timedelta(days=12)).isoformat(), "taka")
    monkeypatch.setattr(script, "script_context", lambda: _script_context(db, ["acme/empty"]))
    monkeypatch.setattr(
        script,
        "build_commit_ingestion_quiet_repos_report_from_db",
        lambda db, **kwargs: build_commit_ingestion_quiet_repos_report_from_db(db, now=NOW, **kwargs),
    )

    assert script.main(["--days", "7", "--limit", "10", "--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "commit_ingestion_quiet_repos"
    assert {row["repository"] for row in payload["rows"]} == {"acme/empty", "acme/quiet"}

    assert script.main(["--days", "7", "--table"]) == 0
    assert "acme/quiet" in capsys.readouterr().out
