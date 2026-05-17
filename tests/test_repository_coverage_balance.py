"""Tests for repository coverage balance reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.repository_coverage_balance import (
    build_repository_coverage_balance_report,
    format_repository_coverage_balance_json,
    format_repository_coverage_balance_table,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "repository_coverage_balance.py"
spec = importlib.util.spec_from_file_location("repository_coverage_balance_script", SCRIPT_PATH)
repository_coverage_balance_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(repository_coverage_balance_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _commit(db, repo: str, sha: str, hours_ago: int = 12) -> None:
    db.insert_commit(repo, sha, f"feat: {sha}", (NOW - timedelta(hours=hours_ago)).isoformat(), "dev@example.com")


def _post(db, commits: list[str], hours_ago: int = 6) -> int:
    content_id = db.insert_generated_content("blog_post", commits, [], "post", 8.0, "ok")
    db.conn.execute(
        "UPDATE generated_content SET published = 1, published_at = ? WHERE id = ?",
        ((NOW - timedelta(hours=hours_ago)).isoformat(), content_id),
    )
    db.conn.commit()
    return content_id


def test_compares_commit_share_to_post_share_and_includes_uncovered_repo(db):
    _commit(db, "alpha/app", "a1")
    _commit(db, "alpha/app", "a2")
    _commit(db, "beta/api", "b1")
    _post(db, ["a1"])
    _post(db, ["a2"])

    report = build_repository_coverage_balance_report(db, lookback_days=7, delta_threshold=0.2, now=NOW)
    rows = {row.repository: row.to_dict() for row in report.rows}

    assert rows["alpha/app"]["commit_count"] == 2
    assert rows["alpha/app"]["attributed_post_count"] == 2
    assert rows["alpha/app"]["coverage_status"] == "over_covered"
    assert rows["beta/api"]["commit_count"] == 1
    assert rows["beta/api"]["attributed_post_count"] == 0
    assert rows["beta/api"]["coverage_status"] == "under_covered"


def test_threshold_json_table_and_cli(db, monkeypatch, capsys):
    _commit(db, "alpha/app", "a1")
    _post(db, ["a1"])
    report = build_repository_coverage_balance_report(db, lookback_days=7, delta_threshold=0.0, now=NOW)
    payload = json.loads(format_repository_coverage_balance_json(report))

    assert payload["artifact_type"] == "repository_coverage_balance"
    assert payload["rows"][0]["coverage_status"] == "balanced"
    assert "Repository Coverage Balance" in format_repository_coverage_balance_table(report)

    monkeypatch.setattr(repository_coverage_balance_script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        repository_coverage_balance_script,
        "build_repository_coverage_balance_report",
        lambda db, **kwargs: build_repository_coverage_balance_report(db, now=NOW, **kwargs),
    )
    assert repository_coverage_balance_script.main(["--format", "table", "--delta-threshold", "0.1"]) == 0
    assert "repository | commit_count" in capsys.readouterr().out
