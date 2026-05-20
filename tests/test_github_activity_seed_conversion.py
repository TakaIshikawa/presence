"""Tests for GitHub activity seed conversion reporting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.github_activity_seed_conversion import (
    build_github_activity_seed_conversion_report,
    build_github_activity_seed_conversion_report_from_db,
    format_github_activity_seed_conversion_json,
    format_github_activity_seed_conversion_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "github_activity_seed_conversion.py"
spec = importlib.util.spec_from_file_location("github_activity_seed_conversion_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _ts(days_ago: int) -> str:
    return (NOW - timedelta(days=days_ago)).isoformat()


def _conn(path: Path | str = ":memory:") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE github_activity (
            id INTEGER PRIMARY KEY,
            repo_name TEXT,
            activity_type TEXT,
            number TEXT,
            state TEXT,
            updated_at TEXT,
            created_at TEXT
        )"""
    )
    conn.execute(
        """CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            source_activity_ids TEXT,
            published INTEGER,
            published_at TEXT
        )"""
    )
    return conn


def _activity(conn: sqlite3.Connection, activity_id: int, repo: str, activity_type: str, number: str, state: str, days_ago: int) -> None:
    conn.execute(
        "INSERT INTO github_activity VALUES (?, ?, ?, ?, ?, ?, ?)",
        (activity_id, repo, activity_type, number, state, _ts(days_ago), _ts(days_ago)),
    )


def _content(conn: sqlite3.Connection, content_id: int, source_activity_ids: object, published: int = 0) -> None:
    value = source_activity_ids if isinstance(source_activity_ids, str) else json.dumps(source_activity_ids)
    conn.execute("INSERT INTO generated_content VALUES (?, ?, ?, ?)", (content_id, value, published, _ts(1) if published else None))


def test_row_builder_groups_conversion_and_findings():
    activities = [
        {"id": 1, "repo_name": "acme/app", "activity_type": "issue", "number": "1", "state": "open", "updated_at": _ts(2)},
        {"id": 2, "repo_name": "acme/app", "activity_type": "issue", "number": "2", "state": "closed", "updated_at": _ts(20)},
        {"id": 3, "repo_name": "acme/app", "activity_type": "pull_request", "number": "3", "state": "merged", "updated_at": _ts(30)},
    ]
    content = [
        {"id": 10, "source_activity_ids": json.dumps(["1", "acme/app#3:pull_request"]), "published": 1},
        {"id": 11, "source_activity_ids": "not-json", "published": 0},
    ]

    report = build_github_activity_seed_conversion_report(activities, content, stale_days=14, min_conversion_rate=0.75, now=NOW)
    payload = json.loads(format_github_activity_seed_conversion_json(report))

    issue = next(group for group in payload["groups"] if group["activity_type"] == "issue")
    assert issue["total_activities"] == 2
    assert issue["linked_activities"] == 1
    assert issue["published_content_count"] == 1
    assert payload["totals"]["by_issue_type"] == {
        "closed_merged_without_content": 1,
        "invalid_source_activity_ids": 1,
        "low_conversion_group": 1,
        "stale_unconverted_activity": 1,
    }
    assert payload["findings"][0]["issue_type"] == "invalid_source_activity_ids"


def test_db_loader_schema_gaps_empty_state_and_text():
    missing = build_github_activity_seed_conversion_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["generated_content", "github_activity"]

    bad = sqlite3.connect(":memory:")
    bad.execute("CREATE TABLE github_activity (id INTEGER PRIMARY KEY)")
    gaps = build_github_activity_seed_conversion_report_from_db(bad, now=NOW)
    assert gaps["missing_tables"] == ["generated_content"]
    assert gaps["missing_columns"] == {"github_activity": ["activity_type", "number", "repo_name", "updated_at"]}

    clean = _conn()
    _activity(clean, 1, "acme/app", "issue", "1", "open", 1)
    _content(clean, 10, ["1"], published=1)
    report = build_github_activity_seed_conversion_report_from_db(clean, min_conversion_rate=0.5, now=NOW)
    assert report["totals"]["finding_count"] == 0
    assert "GitHub Activity Seed Conversion" in format_github_activity_seed_conversion_text(report)


def test_cli_json_output_and_validation(tmp_path, capsys):
    db_path = tmp_path / "github.sqlite"
    conn = _conn(db_path)
    _activity(conn, 1, "acme/app", "issue", "1", "closed", 20)
    conn.commit()
    conn.close()

    original_builder = script.build_github_activity_seed_conversion_report_from_db

    def build_report_with_fixed_now(conn, **kwargs):
        return original_builder(conn, now=NOW, **kwargs)

    import pytest

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(script, "build_github_activity_seed_conversion_report_from_db", build_report_with_fixed_now)
        assert script.main(["--db", str(db_path), "--format", "json", "--stale-days", "14", "--min-conversion-rate", "0.5"]) == 0
        assert json.loads(capsys.readouterr().out)["totals"]["finding_count"] == 3

    assert script.main(["--db", str(db_path), "--stale-days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
