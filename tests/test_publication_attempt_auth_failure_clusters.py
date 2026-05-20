"""Tests for publication attempt auth failure cluster reporting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.publication_attempt_auth_failure_clusters import (
    build_publication_attempt_auth_failure_clusters_report,
    build_publication_attempt_auth_failure_clusters_report_from_db,
    format_publication_attempt_auth_failure_clusters_json,
    format_publication_attempt_auth_failure_clusters_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_attempt_auth_failure_clusters.py"
spec = importlib.util.spec_from_file_location("publication_attempt_auth_failure_clusters_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _ts(hours_ago: int) -> str:
    return (NOW - timedelta(hours=hours_ago)).isoformat()


def _conn(path: Path | str = ":memory:") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE publication_attempts (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            platform TEXT,
            attempted_at TEXT,
            success INTEGER,
            status TEXT,
            error_code TEXT,
            error_message TEXT
        )"""
    )
    return conn


def test_builder_groups_auth_failures_and_excludes_non_auth():
    rows = [
        {"id": 1, "content_id": 10, "platform": "x", "attempted_at": _ts(2), "status": "failed", "error_code": "401", "error_message": "Unauthorized token abc123456789012345 for post 100"},
        {"id": 2, "content_id": 11, "platform": "x", "attempted_at": _ts(1), "status": "failed", "error_code": "401", "error_message": "Unauthorized token differenttoken12345 for post 200"},
        {"id": 3, "content_id": 12, "platform": "x", "attempted_at": _ts(1), "status": "failed", "error_code": "500", "error_message": "HTTP 500 upstream"},
        {"id": 4, "content_id": 13, "platform": "x", "attempted_at": _ts(1), "status": "success", "error_code": "401", "error_message": "Unauthorized token ignored"},
        {"id": 5, "content_id": 14, "platform": "bluesky", "attempted_at": _ts(30), "status": "failed", "error_code": "403", "error_message": "Forbidden credential"},
    ]

    report = build_publication_attempt_auth_failure_clusters_report(rows, min_attempts=2, lookback_hours=24, now=NOW)
    payload = json.loads(format_publication_attempt_auth_failure_clusters_json(report))

    assert payload["artifact_type"] == "publication_attempt_auth_failure_clusters"
    assert payload["summary"]["attempt_count"] == 4
    assert payload["summary"]["auth_failure_attempt_count"] == 2
    assert payload["summary"]["by_platform"] == {"x": 2}
    assert len(payload["findings"]) == 1
    finding = payload["findings"][0]
    assert finding["platform"] == "x"
    assert finding["attempt_count"] == 2
    assert finding["distinct_content_count"] == 2
    assert finding["first_attempted_at"] == _ts(2)
    assert finding["last_attempted_at"] == _ts(1)
    assert finding["auth_signature"] == "unauthorized token <token> for post <id>"


def test_db_loader_schema_gaps_and_optional_error_columns():
    missing = build_publication_attempt_auth_failure_clusters_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["publication_attempts"]

    bad = sqlite3.connect(":memory:")
    bad.execute("CREATE TABLE publication_attempts (id INTEGER PRIMARY KEY, platform TEXT)")
    gaps = build_publication_attempt_auth_failure_clusters_report_from_db(bad, now=NOW)
    assert gaps["missing_columns"] == {
        "publication_attempts": ["attempted_at", "status|error_code|error_message|error|error_category|last_error|message"]
    }

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE publication_attempts (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            platform TEXT,
            attempted_at TEXT,
            error_category TEXT,
            error TEXT
        )"""
    )
    conn.execute("INSERT INTO publication_attempts VALUES (1, 1, 'bluesky', ?, 'auth', 'Invalid token 123')", (_ts(2),))
    conn.execute("INSERT INTO publication_attempts VALUES (2, 2, 'bluesky', ?, 'auth', 'Invalid token 456')", (_ts(1),))
    report = build_publication_attempt_auth_failure_clusters_report_from_db(conn, now=NOW)
    assert report["findings"][0]["platform"] == "bluesky"
    assert report["findings"][0]["auth_signature"] == "invalid token <id>"


def test_empty_state_formatters_cli_and_validation(tmp_path, capsys):
    report = build_publication_attempt_auth_failure_clusters_report([], now=NOW)
    assert "No repeated publication attempt auth failure clusters found" in format_publication_attempt_auth_failure_clusters_text(report)

    db_path = tmp_path / "attempts.sqlite"
    conn = _conn(db_path)
    conn.execute("INSERT INTO publication_attempts VALUES (1, 10, 'x', ?, 0, 'failed', '401', 'Unauthorized token abc123456789012345')", (_ts(1),))
    conn.execute("INSERT INTO publication_attempts VALUES (2, 11, 'x', ?, 0, 'failed', '401', 'Unauthorized token def123456789012345')", (_ts(1),))
    conn.commit()
    conn.close()

    original_builder = script.build_publication_attempt_auth_failure_clusters_report_from_db

    def build_report_with_fixed_now(conn, **kwargs):
        return original_builder(conn, now=NOW, **kwargs)

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(script, "build_publication_attempt_auth_failure_clusters_report_from_db", build_report_with_fixed_now)
        assert script.main(["--db", str(db_path), "--format", "json", "--min-attempts", "2", "--lookback-hours", "24"]) == 0
        assert json.loads(capsys.readouterr().out)["summary"]["finding_count"] == 1
        assert script.main(["--db", str(db_path), "--format", "text"]) == 0
        assert "Publication Attempt Auth Failure Clusters" in capsys.readouterr().out

    with pytest.raises(ValueError, match="min_attempts must be positive"):
        build_publication_attempt_auth_failure_clusters_report([], min_attempts=0)
    with pytest.raises(SystemExit):
        script.parse_args(["--lookback-hours", "0"])
