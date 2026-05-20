"""Tests for publication attempt platform identity conflict reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.publication_attempt_platform_identity_conflicts import (
    build_publication_attempt_platform_identity_conflicts_report,
    build_publication_attempt_platform_identity_conflicts_report_from_db,
    format_publication_attempt_platform_identity_conflicts_json,
    format_publication_attempt_platform_identity_conflicts_text,
)


NOW = datetime(2026, 5, 20, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_attempt_platform_identity_conflicts.py"
spec = importlib.util.spec_from_file_location("publication_attempt_platform_identity_conflicts_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn(path: Path | str = ":memory:") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE publication_attempts (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            platform TEXT,
            attempted_at TEXT,
            success INTEGER,
            platform_post_id TEXT,
            platform_url TEXT,
            response_metadata TEXT
        );
        """
    )
    return conn


def test_builder_flags_duplicate_conflicting_and_metadata_mismatch_reasons():
    report = build_publication_attempt_platform_identity_conflicts_report(
        [
            {"attempt_id": 1, "content_id": 10, "platform": "x", "attempted_at": "2026-05-20T09:00:00+00:00", "success": 1, "platform_post_id": "p1", "platform_url": "https://x/p1"},
            {"attempt_id": 2, "content_id": 11, "platform": "x", "attempted_at": "2026-05-20T10:00:00+00:00", "success": 1, "platform_post_id": "p1", "platform_url": "https://x/p1"},
            {"attempt_id": 3, "content_id": 12, "platform": "x", "attempted_at": "2026-05-20T10:30:00+00:00", "status": "success", "platform_post_id": "p2", "platform_url": "https://x/p2"},
            {"attempt_id": 4, "content_id": 12, "platform": "x", "attempted_at": "2026-05-20T10:31:00+00:00", "status": "published", "platform_post_id": "p3", "platform_url": "https://x/p3"},
            {"attempt_id": 5, "content_id": 13, "platform": "bluesky", "attempted_at": "2026-05-20T11:00:00+00:00", "success": 1, "platform_post_id": "at://post/right", "platform_url": "https://bsky.app/profile/a/post/right", "response_metadata": {"post_id": "at://post/wrong", "url": "https://bsky.app/profile/a/post/wrong"}},
            {"attempt_id": 6, "content_id": 14, "platform": "x", "attempted_at": "2026-04-01T11:00:00+00:00", "success": 1, "platform_post_id": "old"},
        ],
        now=NOW,
        lookback_days=7,
    )
    payload = json.loads(format_publication_attempt_platform_identity_conflicts_json(report))

    assert payload["artifact_type"] == "publication_attempt_platform_identity_conflicts"
    assert payload["totals"]["attempt_count"] == 5
    assert payload["totals"]["by_reason"] == {
        "conflicting_success_identity": 1,
        "duplicate_platform_post_id": 1,
        "metadata_identity_mismatch": 1,
    }
    assert [group["reason"] for group in payload["findings"]] == [
        "duplicate_platform_post_id",
        "conflicting_success_identity",
        "metadata_identity_mismatch",
    ]
    assert "reason | content_id | platform" in format_publication_attempt_platform_identity_conflicts_text(report)


def test_from_db_schema_gaps_empty_state_and_limit_ordering():
    missing = build_publication_attempt_platform_identity_conflicts_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["publication_attempts"]

    bad = sqlite3.connect(":memory:")
    bad.execute("CREATE TABLE publication_attempts (content_id INTEGER, platform TEXT)")
    gaps = build_publication_attempt_platform_identity_conflicts_report_from_db(bad, now=NOW)
    assert gaps["missing_columns"]["publication_attempts"] == [
        "attempted_at",
        "created_at",
        "id",
        "platform_post_id",
        "platform_url",
        "post_id",
        "published_at",
        "response_metadata",
        "success/status",
        "url",
    ]

    conn = _conn()
    conn.execute("INSERT INTO publication_attempts VALUES (1, 1, 'x', '2026-05-20T10:00:00+00:00', 1, 'p1', 'u1', NULL)")
    conn.execute("INSERT INTO publication_attempts VALUES (2, 2, 'x', '2026-05-20T10:01:00+00:00', 1, 'p1', 'u1', NULL)")
    conn.execute("INSERT INTO publication_attempts VALUES (3, 3, 'x', '2026-05-20T10:02:00+00:00', 1, 'p2', 'u2', NULL)")
    conn.execute("INSERT INTO publication_attempts VALUES (4, 3, 'x', '2026-05-20T10:03:00+00:00', 1, 'p3', 'u2', NULL)")
    report = build_publication_attempt_platform_identity_conflicts_report_from_db(conn, now=NOW, limit=1)
    flat = [item for group in report["findings"] for item in group["items"]]
    assert [item["reason"] for item in flat] == ["duplicate_platform_post_id"]
    assert report["totals"]["finding_count"] == 2
    assert report["totals"]["shown_count"] == 1

    clean = _conn()
    clean.execute("INSERT INTO publication_attempts VALUES (1, 1, 'x', '2026-05-20T10:00:00+00:00', 1, 'p1', 'u1', NULL)")
    clean_report = build_publication_attempt_platform_identity_conflicts_report_from_db(clean, now=NOW)
    assert clean_report["empty_state"]["is_empty"] is True
    assert "No publication attempt platform identity conflicts found" in format_publication_attempt_platform_identity_conflicts_text(clean_report)


def test_cli_db_json_text_and_validation(tmp_path, capsys):
    db_path = tmp_path / "attempts.sqlite"
    conn = _conn(db_path)
    conn.execute("INSERT INTO publication_attempts VALUES (1, 1, 'x', '2026-05-20T10:00:00+00:00', 1, 'p1', 'u1', NULL)")
    conn.execute("INSERT INTO publication_attempts VALUES (2, 2, 'x', '2026-05-20T10:01:00+00:00', 1, 'p1', 'u1', NULL)")
    conn.commit()
    conn.close()

    assert script.main(["--db", str(db_path), "--format", "json", "--lookback-days", "30", "--limit", "5"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "publication_attempt_platform_identity_conflicts"
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Publication Attempt Platform Identity Conflicts" in capsys.readouterr().out
    assert script.main(["--lookback-days", "0"]) == 2
    with pytest.raises(ValueError, match="limit must be positive"):
        build_publication_attempt_platform_identity_conflicts_report([], limit=0)
