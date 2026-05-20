"""Tests for content claim-check coverage reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
from pathlib import Path
import sqlite3

from evaluation.content_claim_check_coverage import (
    build_content_claim_check_coverage_report,
    build_content_claim_check_coverage_report_from_db,
    format_content_claim_check_coverage_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_claim_check_coverage.py"
spec = importlib.util.spec_from_file_location("content_claim_check_coverage_script", SCRIPT_PATH)
content_claim_check_coverage_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(content_claim_check_coverage_script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            content_type TEXT,
            status TEXT,
            published INTEGER,
            created_at TEXT,
            updated_at TEXT
        );
        CREATE TABLE content_claim_checks (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            supported_count INTEGER,
            unsupported_count INTEGER,
            created_at TEXT,
            updated_at TEXT
        );
        """
    )
    return conn


def test_builder_flags_missing_unsupported_and_stale_with_limit():
    rows = [
        {"content_id": 3, "status": "queued", "claim_check_content_id": None},
        {
            "content_id": 2,
            "status": "published",
            "claim_check_content_id": 2,
            "unsupported_count": 3,
            "content_updated_at": "2026-05-20T10:00:00+00:00",
            "claim_check_updated_at": "2026-05-20T11:00:00+00:00",
        },
        {
            "content_id": 1,
            "status": "published",
            "claim_check_content_id": 1,
            "unsupported_count": 0,
            "content_updated_at": "2026-05-20T10:00:00+00:00",
            "claim_check_updated_at": "2026-05-20T09:00:00+00:00",
        },
        {"content_id": 4, "status": "draft", "claim_check_content_id": None},
    ]

    report = build_content_claim_check_coverage_report(
        rows,
        now=NOW,
        unsupported_threshold=1,
        limit=2,
    )

    assert report["artifact_type"] == "content_claim_check_coverage"
    assert report["summary"]["issue_count"] == 3
    assert [item["issue_type"] for item in report["issue_items"]] == [
        "missing_claim_check",
        "unsupported_claims",
    ]


def test_db_loader_joins_content_and_claim_checks():
    conn = _conn()
    conn.executemany(
        "INSERT INTO generated_content VALUES (?, ?, ?, ?, ?, ?)",
        [
            (1, "x_post", "published", 1, "2026-05-19T00:00:00+00:00", "2026-05-20T10:00:00+00:00"),
            (2, "x_post", "queued", 0, "2026-05-19T00:00:00+00:00", "2026-05-20T09:00:00+00:00"),
            (3, "x_post", "draft", 0, "2026-05-19T00:00:00+00:00", None),
        ],
    )
    conn.executemany(
        "INSERT INTO content_claim_checks VALUES (?, ?, ?, ?, ?, ?)",
        [
            (10, 1, 2, 0, "2026-05-19T01:00:00+00:00", "2026-05-20T08:00:00+00:00"),
            (11, 2, 1, 2, "2026-05-20T10:00:00+00:00", "2026-05-20T10:00:00+00:00"),
        ],
    )

    report = build_content_claim_check_coverage_report_from_db(
        conn,
        now=NOW,
        unsupported_threshold=0,
        limit=10,
    )

    assert [item["issue_type"] for item in report["issue_items"]] == [
        "unsupported_claims",
        "stale_claim_check",
    ]
    assert [item["content_id"] for item in report["issue_items"]] == [2, 1]
    assert "Content Claim Check Coverage" in format_content_claim_check_coverage_text(report)


def test_missing_tables_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    report = build_content_claim_check_coverage_report_from_db(conn, now=NOW)
    assert report["missing_tables"] == ["generated_content", "content_claim_checks"]
    assert report["summary"]["content_rows_scanned"] == 0


def test_cli_outputs_json_and_text(tmp_path, capsys):
    db_path = tmp_path / "db.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE generated_content (id INTEGER PRIMARY KEY, status TEXT);
        CREATE TABLE content_claim_checks (content_id INTEGER, unsupported_count INTEGER);
        INSERT INTO generated_content VALUES (1, 'published');
        """
    )
    conn.close()

    assert content_claim_check_coverage_script.main(["--db", str(db_path), "--format", "json"]) == 0
    assert '"artifact_type": "content_claim_check_coverage"' in capsys.readouterr().out
    assert content_claim_check_coverage_script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Content Claim Check Coverage" in capsys.readouterr().out
