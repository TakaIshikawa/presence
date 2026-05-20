"""Tests for content claim-check failure triage reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.content_claim_check_failure_triage import (
    build_content_claim_check_failure_triage_report_from_db,
    format_content_claim_check_failure_triage_json,
    format_content_claim_check_failure_triage_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_claim_check_failure_triage.py"
spec = importlib.util.spec_from_file_location("content_claim_check_failure_triage_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE generated_content (
             id INTEGER PRIMARY KEY,
             content_type TEXT,
             published INTEGER,
             published_at TEXT,
             status TEXT
           );
           CREATE TABLE content_claim_checks (
             id INTEGER PRIMARY KEY,
             content_id INTEGER,
             supported_count INTEGER,
             unsupported_count INTEGER,
             annotation_text TEXT,
             created_at TEXT,
             updated_at TEXT
           );"""
    )
    return conn


def _content(conn: sqlite3.Connection, content_id: int, *, published: int = 0, status: str | None = None):
    conn.execute(
        "INSERT INTO generated_content VALUES (?, ?, ?, ?, ?)",
        (
            content_id,
            "x_post",
            published,
            "2026-05-20T09:00:00+00:00" if published else None,
            status,
        ),
    )


def _claim(conn: sqlite3.Connection, content_id: int, *, supported=1, unsupported=0, annotation="ok", updated_at=None):
    checked_at = updated_at or "2026-05-20T10:00:00+00:00"
    conn.execute(
        """INSERT INTO content_claim_checks
           (content_id, supported_count, unsupported_count, annotation_text, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (content_id, supported, unsupported, annotation, checked_at, checked_at),
    )
    conn.commit()


def test_triages_unsupported_missing_annotation_orphan_and_published_unresolved():
    conn = _conn()
    _content(conn, 1, published=1)
    _content(conn, 2)
    _content(conn, 3)
    _claim(conn, 1, supported=2, unsupported=1, annotation="unsupported claim")
    _claim(conn, 2, supported=0, unsupported=2, annotation=None)
    _claim(conn, 3, supported=-1, unsupported=-2, annotation="bad counts")
    _claim(conn, 99, supported=1, unsupported=1, annotation="orphan")

    report = build_content_claim_check_failure_triage_report_from_db(conn, now=NOW)

    issue_types = [finding["issue_type"] for finding in report["findings"]]
    assert issue_types == [
        "published_with_unsupported_claims",
        "unsupported_claims",
        "unsupported_claims",
        "unsupported_claims",
        "negative_claim_count",
        "missing_annotation",
        "orphan_claim_check",
    ]
    assert report["summary"]["checked_row_count"] == 4
    assert report["summary"]["unsupported_row_count"] == 3
    assert report["summary"]["published_unresolved_row_count"] == 1
    assert report["summary"]["by_issue_type"]["orphan_claim_check"] == 1


def test_days_and_limit_filters_are_applied():
    conn = _conn()
    _content(conn, 1)
    _content(conn, 2)
    _claim(conn, 1, unsupported=1, annotation="recent", updated_at=(NOW - timedelta(days=2)).isoformat())
    _claim(conn, 2, unsupported=1, annotation="old", updated_at=(NOW - timedelta(days=40)).isoformat())

    report = build_content_claim_check_failure_triage_report_from_db(conn, days=7, limit=1, now=NOW)

    assert report["summary"]["unsupported_row_count"] == 1
    assert report["summary"]["shown_count"] == 1
    assert report["findings"][0]["content_id"] == "1"


def test_missing_schema_gaps_are_reported():
    empty = sqlite3.connect(":memory:")
    empty.row_factory = sqlite3.Row
    report = build_content_claim_check_failure_triage_report_from_db(empty, now=NOW)

    assert report["missing_tables"] == ["content_claim_checks", "generated_content"]
    assert report["empty_state"] == {"is_empty": True, "reason": "missing_schema"}

    partial = sqlite3.connect(":memory:")
    partial.row_factory = sqlite3.Row
    partial.executescript(
        """CREATE TABLE generated_content (id INTEGER PRIMARY KEY);
           CREATE TABLE content_claim_checks (content_id INTEGER PRIMARY KEY, unsupported_count INTEGER);"""
    )
    report = build_content_claim_check_failure_triage_report_from_db(partial, now=NOW)
    assert report["missing_columns"]["content_claim_checks"] == [
        "annotation_text",
        "created_at",
        "supported_count",
        "updated_at",
    ]
    assert report["missing_columns"]["generated_content"] == [
        "content_type",
        "published",
        "published_at",
        "status",
    ]


def test_formatters_cli_json_and_argument_validation(tmp_path, monkeypatch, capsys):
    conn = _conn()
    _content(conn, 1, published=1)
    _claim(conn, 1, unsupported=1, annotation="unsupported")
    report = build_content_claim_check_failure_triage_report_from_db(conn, now=NOW)
    assert json.loads(format_content_claim_check_failure_triage_json(report))["artifact_type"] == "content_claim_check_failure_triage"
    assert "Content Claim Check Failure Triage" in format_content_claim_check_failure_triage_text(report)

    db_path = tmp_path / "claims.sqlite"
    dest = sqlite3.connect(db_path)
    conn.backup(dest)
    dest.close()
    assert script.main(["--db", str(db_path), "--format", "json", "--days", "7", "--limit", "5"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["published_unresolved_row_count"] == 1
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Content Claim Check Failure Triage" in capsys.readouterr().out

    monkeypatch.setattr(script, "script_context", lambda: _script_context(sqlite3.connect(":memory:")))
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["missing_tables"] == ["content_claim_checks", "generated_content"]
    with pytest.raises(SystemExit):
        script.parse_args(["--days", "0"])
