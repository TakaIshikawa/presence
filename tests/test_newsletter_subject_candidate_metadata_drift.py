"""Tests for newsletter subject candidate metadata drift reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.newsletter_subject_candidate_metadata_drift import (
    build_newsletter_subject_candidate_metadata_drift_report_from_db,
    format_newsletter_subject_candidate_metadata_drift_json,
    format_newsletter_subject_candidate_metadata_drift_text,
)


NOW = datetime(2026, 5, 20, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_subject_candidate_metadata_drift.py"
spec = importlib.util.spec_from_file_location("newsletter_subject_candidate_metadata_drift_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE newsletter_subject_candidates (
            id INTEGER PRIMARY KEY,
            newsletter_send_id INTEGER,
            issue_id TEXT,
            subject TEXT,
            source TEXT,
            rank INTEGER,
            selected INTEGER,
            rationale TEXT,
            source_content_ids TEXT,
            metadata TEXT,
            created_at TEXT
        )"""
    )
    return conn


def _candidate(
    conn: sqlite3.Connection,
    candidate_id: int,
    *,
    source: str = "llm",
    rank: int | None = 1,
    selected: int = 0,
    rationale: str | None = "reason",
    source_content_ids: object = ("1", "2"),
    metadata: object = None,
    created_at: str = "2026-05-20T10:00:00+00:00",
) -> None:
    raw_source_ids = source_content_ids if isinstance(source_content_ids, str) else json.dumps(list(source_content_ids))
    raw_metadata = (
        json.dumps(metadata if metadata is not None else {"model": "gpt", "provider": "openai", "prompt_version": "v1", "source_content_ids": ["1", "2"]})
        if not isinstance(metadata, str)
        else metadata
    )
    conn.execute(
        """INSERT INTO newsletter_subject_candidates
           (id, newsletter_send_id, issue_id, subject, source, rank, selected, rationale,
            source_content_ids, metadata, created_at)
           VALUES (?, 7, 'issue-a', ?, ?, ?, ?, ?, ?, ?, ?)""",
        (candidate_id, f"Subject {candidate_id}", source, rank, selected, rationale, raw_source_ids, raw_metadata, created_at),
    )


def test_report_flags_metadata_drift_deterministically():
    conn = _conn()
    _candidate(conn, 1, metadata="{bad-json")
    _candidate(conn, 2, metadata={"model": "gpt", "source_content_ids": ["1", "2"]})
    _candidate(conn, 3, source_content_ids=["1", "2"], metadata={"model": "gpt", "provider": "openai", "prompt_version": "v1", "source_content_ids": ["2", "3"]})
    _candidate(conn, 4, selected=1, rationale="", rank=None)
    _candidate(conn, 5, source="heuristic", metadata={})

    report = build_newsletter_subject_candidate_metadata_drift_report_from_db(conn, now=NOW)

    assert report["artifact_type"] == "newsletter_subject_candidate_metadata_drift"
    assert [finding["gap_type"] for finding in report["findings"]] == [
        "malformed_metadata",
        "missing_evaluation_metadata",
        "source_content_ids_mismatch",
        "selected_missing_rationale",
        "selected_missing_rank",
    ]
    assert report["summary"]["by_gap_type"] == {
        "malformed_metadata": 1,
        "missing_evaluation_metadata": 1,
        "selected_missing_rank": 1,
        "selected_missing_rationale": 1,
        "source_content_ids_mismatch": 1,
    }
    assert report["groups"][0] == {"gap_type": "malformed_metadata", "finding_count": 1}


def test_source_days_limit_schema_and_formatters():
    conn = _conn()
    _candidate(conn, 1, metadata={"model": "gpt"}, created_at="2026-05-20T10:00:00+00:00")
    _candidate(conn, 2, source="heuristic", metadata={}, created_at="2026-05-20T10:01:00+00:00")
    _candidate(conn, 3, metadata={"model": "gpt"}, created_at="2026-04-01T10:00:00+00:00")

    report = build_newsletter_subject_candidate_metadata_drift_report_from_db(conn, source="llm", days=10, limit=1, now=NOW)

    assert report["summary"]["candidate_count"] == 1
    assert report["summary"]["finding_count"] == 1
    assert len(report["findings"]) == 1
    assert json.loads(format_newsletter_subject_candidate_metadata_drift_json(report))["artifact_type"] == "newsletter_subject_candidate_metadata_drift"
    assert "candidate_id | source | gap_type" in format_newsletter_subject_candidate_metadata_drift_text(report)

    missing = build_newsletter_subject_candidate_metadata_drift_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["newsletter_subject_candidates"]

    bad = sqlite3.connect(":memory:")
    bad.execute("CREATE TABLE newsletter_subject_candidates (id INTEGER)")
    schema_report = build_newsletter_subject_candidate_metadata_drift_report_from_db(bad, now=NOW)
    assert schema_report["missing_columns"] == {"newsletter_subject_candidates": ["metadata"]}


def test_cli_supports_db_json_text_context_and_invalid_args(tmp_path, monkeypatch, capsys):
    db_path = tmp_path / "subjects.sqlite"
    conn = _conn()
    _candidate(conn, 1, metadata={"model": "gpt"})
    conn.commit()
    dest = sqlite3.connect(db_path)
    conn.backup(dest)
    dest.close()
    conn.close()

    assert script.main(["--db", str(db_path), "--format", "json", "--days", "7", "--source", "llm"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["finding_count"] == 1

    assert script.main(["--db", str(db_path), "--format", "text", "--limit", "1"]) == 0
    assert "Newsletter Subject Candidate Metadata Drift" in capsys.readouterr().out

    monkeypatch.setattr(script, "script_context", lambda: _script_context(sqlite3.connect(":memory:")))
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["missing_tables"] == ["newsletter_subject_candidates"]

    with pytest.raises(SystemExit):
        script.parse_args(["--days", "0"])
