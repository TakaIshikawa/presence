"""Tests for commit prompt link confidence decay reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.commit_prompt_link_confidence_decay import (
    build_commit_prompt_link_confidence_decay_report,
    build_commit_prompt_link_confidence_decay_report_from_db,
    format_commit_prompt_link_confidence_decay_json,
    format_commit_prompt_link_confidence_decay_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "commit_prompt_link_confidence_decay.py"
spec = importlib.util.spec_from_file_location("commit_prompt_link_confidence_decay_script", SCRIPT_PATH)
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
        """CREATE TABLE github_commits (
             id INTEGER PRIMARY KEY,
             commit_sha TEXT,
             timestamp TEXT
           );
           CREATE TABLE claude_messages (
             id INTEGER PRIMARY KEY,
             message_uuid TEXT,
             timestamp TEXT
           );
           CREATE TABLE commit_prompt_links (
             id INTEGER PRIMARY KEY,
             commit_id INTEGER,
             message_id INTEGER,
             confidence REAL
           );"""
    )
    return conn


def _commit(conn: sqlite3.Connection, commit_id: int, sha: str, timestamp: str) -> None:
    conn.execute(
        "INSERT INTO github_commits (id, commit_sha, timestamp) VALUES (?, ?, ?)",
        (commit_id, sha, timestamp),
    )


def _message(conn: sqlite3.Connection, message_id: int, uuid: str, timestamp: str) -> None:
    conn.execute(
        "INSERT INTO claude_messages (id, message_uuid, timestamp) VALUES (?, ?, ?)",
        (message_id, uuid, timestamp),
    )


def _link(conn: sqlite3.Connection, link_id: int, commit_id: int, message_id: int, confidence: float | None) -> None:
    conn.execute(
        "INSERT INTO commit_prompt_links (id, commit_id, message_id, confidence) VALUES (?, ?, ?, ?)",
        (link_id, commit_id, message_id, confidence),
    )
    conn.commit()


def test_clean_data_has_no_findings():
    conn = _conn()
    _commit(conn, 1, "abc", "2026-05-02T10:00:00+00:00")
    _message(conn, 2, "msg", "2026-05-02T10:30:00+00:00")
    _link(conn, 3, 1, 2, 0.9)

    report = build_commit_prompt_link_confidence_decay_report_from_db(
        conn,
        min_confidence=0.5,
        max_gap_hours=2,
        now=NOW,
    )

    assert report["artifact_type"] == "commit_prompt_link_confidence_decay"
    assert report["totals"]["links_scanned"] == 1
    assert report["totals"]["finding_count"] == 0
    assert report["findings"] == []
    assert report["missing_tables"] == []
    assert report["missing_columns"] == {}


def test_classifies_each_issue_type_and_sorts_deterministically():
    conn = _conn()
    _commit(conn, 1, "low", "2026-05-02T10:00:00+00:00")
    _message(conn, 1, "low-msg", "2026-05-02T10:05:00+00:00")
    _commit(conn, 2, "gap", "2026-05-02T10:00:00+00:00")
    _message(conn, 2, "gap-msg", "2026-05-02T02:00:00+00:00")
    _link(conn, 10, 1, 1, 0.2)
    _link(conn, 11, 99, 1, 0.9)
    _link(conn, 12, 1, 99, 0.9)
    _link(conn, 13, 2, 2, 0.95)

    report = build_commit_prompt_link_confidence_decay_report_from_db(
        conn,
        min_confidence=0.5,
        max_gap_hours=2,
        now=NOW,
    )

    assert [finding["issue_type"] for finding in report["findings"]] == [
        "low_confidence",
        "missing_commit",
        "missing_message",
        "timestamp_gap_exceeded",
    ]
    assert report["totals"]["low_confidence"] == 1
    assert report["totals"]["missing_commit"] == 1
    assert report["totals"]["missing_message"] == 1
    assert report["totals"]["timestamp_gap_exceeded"] == 1
    assert report["findings"][3]["timestamp_gap_hours"] == 8.0


def test_missing_schema_is_reported_without_querying_links():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE commit_prompt_links (id INTEGER PRIMARY KEY, commit_id INTEGER)")

    report = build_commit_prompt_link_confidence_decay_report_from_db(conn, now=NOW)

    assert report["missing_tables"] == ["claude_messages", "github_commits"]
    assert report["missing_columns"] == {
        "commit_prompt_links": ["confidence", "message_id"],
    }
    assert report["totals"]["links_scanned"] == 0
    assert report["findings"] == []


def test_formatters_are_stable_and_text_is_readable():
    report = build_commit_prompt_link_confidence_decay_report(
        [
            {
                "link_id": 1,
                "commit_id": 10,
                "message_id": 20,
                "confidence": 0.25,
                "commit_sha": "abc",
                "commit_timestamp": "2026-05-02T10:00:00+00:00",
                "message_uuid": "msg",
                "message_timestamp": "2026-05-02T10:10:00+00:00",
            }
        ],
        min_confidence=0.5,
        now=NOW,
    )

    payload = json.loads(format_commit_prompt_link_confidence_decay_json(report))
    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "commit_prompt_link_confidence_decay"
    text = format_commit_prompt_link_confidence_decay_text(report)
    assert "Commit Prompt Link Confidence Decay" in text
    assert "low_confidence link_id=1" in text
    assert "commit_sha=abc" in text


def test_cli_supports_db_json_text_and_argument_validation(tmp_path, monkeypatch, capsys):
    db_path = tmp_path / "links.sqlite"
    conn = _conn()
    _commit(conn, 1, "abc", "2026-05-02 10:00:00")
    _message(conn, 1, "msg", "2026-05-02 04:00:00")
    _link(conn, 1, 1, 1, 0.25)
    conn.backup(sqlite3.connect(db_path))
    conn.close()

    assert script.main(["--db", str(db_path), "--min-confidence", "0.5", "--max-gap-hours", "2", "--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["totals"]["finding_count"] == 2

    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Commit Prompt Link Confidence Decay" in capsys.readouterr().out

    memory = _conn()
    monkeypatch.setattr(script, "script_context", lambda: _script_context(memory))
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["findings"] == []
    assert script.main(["--min-confidence", "1.5"]) == 2
    assert "value must be between 0 and 1" in capsys.readouterr().err


def test_builder_rejects_invalid_thresholds():
    with pytest.raises(ValueError, match="min_confidence must be between 0 and 1"):
        build_commit_prompt_link_confidence_decay_report([], min_confidence=-0.1)
    with pytest.raises(ValueError, match="max_gap_hours must be non-negative"):
        build_commit_prompt_link_confidence_decay_report([], max_gap_hours=-1)
