"""Tests for reply draft evaluation outcome reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from engagement.reply_draft_evaluation_outcome import (
    build_reply_draft_evaluation_outcome_report,
    build_reply_draft_evaluation_outcome_report_from_db,
    format_reply_draft_evaluation_outcome_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_draft_evaluation_outcome.py"
spec = importlib.util.spec_from_file_location("reply_draft_evaluation_outcome_script", SCRIPT_PATH)
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
        """CREATE TABLE reply_drafts (
               id INTEGER PRIMARY KEY,
               mention_id TEXT,
               status TEXT,
               updated_at TEXT
           );
           CREATE TABLE reply_draft_evaluations (
               id INTEGER PRIMARY KEY,
               reply_draft_id INTEGER,
               quality_score REAL,
               quality_flags TEXT,
               evaluated_at TEXT
           );"""
    )
    return conn


def _draft(conn: sqlite3.Connection, draft_id: int, *, updated_at: str = "2026-05-20T10:00:00+00:00", status: str = "pending") -> None:
    conn.execute("INSERT INTO reply_drafts VALUES (?, ?, ?, ?)", (draft_id, f"m{draft_id}", status, updated_at))


def _eval(conn: sqlite3.Connection, eval_id: int, draft_id: int, *, score: float = 8.0, flags: list[str] | None = None, at: str = "2026-05-20T11:00:00+00:00") -> None:
    conn.execute(
        "INSERT INTO reply_draft_evaluations VALUES (?, ?, ?, ?, ?)",
        (eval_id, draft_id, score, json.dumps(flags or []), at),
    )


def test_report_emits_missing_stale_low_score_and_flag_findings():
    rows = [
        {"draft_id": 1, "status": "pending", "draft_updated_at": "2026-05-20T10:00:00+00:00"},
        {"draft_id": 2, "status": "pending", "draft_updated_at": "2026-05-20T10:00:00+00:00", "evaluation_id": 20, "quality_score": 6.5, "quality_flags": "[]", "evaluated_at": "2026-05-20T11:00:00+00:00"},
        {"draft_id": 3, "status": "pending", "draft_updated_at": "2026-05-20T10:00:00+00:00", "evaluation_id": 30, "quality_score": 8.0, "quality_flags": '["sycophantic", "generic"]', "evaluated_at": "2026-05-20T11:00:00+00:00"},
        {"draft_id": 4, "status": "pending", "draft_updated_at": "2026-05-20T10:00:00+00:00", "evaluation_id": 40, "quality_score": 8.0, "quality_flags": "[]", "evaluated_at": "2026-05-20T09:00:00+00:00"},
    ]

    report = build_reply_draft_evaluation_outcome_report(rows, min_quality_score=7.0, now=NOW)
    by_id = {finding["draft_id"]: finding for finding in report["findings"]}

    assert by_id["1"]["reason_codes"] == ["missing_evaluation"]
    assert by_id["2"]["reason_codes"] == ["low_quality_score"]
    assert by_id["3"]["reason_codes"] == ["generic_flag", "sycophantic_flag"]
    assert by_id["4"]["reason_codes"] == ["stale_evaluation"]
    assert report["summary"]["finding_count"] == 4
    assert "Reply Draft Evaluation Outcome" in format_reply_draft_evaluation_outcome_text(report)


def test_clean_state_has_no_findings():
    report = build_reply_draft_evaluation_outcome_report(
        [
            {
                "draft_id": 1,
                "status": "pending",
                "draft_updated_at": "2026-05-20T10:00:00+00:00",
                "evaluation_id": 10,
                "quality_score": 8.5,
                "quality_flags": "[]",
                "evaluated_at": "2026-05-20T11:00:00+00:00",
            }
        ],
        now=NOW,
    )
    assert report["findings"] == []
    assert report["summary"]["finding_count"] == 0


def test_db_loader_uses_latest_evaluation_and_filters_reviewed_statuses():
    conn = _conn()
    _draft(conn, 1)
    _draft(conn, 2)
    _draft(conn, 3, status="approved")
    _eval(conn, 1, 1, score=5.0, at="2026-05-20T09:00:00+00:00")
    _eval(conn, 2, 1, score=8.0, at="2026-05-20T11:00:00+00:00")
    _eval(conn, 3, 3, score=4.0)
    conn.commit()

    report = build_reply_draft_evaluation_outcome_report_from_db(conn, now=NOW)

    assert [finding["draft_id"] for finding in report["findings"]] == ["2"]
    assert report["findings"][0]["reason_codes"] == ["missing_evaluation"]


def test_reply_queue_inline_evaluation_fields_are_supported():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """CREATE TABLE reply_queue (
               id INTEGER PRIMARY KEY,
               status TEXT,
               quality_score REAL,
               quality_flags TEXT,
               updated_at TEXT,
               quality_evaluated_at TEXT
           )"""
    )
    conn.execute("INSERT INTO reply_queue VALUES (1, 'pending', 6.0, '[]', '2026-05-20T10:00:00+00:00', '2026-05-20T11:00:00+00:00')")
    report = build_reply_draft_evaluation_outcome_report_from_db(conn, min_quality_score=7.0, now=NOW)
    assert report["findings"][0]["draft_id"] == "1"
    assert report["findings"][0]["reason_codes"] == ["low_quality_score"]


def test_cli_json_output_supports_threshold_arguments(monkeypatch, capsys):
    monkeypatch.setattr(script, "script_context", lambda: _script_context(SimpleNamespace()))
    monkeypatch.setattr(
        script,
        "build_reply_draft_evaluation_outcome_report_from_db",
        lambda _db, **kwargs: build_reply_draft_evaluation_outcome_report(
            [{"draft_id": 1, "status": "pending"}],
            now=NOW,
            **{key: value for key, value in kwargs.items() if key in {"limit", "min_quality_score"}},
        ),
    )

    assert script.main(["--min-quality-score", "7.5", "--limit", "5", "--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "reply_draft_evaluation_outcome"
    assert payload["filters"]["min_quality_score"] == 7.5
