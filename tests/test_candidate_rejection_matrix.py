"""Tests for candidate rejection matrix reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from evaluation.candidate_rejection_matrix import build_candidate_rejection_matrix_report, build_candidate_rejection_matrix_report_from_db


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "candidate_rejection_matrix.py"
spec = importlib.util.spec_from_file_location("candidate_rejection_matrix_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_groups_by_prompt_format_reason_and_outcome():
    report = build_candidate_rejection_matrix_report(
        [
            {"prompt_version": "p1", "format": "blog", "rejection_reason": "off topic", "outcome": "rejected"},
            {"prompt_version": "p1", "format": "blog", "rejection_reason": "off topic", "outcome": "rejected"},
            {"prompt_version": "p1", "format": "x", "outcome": "accepted"},
        ],
        now=NOW,
    )

    assert report["matrix"][0]["count"] == 2
    assert report["matrix"][0]["rejection_reason"] == "off_topic"
    assert report["totals"] == {"reviewed_count": 3, "rejected_count": 2, "accepted_count": 1, "rejection_rate": 0.6667}


def test_text_matrix_sorts_highest_rejection_count_first():
    report = build_candidate_rejection_matrix_report(
        [
            {"prompt_version": "p2", "format": "blog", "outcome": "accepted"},
            {"prompt_version": "p1", "format": "blog", "reason": "stale", "status": "rejected"},
        ],
        now=NOW,
    )

    assert report["matrix"][0]["prompt_version"] == "p1"
    assert "prompt_version | format | reason" in script.format_candidate_rejection_matrix_text(report)


def test_db_adapter_tolerates_generated_content_fallback():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE generated_content (id INTEGER PRIMARY KEY, content_type TEXT)")
    report = build_candidate_rejection_matrix_report_from_db(conn, now=NOW)

    assert report["totals"]["reviewed_count"] == 0
    assert report["empty_state"]["is_empty"] is True


def test_cli_json_and_table(monkeypatch, capsys):
    monkeypatch.setattr(script, "script_context", lambda: _script_context(SimpleNamespace()))
    monkeypatch.setattr(
        script,
        "build_candidate_rejection_matrix_report_from_db",
        lambda _db: build_candidate_rejection_matrix_report([{"prompt_version": "p1", "format": "blog", "outcome": "rejected"}], now=NOW),
    )

    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "candidate_rejection_matrix"
    assert script.main(["--table"]) == 0
    assert "Candidate Rejection Matrix" in capsys.readouterr().out
