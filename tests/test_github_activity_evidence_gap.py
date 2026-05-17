"""Tests for GitHub activity evidence gap reporting."""

from __future__ import annotations

from contextlib import contextmanager
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.github_activity_evidence_gap import build_github_activity_evidence_gap_report


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "github_activity_evidence_gap.py"
spec = importlib.util.spec_from_file_location("github_activity_evidence_gap_script", SCRIPT_PATH)
github_activity_evidence_gap_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(github_activity_evidence_gap_script)


@contextmanager
def _script_context(conn: sqlite3.Connection):
    yield SimpleNamespace(), SimpleNamespace(conn=conn)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE generated_content (
            id TEXT, content_type TEXT, title TEXT, github_activity_id TEXT,
            commit_sha TEXT, session_id TEXT, pr_url TEXT, source_url TEXT,
            source_activity_ids TEXT
        )"""
    )
    return conn


def test_complete_evidence_is_not_reported():
    conn = _conn()
    conn.execute(
        "INSERT INTO generated_content VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("c1", "blog", "Complete", "act-1", "abc", "s1", "https://github/p/1", "https://github/src", '["act-1"]'),
    )
    assert build_github_activity_evidence_gap_report(conn)["findings"] == []


def test_partial_gap_lists_missing_evidence_types():
    conn = _conn()
    conn.execute(
        "INSERT INTO generated_content VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("c1", "blog", "Partial", "act-1", "abc", "", "", "", '["act-1"]'),
    )
    finding = build_github_activity_evidence_gap_report(conn)["findings"][0]
    assert finding["missing_evidence"] == ["session", "pr"]
    assert finding["severity"] == "medium"


def test_orphan_generated_content_is_reported_high():
    conn = _conn()
    conn.execute(
        "INSERT INTO generated_content VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("c1", "blog", "Orphan", "", "", "", "", "", ""),
    )
    finding = build_github_activity_evidence_gap_report(conn)["findings"][0]
    assert finding["activity_id"] == ""
    assert finding["severity"] == "high"


def test_cli_json_output(capsys, monkeypatch):
    conn = _conn()
    conn.execute(
        "INSERT INTO generated_content VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("c1", "blog", "Partial", "act-1", "abc", "", "", "", '["act-1"]'),
    )
    monkeypatch.setattr(github_activity_evidence_gap_script, "script_context", lambda: _script_context(conn))
    assert github_activity_evidence_gap_script.main(["--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "github_activity_evidence_gap"
    assert payload["findings"][0]["content_id"] == "c1"
