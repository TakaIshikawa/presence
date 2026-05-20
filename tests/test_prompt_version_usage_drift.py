"""Tests for prompt version usage drift reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.prompt_version_usage_drift import (
    build_prompt_version_usage_drift_report_from_db,
    format_prompt_version_usage_drift_json,
    format_prompt_version_usage_drift_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "prompt_version_usage_drift.py"
spec = importlib.util.spec_from_file_location("prompt_version_usage_drift_script", SCRIPT_PATH)
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
        """CREATE TABLE prompt_versions (
             id INTEGER PRIMARY KEY,
             prompt_type TEXT,
             prompt_version TEXT,
             prompt_hash TEXT,
             usage_count INTEGER
           );
           CREATE TABLE engagement_predictions (
             id INTEGER PRIMARY KEY,
             prompt_type TEXT,
             prompt_version TEXT,
             prompt_hash TEXT
           );"""
    )
    return conn


def test_report_flags_usage_unknown_hash_mismatch_and_reused_hashes():
    conn = _conn()
    conn.executemany(
        "INSERT INTO prompt_versions VALUES (?, ?, ?, ?, ?)",
        [
            (1, "reply", "v1", "h1", 3),
            (2, "draft", "v1", "h1", 0),
            (3, "reply", "v2", "h2", 0),
        ],
    )
    conn.executemany(
        "INSERT INTO engagement_predictions VALUES (?, ?, ?, ?)",
        [
            (10, "reply", "v1", "h1"),
            (11, "reply", "v1", "wrong"),
            (12, "missing", "v1", "h9"),
        ],
    )

    report = build_prompt_version_usage_drift_report_from_db(conn, now=NOW, min_delta=1)

    assert report["artifact_type"] == "prompt_version_usage_drift"
    assert report["prompt_count"] == 3
    assert report["prediction_count"] == 3
    issue_types = [finding["issue_type"] for finding in report["findings"]]
    assert issue_types.count("usage_count_mismatch") == 1
    assert "prompt_hash_mismatch" in issue_types
    assert "unknown_prompt_reference" in issue_types
    assert "hash_reused_across_types" in issue_types
    assert report["issue_counts"]["usage_count_mismatch"] == 1


def test_prompt_type_filter_and_min_delta():
    conn = _conn()
    conn.executemany(
        "INSERT INTO prompt_versions VALUES (?, ?, ?, ?, ?)",
        [(1, "reply", "v1", "h1", 2), (2, "draft", "v1", "h2", 10)],
    )
    conn.execute("INSERT INTO engagement_predictions VALUES (1, 'reply', 'v1', 'h1')")

    report = build_prompt_version_usage_drift_report_from_db(conn, prompt_type="reply", min_delta=2, now=NOW)

    assert report["prompt_count"] == 1
    assert report["prediction_count"] == 1
    assert report["findings"] == []


def test_formatters_cli_and_schema(tmp_path, monkeypatch, capsys):
    conn = _conn()
    conn.execute("INSERT INTO prompt_versions VALUES (1, 'reply', 'v1', 'h1', 1)")
    conn.commit()
    report = build_prompt_version_usage_drift_report_from_db(conn, now=NOW)
    assert json.loads(format_prompt_version_usage_drift_json(report))["artifact_type"] == "prompt_version_usage_drift"
    assert "usage_count_mismatch=1" in format_prompt_version_usage_drift_text(report)

    db_path = tmp_path / "prompts.sqlite"
    dest = sqlite3.connect(db_path)
    conn.backup(dest)
    dest.close()
    assert script.main(["--db", str(db_path), "--format", "json", "--prompt-type", "reply", "--min-delta", "1"]) == 0
    assert json.loads(capsys.readouterr().out)["finding_count"] == 1
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Prompt Version Usage Drift" in capsys.readouterr().out

    memory = _conn()
    monkeypatch.setattr(script, "script_context", lambda: _script_context(memory))
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["findings"] == []


def test_missing_schema_and_invalid_delta():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE prompt_versions (id INTEGER PRIMARY KEY)")
    report = build_prompt_version_usage_drift_report_from_db(conn, now=NOW)
    assert report["missing_tables"] == ["engagement_predictions"]
    assert report["missing_columns"] == {
        "prompt_versions": ["prompt_hash", "prompt_type", "prompt_version", "usage_count"],
    }
    with pytest.raises(ValueError, match="min_delta must be non-negative"):
        build_prompt_version_usage_drift_report_from_db(_conn(), min_delta=-1)
