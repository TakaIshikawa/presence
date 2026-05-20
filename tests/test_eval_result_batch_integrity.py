"""Tests for eval result batch integrity reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.eval_result_batch_integrity import (
    build_eval_result_batch_integrity_report_from_db,
    format_eval_result_batch_integrity_json,
    format_eval_result_batch_integrity_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "eval_result_batch_integrity.py"
spec = importlib.util.spec_from_file_location("eval_result_batch_integrity_script", SCRIPT_PATH)
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
        """CREATE TABLE eval_batches (
             id INTEGER PRIMARY KEY,
             content_type TEXT,
             generator_model TEXT,
             evaluator_model TEXT,
             threshold REAL
           );
           CREATE TABLE eval_results (
             id INTEGER PRIMARY KEY,
             batch_id INTEGER,
             content_type TEXT,
             generator_model TEXT,
             evaluator_model TEXT,
             threshold REAL,
             candidate_count INTEGER,
             accepted_count INTEGER,
             rejected_count INTEGER,
             final_content TEXT,
             final_score REAL,
             created_at TEXT
           );"""
    )
    return conn


def test_report_flags_each_integrity_issue():
    conn = _conn()
    conn.execute("INSERT INTO eval_batches VALUES (1, 'post', 'g1', 'e1', 0.7)")
    conn.executemany(
        """INSERT INTO eval_results
           (id, batch_id, content_type, generator_model, evaluator_model, threshold, candidate_count, accepted_count, rejected_count, final_content, final_score, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            (1, 99, "post", "g1", "e1", 0.7, 1, 0, 1, None, None, "2026-05-20T10:00:00+00:00"),
            (2, 1, "thread", "g2", "e2", 0.8, -1, 0, 1, "final", None, "2026-05-20T10:00:00+00:00"),
        ],
    )

    report = build_eval_result_batch_integrity_report_from_db(conn, now=NOW)

    assert report["artifact_type"] == "eval_result_batch_integrity"
    assert report["totals"]["result_count"] == 2
    assert [finding["issue_type"] for finding in report["findings"]] == [
        "missing_batch",
        "content_type_mismatch",
        "generator_model_mismatch",
        "evaluator_model_mismatch",
        "threshold_mismatch",
        "invalid_counter",
        "final_content_without_score",
    ]
    assert report["totals"]["issue_counts"]["invalid_counter"] == 1


def test_days_and_content_type_filters():
    conn = _conn()
    conn.execute("INSERT INTO eval_batches VALUES (1, 'post', 'g1', 'e1', 0.7)")
    conn.executemany(
        """INSERT INTO eval_results
           (id, batch_id, content_type, generator_model, evaluator_model, threshold, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        [
            (1, 1, "post", "g1", "e1", 0.7, "2026-04-01T00:00:00+00:00"),
            (2, 1, "thread", "g1", "e1", 0.7, "2026-05-20T00:00:00+00:00"),
        ],
    )
    report = build_eval_result_batch_integrity_report_from_db(conn, now=NOW, days=7, content_type="post")

    assert report["totals"]["result_count"] == 0


def test_formatters_cli_and_schema(tmp_path, monkeypatch, capsys):
    conn = _conn()
    conn.execute("INSERT INTO eval_results (id, batch_id, created_at) VALUES (1, 42, '2026-05-20T10:00:00+00:00')")
    conn.commit()
    report = build_eval_result_batch_integrity_report_from_db(conn, now=NOW)
    assert json.loads(format_eval_result_batch_integrity_json(report))["artifact_type"] == "eval_result_batch_integrity"
    assert "missing_batch=1" in format_eval_result_batch_integrity_text(report)

    db_path = tmp_path / "eval.sqlite"
    dest = sqlite3.connect(db_path)
    conn.backup(dest)
    dest.close()
    assert script.main(["--db", str(db_path), "--format", "json", "--days", "7"]) == 0
    assert json.loads(capsys.readouterr().out)["totals"]["finding_count"] == 1
    assert script.main(["--db", str(db_path), "--format", "text", "--content-type", "post"]) == 0
    assert "Eval Result Batch Integrity" in capsys.readouterr().out

    memory = _conn()
    monkeypatch.setattr(script, "script_context", lambda: _script_context(memory))
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["findings"] == []


def test_missing_schema_and_invalid_days():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE eval_results (id INTEGER PRIMARY KEY)")
    report = build_eval_result_batch_integrity_report_from_db(conn, now=NOW)
    assert report["missing_tables"] == ["eval_batches"]
    assert report["missing_columns"] == {"eval_results": ["batch_id"]}
    with pytest.raises(ValueError, match="days must be positive"):
        build_eval_result_batch_integrity_report_from_db(_conn(), days=0)
