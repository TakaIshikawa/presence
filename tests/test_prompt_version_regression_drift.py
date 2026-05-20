"""Tests for prompt version regression drift reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.prompt_version_regression_drift import (
    build_prompt_version_regression_drift_report_from_db,
    format_prompt_version_regression_drift_json,
    format_prompt_version_regression_drift_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "prompt_version_regression_drift.py"
spec = importlib.util.spec_from_file_location("prompt_version_regression_drift_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _conn(*, include_prompt_version: bool = True) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    version_col = "prompt_version TEXT," if include_prompt_version else ""
    conn.executescript(
        f"""CREATE TABLE eval_results (
             id INTEGER PRIMARY KEY,
             prompt_family TEXT,
             {version_col}
             quality_score REAL,
             authenticity_score REAL,
             engagement_score REAL,
             created_at TEXT
           );
           CREATE TABLE prompt_versions (
             id INTEGER PRIMARY KEY,
             prompt_type TEXT,
             version INTEGER,
             prompt_hash TEXT,
             created_at TEXT
           );"""
    )
    return conn


def _insert_eval(conn: sqlite3.Connection, row_id: int, family: str | None, version: str | None, quality: float, auth: float, engagement: float) -> None:
    columns = {info["name"] for info in conn.execute("PRAGMA table_info(eval_results)")}
    if "prompt_version" in columns:
        conn.execute(
            "INSERT INTO eval_results VALUES (?, ?, ?, ?, ?, ?, ?)",
            (row_id, family, version, quality, auth, engagement, f"2026-05-{row_id:02d}T00:00:00+00:00"),
        )
    else:
        conn.execute(
            "INSERT INTO eval_results VALUES (?, ?, ?, ?, ?, ?)",
            (row_id, family, quality, auth, engagement, f"2026-05-{row_id:02d}T00:00:00+00:00"),
        )
    conn.commit()


def test_no_regression_when_current_scores_hold():
    conn = _conn()
    for idx, score in enumerate((8.0, 8.2, 8.1), start=1):
        _insert_eval(conn, idx, "x_post", "v1", score, 7.5, 6.0)
    for idx, score in enumerate((8.3, 8.1, 8.2), start=4):
        _insert_eval(conn, idx, "x_post", "v2", score, 7.6, 6.2)

    report = build_prompt_version_regression_drift_report_from_db(conn, now=NOW, min_samples=3)

    assert report["artifact_type"] == "prompt_version_regression_drift"
    assert report["findings"] == []
    assert report["summary"]["version_group_count"] == 2


def test_flags_score_regression_against_prior_stable_version():
    conn = _conn()
    for idx in range(1, 4):
        _insert_eval(conn, idx, "x_thread", "v1", 8.5, 8.0, 7.5)
    for idx in range(4, 7):
        _insert_eval(conn, idx, "x_thread", "v2", 6.0, 7.8, 5.0)

    report = build_prompt_version_regression_drift_report_from_db(conn, now=NOW, min_samples=3, regression_threshold=0.5)

    finding = report["findings"][0]
    assert finding["prompt_family"] == "x_thread"
    assert finding["current_version"] == "v2"
    assert finding["baseline_version"] == "v1"
    assert finding["metric_deltas"]["quality_score"] == -2.5
    assert finding["metric_deltas"]["engagement_score"] == -2.5
    assert finding["current_sample_count"] == 3
    assert finding["baseline_sample_count"] == 3
    assert finding["severity"] == "critical"


def test_insufficient_sample_size_skips_regression():
    conn = _conn()
    for idx in range(1, 4):
        _insert_eval(conn, idx, "blog_post", "v1", 8.0, 8.0, 8.0)
    _insert_eval(conn, 4, "blog_post", "v2", 2.0, 2.0, 2.0)

    report = build_prompt_version_regression_drift_report_from_db(conn, now=NOW, min_samples=3)

    assert report["findings"] == []
    assert report["summary"]["skipped_counts"]["insufficient_current_samples"] == 1


def test_missing_prompt_version_metadata_is_counted_and_prompt_windows_fill_version():
    conn = _conn(include_prompt_version=False)
    conn.executemany(
        "INSERT INTO prompt_versions VALUES (?, ?, ?, ?, ?)",
        [
            (1, "reply", 1, "h1", "2026-05-01T00:00:00+00:00"),
            (2, "reply", 2, "h2", "2026-05-10T00:00:00+00:00"),
        ],
    )
    for idx in range(2, 5):
        _insert_eval(conn, idx, "reply", None, 8.0, 8.0, 8.0)
    for idx in range(11, 14):
        _insert_eval(conn, idx, "reply", None, 6.0, 6.0, 6.0)
    _insert_eval(conn, 15, None, None, 5.0, 5.0, 5.0)

    report = build_prompt_version_regression_drift_report_from_db(conn, now=NOW, min_samples=3)

    assert report["summary"]["missing_prompt_version_metadata_count"] == 1
    assert report["findings"][0]["current_version"] == "2"
    assert report["findings"][0]["baseline_version"] == "1"


def test_formatters_cli_and_invalid_arguments(tmp_path, monkeypatch, capsys):
    conn = _conn()
    for idx in range(1, 4):
        _insert_eval(conn, idx, "x_post", "v1", 8.0, 8.0, 8.0)
    report = build_prompt_version_regression_drift_report_from_db(conn, now=NOW)
    assert json.loads(format_prompt_version_regression_drift_json(report))["artifact_type"] == "prompt_version_regression_drift"
    assert "Prompt Version Regression Drift" in format_prompt_version_regression_drift_text(report)

    db_path = tmp_path / "prompts.sqlite"
    dest = sqlite3.connect(db_path)
    conn.backup(dest)
    dest.close()
    assert script.main(["--db", str(db_path), "--format", "json", "--min-samples", "3"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["version_group_count"] == 1
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Prompt Version Regression Drift" in capsys.readouterr().out

    monkeypatch.setattr(script, "script_context", lambda: _script_context(sqlite3.connect(":memory:")))
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["missing_tables"] == ["eval_results"]
    with pytest.raises(SystemExit):
        script.parse_args(["--min-samples", "0"])
