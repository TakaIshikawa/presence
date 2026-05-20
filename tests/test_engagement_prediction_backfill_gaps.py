"""Tests for engagement prediction backfill gap reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.engagement_prediction_backfill_gaps import (
    build_engagement_prediction_backfill_gaps_report_from_db,
    format_engagement_prediction_backfill_gaps_json,
    format_engagement_prediction_backfill_gaps_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "engagement_prediction_backfill_gaps.py"
spec = importlib.util.spec_from_file_location("engagement_prediction_backfill_gaps_script", SCRIPT_PATH)
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
             status TEXT,
             content_type TEXT,
             platform TEXT
           );
           CREATE TABLE engagement_predictions (
             id INTEGER PRIMARY KEY,
             content_id INTEGER,
             predicted_engagement_score REAL,
             actual_engagement_score REAL,
             prediction_error REAL,
             created_at TEXT
           );
           CREATE TABLE post_engagement (
             id INTEGER PRIMARY KEY,
             content_id INTEGER,
             engagement_score REAL,
             recorded_at TEXT
           );"""
    )
    return conn


def test_report_flags_backfill_gaps_and_mismatches():
    conn = _conn()
    conn.executemany(
        "INSERT INTO generated_content (id, status, content_type, platform) VALUES (?, ?, ?, ?)",
        [(1, "published", "post", "x"), (2, "draft", "post", "x"), (3, "published", "post", "x")],
    )
    conn.executemany(
        """INSERT INTO engagement_predictions
           (id, content_id, predicted_engagement_score, actual_engagement_score, prediction_error, created_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        [
            (10, 99, 0.5, None, None, "2026-05-19T12:00:00+00:00"),
            (11, 1, 0.2, None, None, "2026-05-19T12:00:00+00:00"),
            (12, 2, 0.4, None, None, "2026-04-01T12:00:00+00:00"),
            (13, 3, 0.2, 0.9, 0.1, "2026-05-19T12:00:00+00:00"),
        ],
    )
    conn.executemany(
        "INSERT INTO post_engagement (id, content_id, engagement_score, recorded_at) VALUES (?, ?, ?, ?)",
        [(1, 1, 0.75, "2026-05-19T13:00:00+00:00"), (2, 3, 0.9, "2026-05-19T13:00:00+00:00")],
    )

    report = build_engagement_prediction_backfill_gaps_report_from_db(conn, now=NOW, days=14, max_error_delta=0.01)

    assert report["artifact_type"] == "engagement_prediction_backfill_gaps"
    assert [finding["issue_type"] for finding in report["findings"]] == [
        "missing_content",
        "metrics_available_not_backfilled",
        "stale_without_metrics",
        "prediction_error_mismatch",
    ]
    assert report["totals"]["issue_counts"]["metrics_available_not_backfilled"] == 1
    assert report["totals"]["affected_content_ids"] == [1, 2, 3, 99]


def test_uses_latest_post_engagement_metric():
    conn = _conn()
    conn.execute("INSERT INTO generated_content (id, status, content_type, platform) VALUES (1, 'published', 'post', 'x')")
    conn.execute(
        """INSERT INTO engagement_predictions
           (id, content_id, predicted_engagement_score, actual_engagement_score, prediction_error, created_at)
           VALUES (1, 1, 0.1, 0.8, 0.7, '2026-05-19T12:00:00+00:00')"""
    )
    conn.executemany(
        "INSERT INTO post_engagement (id, content_id, engagement_score, recorded_at) VALUES (?, ?, ?, ?)",
        [(1, 1, 0.2, "2026-05-18T12:00:00+00:00"), (2, 1, 0.8, "2026-05-19T12:00:00+00:00")],
    )

    report = build_engagement_prediction_backfill_gaps_report_from_db(conn, now=NOW)

    assert report["findings"] == []


def test_formatters_cli_and_schema(tmp_path, monkeypatch, capsys):
    conn = _conn()
    conn.execute(
        """INSERT INTO engagement_predictions
           (id, content_id, predicted_engagement_score, created_at)
           VALUES (1, 42, 0.5, '2026-05-19T12:00:00+00:00')"""
    )
    conn.commit()
    report = build_engagement_prediction_backfill_gaps_report_from_db(conn, now=NOW)
    assert json.loads(format_engagement_prediction_backfill_gaps_json(report))["artifact_type"] == "engagement_prediction_backfill_gaps"
    assert "missing_content=1" in format_engagement_prediction_backfill_gaps_text(report)

    db_path = tmp_path / "predictions.sqlite"
    dest = sqlite3.connect(db_path)
    conn.backup(dest)
    dest.close()
    assert script.main(["--db", str(db_path), "--format", "json", "--days", "7", "--max-error-delta", "0.1"]) == 0
    assert json.loads(capsys.readouterr().out)["totals"]["finding_count"] == 1
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Engagement Prediction Backfill Gaps" in capsys.readouterr().out

    memory = _conn()
    monkeypatch.setattr(script, "script_context", lambda: _script_context(memory))
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["findings"] == []


def test_missing_schema_and_invalid_args():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE engagement_predictions (id INTEGER PRIMARY KEY)")
    report = build_engagement_prediction_backfill_gaps_report_from_db(conn, now=NOW)
    assert report["missing_tables"] == ["generated_content", "post_engagement"]
    assert report["missing_columns"] == {"engagement_predictions": ["content_id"]}
    with pytest.raises(ValueError, match="days must be positive"):
        build_engagement_prediction_backfill_gaps_report_from_db(_conn(), days=0)
