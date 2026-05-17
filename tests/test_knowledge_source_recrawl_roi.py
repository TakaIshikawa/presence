"""Tests for knowledge source recrawl ROI reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from knowledge.source_recrawl_roi import build_source_recrawl_roi_report


NOW = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "source_recrawl_roi.py"
spec = importlib.util.spec_from_file_location("source_recrawl_roi_script", SCRIPT_PATH)
source_recrawl_roi_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(source_recrawl_roi_script)


@contextmanager
def _script_context(conn: sqlite3.Connection):
    yield SimpleNamespace(), SimpleNamespace(conn=conn)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE knowledge_sources (id TEXT, url TEXT, last_crawled_at TEXT, usage_count INTEGER, failure_count INTEGER, success_rate REAL)"
    )
    return conn


def test_ranks_candidates_by_staleness_usage_and_failure_penalty():
    conn = _conn()
    conn.executemany(
        "INSERT INTO knowledge_sources VALUES (?, ?, ?, ?, ?, ?)",
        [
            ("a", "https://a.example", "2026-04-18T12:00:00+00:00", 10, 0, 1.0),
            ("b", "https://b.example", "2026-01-18T12:00:00+00:00", 1, 5, 0.2),
            ("c", "https://c.example", "2026-05-17T12:00:00+00:00", 50, 0, 1.0),
        ],
    )
    report = build_source_recrawl_roi_report(conn, now=NOW)
    assert [item["source_id"] for item in report["candidates"]] == ["c", "b", "a"]
    assert report["candidates"][1]["failure_penalty"] > 0


def test_configurable_weights_change_ordering():
    conn = _conn()
    conn.executemany(
        "INSERT INTO knowledge_sources VALUES (?, ?, ?, ?, ?, ?)",
        [
            ("old", "https://old.example", "2026-01-18T12:00:00+00:00", 1, 0, 1.0),
            ("used", "https://used.example", "2026-05-17T12:00:00+00:00", 20, 0, 1.0),
        ],
    )
    report = build_source_recrawl_roi_report(conn, staleness_weight=0.1, usage_weight=10, now=NOW)
    assert report["candidates"][0]["source_id"] == "used"


def test_stable_ordering_for_tied_scores():
    conn = _conn()
    conn.executemany(
        "INSERT INTO knowledge_sources VALUES (?, ?, ?, ?, ?, ?)",
        [
            ("b", "https://b.example", "2026-05-17T12:00:00+00:00", 1, 0, 1.0),
            ("a", "https://a.example", "2026-05-17T12:00:00+00:00", 1, 0, 1.0),
        ],
    )
    report = build_source_recrawl_roi_report(conn, now=NOW)
    assert [item["source_id"] for item in report["candidates"]] == ["a", "b"]


def test_cli_json_output(capsys, monkeypatch):
    conn = _conn()
    conn.execute(
        "INSERT INTO knowledge_sources VALUES (?, ?, ?, ?, ?, ?)",
        ("a", "https://a.example", "2026-05-17T12:00:00+00:00", 1, 0, 1.0),
    )
    monkeypatch.setattr(source_recrawl_roi_script, "script_context", lambda: _script_context(conn))
    assert source_recrawl_roi_script.main(["--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "source_recrawl_roi"
    assert payload["candidates"][0]["source_id"] == "a"
