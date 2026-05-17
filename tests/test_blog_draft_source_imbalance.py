"""Tests for blog draft source imbalance reporting."""

from __future__ import annotations

from contextlib import contextmanager
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.blog_draft_source_imbalance import build_blog_draft_source_imbalance_report


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "blog_draft_source_imbalance.py"
spec = importlib.util.spec_from_file_location("blog_draft_source_imbalance_script", SCRIPT_PATH)
blog_draft_source_imbalance_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(blog_draft_source_imbalance_script)


@contextmanager
def _script_context(conn: sqlite3.Connection):
    yield SimpleNamespace(), SimpleNamespace(conn=conn)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE blog_draft_sources (draft_id TEXT, source_id TEXT, author TEXT, url TEXT)")
    return conn


def test_concentrated_draft_is_reported_with_dominant_details():
    conn = _conn()
    conn.executemany(
        "INSERT INTO blog_draft_sources VALUES (?, ?, ?, ?)",
        [
            ("draft-1", "a", "Ann", "https://same.example/1"),
            ("draft-1", "a", "Ann", "https://same.example/2"),
            ("draft-1", "a", "Ann", "https://same.example/3"),
            ("draft-1", "b", "Bo", "https://other.example/1"),
        ],
    )
    report = build_blog_draft_source_imbalance_report(conn, concentration_threshold=0.6)
    finding = report["findings"][0]
    assert finding["draft_id"] == "draft-1"
    assert finding["dominant_value"] in {"a", "Ann", "same.example"}
    assert finding["max_concentration"] == 0.75
    assert finding["severity"] == "medium"


def test_balanced_draft_is_not_reported():
    conn = _conn()
    conn.executemany(
        "INSERT INTO blog_draft_sources VALUES (?, ?, ?, ?)",
        [
            ("draft-1", "a", "Ann", "https://a.example/1"),
            ("draft-1", "b", "Bo", "https://b.example/1"),
            ("draft-1", "c", "Cy", "https://c.example/1"),
        ],
    )
    assert build_blog_draft_source_imbalance_report(conn, concentration_threshold=0.6)["findings"] == []


def test_threshold_controls_risk_classification():
    conn = _conn()
    conn.executemany(
        "INSERT INTO blog_draft_sources VALUES (?, ?, ?, ?)",
        [
            ("draft-1", "a", "Ann", "https://a.example/1"),
            ("draft-1", "a", "Ann", "https://a.example/2"),
            ("draft-1", "b", "Bo", "https://b.example/1"),
        ],
    )
    assert build_blog_draft_source_imbalance_report(conn, concentration_threshold=0.7)["findings"] == []
    assert build_blog_draft_source_imbalance_report(conn, concentration_threshold=0.6)["findings"]


def test_cli_json_output_and_empty_queue(capsys, monkeypatch):
    conn = _conn()
    monkeypatch.setattr(blog_draft_source_imbalance_script, "script_context", lambda: _script_context(conn))
    assert blog_draft_source_imbalance_script.main(["--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "blog_draft_source_imbalance"
    assert payload["findings"] == []
