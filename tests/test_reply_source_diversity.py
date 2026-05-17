"""Tests for reply source diversity reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from engagement.reply_source_diversity import build_reply_source_diversity_report


NOW = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_source_diversity.py"
spec = importlib.util.spec_from_file_location("reply_source_diversity_script", SCRIPT_PATH)
reply_source_diversity_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(reply_source_diversity_script)


@contextmanager
def _script_context(conn: sqlite3.Connection):
    yield SimpleNamespace(), SimpleNamespace(conn=conn)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE reply_draft_sources (reply_id TEXT, source_id TEXT, url TEXT, drafted_at TEXT)")
    return conn


def test_computes_source_and_domain_concentration():
    conn = _conn()
    conn.executemany(
        "INSERT INTO reply_draft_sources VALUES (?, ?, ?, ?)",
        [
            ("r1", "src-a", "https://same.example/a", "2026-05-18T00:00:00+00:00"),
            ("r2", "src-a", "https://same.example/b", "2026-05-18T00:00:00+00:00"),
            ("r3", "src-a", "https://same.example/c", "2026-05-18T00:00:00+00:00"),
            ("r4", "src-b", "https://other.example/d", "2026-05-18T00:00:00+00:00"),
        ],
    )
    report = build_reply_source_diversity_report(conn, days=7, concentration_threshold=0.6, now=NOW)
    values = {(item["dimension"], item["value"]) for item in report["findings"]}
    assert ("source", "src-a") in values
    assert ("domain", "same.example") in values
    assert report["findings"][0]["affected_reply_ids"] == ["r1", "r2", "r3"]


def test_recent_window_filters_old_sources():
    conn = _conn()
    conn.executemany(
        "INSERT INTO reply_draft_sources VALUES (?, ?, ?, ?)",
        [
            ("old", "src-a", "https://same.example/a", "2026-04-01T00:00:00+00:00"),
            ("new", "src-b", "https://other.example/a", "2026-05-18T00:00:00+00:00"),
        ],
    )
    report = build_reply_source_diversity_report(conn, days=7, concentration_threshold=0.9, now=NOW)
    assert report["totals"]["reply_count"] == 1
    assert report["findings"][0]["value"] in {"src-b", "other.example"}


def test_replies_without_sources_do_not_crash():
    conn = _conn()
    conn.execute("INSERT INTO reply_draft_sources VALUES (?, ?, ?, ?)", ("r1", "", "", "2026-05-18T00:00:00+00:00"))
    report = build_reply_source_diversity_report(conn, now=NOW)
    assert report["findings"] == []


def test_cli_json_output(capsys, monkeypatch):
    conn = _conn()
    conn.execute(
        "INSERT INTO reply_draft_sources VALUES (?, ?, ?, ?)",
        ("r1", "src-a", "https://same.example/a", "2026-05-18T00:00:00+00:00"),
    )
    monkeypatch.setattr(reply_source_diversity_script, "script_context", lambda: _script_context(conn))
    assert reply_source_diversity_script.main(["--format", "json", "--threshold", "1"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "reply_source_diversity"
    assert payload["totals"]["reply_count"] == 1
