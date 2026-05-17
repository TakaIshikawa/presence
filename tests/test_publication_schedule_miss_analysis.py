"""Tests for publication schedule miss analysis."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from evaluation.publication_schedule_miss_analysis import build_publication_schedule_miss_analysis_report_from_db


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_schedule_miss_analysis.py"
spec = importlib.util.spec_from_file_location("publication_schedule_miss_analysis_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_detects_missing_late_and_unpublished(monkeypatch, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE generated_content (id INTEGER, content_type TEXT, scheduled_at TEXT, published_at TEXT)")
    conn.execute("CREATE TABLE publication_attempts (content_id INTEGER, attempted_at TEXT, published_at TEXT, status TEXT)")
    rows = [
        (1, "post", (NOW - timedelta(hours=4)).isoformat(), None),
        (2, "post", (NOW - timedelta(hours=3)).isoformat(), None),
        (3, "blog", (NOW - timedelta(hours=3)).isoformat(), (NOW - timedelta(hours=1)).isoformat()),
        (4, "post", (NOW - timedelta(minutes=30)).isoformat(), (NOW - timedelta(minutes=20)).isoformat()),
    ]
    conn.executemany("INSERT INTO generated_content VALUES (?, ?, ?, ?)", rows)
    conn.executemany(
        "INSERT INTO publication_attempts VALUES (?, ?, ?, ?)",
        [
            (2, (NOW - timedelta(hours=1)).isoformat(), None, "failed"),
            (3, (NOW - timedelta(hours=2, minutes=30)).isoformat(), (NOW - timedelta(hours=1)).isoformat(), "published"),
        ],
    )
    db = SimpleNamespace(conn=conn)

    report = build_publication_schedule_miss_analysis_report_from_db(db, now=NOW, window_minutes=60)

    reasons = {item["content_id"]: item["reason_code"] for item in report["findings"]}
    assert reasons["1"] == "missing_attempt"
    assert reasons["2"] == "late_attempt"
    assert reasons["3"] == "late_publication"
    assert report["summary"]["on_time"] == 1

    monkeypatch.setattr(script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        script,
        "build_publication_schedule_miss_analysis_report_from_db",
        lambda db, **kwargs: build_publication_schedule_miss_analysis_report_from_db(db, now=NOW, **kwargs),
    )
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "publication_schedule_miss_analysis"
    assert script.main(["--table"]) == 0
    assert "reason=missing_attempt" in capsys.readouterr().out
