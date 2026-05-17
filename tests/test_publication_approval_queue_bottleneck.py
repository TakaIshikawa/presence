"""Tests for publication approval queue bottleneck reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.publication_approval_queue_bottleneck import build_publication_approval_queue_bottleneck_report


NOW = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_approval_queue_bottleneck.py"
spec = importlib.util.spec_from_file_location("publication_approval_queue_bottleneck_script", SCRIPT_PATH)
publication_approval_queue_bottleneck_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(publication_approval_queue_bottleneck_script)


@contextmanager
def _script_context(conn: sqlite3.Connection):
    yield SimpleNamespace(), SimpleNamespace(conn=conn)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE publication_queue (content_id TEXT, content_type TEXT, review_state TEXT, entered_state_at TEXT, title TEXT)"
    )
    return conn


def test_empty_queue_returns_clean_report():
    report = build_publication_approval_queue_bottleneck_report(_conn(), now=NOW)
    assert report["groups"] == []
    assert report["totals"]["item_count"] == 0


def test_mixed_state_aggregation_and_oldest_item():
    conn = _conn()
    conn.executemany(
        "INSERT INTO publication_queue VALUES (?, ?, ?, ?, ?)",
        [
            ("post-1", "blog", "review", "2026-05-10T00:00:00+00:00", "Old"),
            ("post-2", "blog", "review", "2026-05-15T00:00:00+00:00", "New"),
            ("news-1", "newsletter", "approval", "2026-05-01T00:00:00+00:00", "Issue"),
            ("done-1", "blog", "published", "2026-05-01T00:00:00+00:00", "Done"),
        ],
    )
    report = build_publication_approval_queue_bottleneck_report(conn, now=NOW)
    assert report["totals"]["item_count"] == 3
    assert report["groups"][0]["oldest_item"]["content_id"] == "news-1"
    assert report["groups"][0]["severity"] == "critical"


def test_threshold_behavior_marks_warning_and_critical():
    conn = _conn()
    conn.executemany(
        "INSERT INTO publication_queue VALUES (?, ?, ?, ?, ?)",
        [
            ("post-1", "blog", "review", "2026-05-14T12:00:00+00:00", "Warn"),
            ("post-2", "blog", "approval", "2026-05-10T12:00:00+00:00", "Crit"),
        ],
    )
    report = build_publication_approval_queue_bottleneck_report(conn, warning_days=3, critical_days=7, now=NOW)
    severities = {group["oldest_item"]["content_id"]: group["severity"] for group in report["groups"]}
    assert severities["post-1"] == "warning"
    assert severities["post-2"] == "critical"


def test_cli_json_output(capsys, monkeypatch):
    conn = _conn()
    conn.execute(
        "INSERT INTO publication_queue VALUES (?, ?, ?, ?, ?)",
        ("post-1", "blog", "review", "2026-05-01T00:00:00+00:00", "Old"),
    )
    monkeypatch.setattr(publication_approval_queue_bottleneck_script, "script_context", lambda: _script_context(conn))
    assert publication_approval_queue_bottleneck_script.main(["--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "publication_approval_queue_bottleneck"
    assert payload["totals"]["item_count"] == 1
