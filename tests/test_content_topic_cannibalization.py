"""Tests for content topic cannibalization."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from evaluation.content_topic_cannibalization import build_content_topic_cannibalization_report_from_db


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_topic_cannibalization.py"
spec = importlib.util.spec_from_file_location("content_topic_cannibalization_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_groups_recent_overlapping_topics(monkeypatch, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE generated_content (id INTEGER, content_type TEXT, status TEXT, title TEXT, body TEXT, metadata TEXT, created_at TEXT)"
    )
    conn.executemany(
        "INSERT INTO generated_content VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (1, "blog", "published", "Prompt eval reliability", "Agent prompt eval reliability", json.dumps({"tags": ["eval"]}), NOW.isoformat()),
            (2, "blog", "draft", "Prompt eval guide", "Reliability prompt eval checklist", json.dumps({"topic": "eval"}), NOW.isoformat()),
            (3, "post", "draft", "Different launch", "No overlap here", "{}", NOW.isoformat()),
        ],
    )
    db = SimpleNamespace(conn=conn)

    report = build_content_topic_cannibalization_report_from_db(db, now=NOW, min_overlap_score=0.2)

    assert report["summary"]["content_scanned"] == 3
    assert report["findings"]
    assert {"1", "2"}.issubset(set(report["findings"][0]["content_ids"]))
    assert report["findings"][0]["reason_code"] == "same_channel_topic_overlap"

    filtered = build_content_topic_cannibalization_report_from_db(db, now=NOW, content_type="post")
    assert filtered["findings"] == []

    monkeypatch.setattr(script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        script,
        "build_content_topic_cannibalization_report_from_db",
        lambda db, **kwargs: build_content_topic_cannibalization_report_from_db(db, now=NOW, **kwargs),
    )
    assert script.main(["--format", "json", "--min-overlap-score", "0.2"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "content_topic_cannibalization"
    assert script.main(["--table", "--min-overlap-score", "0.2"]) == 0
    assert "topic=" in capsys.readouterr().out
