"""Tests for draft publish dropoff reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from evaluation.draft_publish_dropoff import build_draft_publish_dropoff_report, build_draft_publish_dropoff_report_from_db


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "draft_publish_dropoff.py"
spec = importlib.util.spec_from_file_location("draft_publish_dropoff_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_buckets_publish_stages_and_totals():
    report = build_draft_publish_dropoff_report(
        [
            {"id": "draft", "created_at": "2026-04-30T00:00:00+00:00"},
            {"id": "queued", "created_at": "2026-04-30T00:00:00+00:00", "status": "queued"},
            {"id": "failed", "created_at": "2026-04-30T00:00:00+00:00", "last_error": "boom"},
            {"id": "published", "created_at": "2026-04-30T00:00:00+00:00", "published_at": "2026-05-01T00:00:00+00:00"},
            {"id": "stale", "created_at": "2026-04-01T00:00:00+00:00"},
        ],
        stale_days=14,
        now=NOW,
    )

    assert report["totals"]["generated"] == 5
    assert report["totals"]["published"] == 1
    assert report["totals"]["queued"] == 1
    assert report["totals"]["failed"] == 1
    assert report["totals"]["stale_unpublished"] == 1
    assert report["totals"]["publish_rate"] == 0.2


def test_per_content_records_include_required_fields():
    report = build_draft_publish_dropoff_report([{"id": "c1", "content_type": "blog", "created_at": "2026-04-30T12:00:00+00:00"}], now=NOW)

    row = report["contents"][0]
    assert row["content_id"] == "c1"
    assert row["format"] == "blog"
    assert row["age_days"] == 1.0
    assert row["publish_stage"] == "draft"
    assert row["last_error"] is None


def test_db_adapter_merges_publication_and_queue_state():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE generated_content (id INTEGER PRIMARY KEY, content_type TEXT, created_at TEXT)")
    conn.execute("CREATE TABLE content_publications (content_id INTEGER, status TEXT, published_at TEXT)")
    conn.execute("CREATE TABLE publish_queue (content_id INTEGER, status TEXT, last_error TEXT)")
    conn.execute("INSERT INTO generated_content VALUES (1, 'blog', '2026-04-30T00:00:00+00:00')")
    conn.execute("INSERT INTO content_publications VALUES (1, 'published', '2026-05-01T00:00:00+00:00')")

    report = build_draft_publish_dropoff_report_from_db(conn, now=NOW)

    assert report["contents"][0]["publish_stage"] == "published"


def test_cli_and_invalid_stale_days(monkeypatch, capsys):
    monkeypatch.setattr(script, "script_context", lambda: _script_context(SimpleNamespace()))
    monkeypatch.setattr(
        script,
        "build_draft_publish_dropoff_report_from_db",
        lambda _db, **kwargs: build_draft_publish_dropoff_report([{"id": "c1"}], now=NOW, **kwargs),
    )

    assert script.main(["--stale-days", "5", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "draft_publish_dropoff"
    assert script.main(["--table"]) == 0
    assert "content_id | format" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        script.parse_args(["--stale-days", "0"])
