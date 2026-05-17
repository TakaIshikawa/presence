"""Tests for generation review queue aging reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from evaluation.generation_review_queue_aging import (
    build_generation_review_queue_aging_report,
    build_generation_review_queue_aging_report_from_db,
    format_generation_review_queue_aging_text,
)


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "generation_review_queue_aging.py"
spec = importlib.util.spec_from_file_location("generation_review_queue_aging_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_empty_rows_return_structured_empty_result():
    report = build_generation_review_queue_aging_report([], now=NOW)

    assert report["totals"]["total_pending_items"] == 0
    assert report["totals"]["aged_pending_count"] == 0
    assert report["totals"]["oldest_pending_age_hours"] is None
    assert report["empty_state"]["is_empty"] is True
    assert report["aged_items"] == []


def test_aged_pending_items_include_reason_and_oldest_age():
    report = build_generation_review_queue_aging_report(
        [
            {"id": 1, "content_type": "blog_post", "status": "pending_review", "created_at": "2026-05-13T09:00:00+00:00"},
            {"id": 2, "content_type": "newsletter", "status": "approved", "created_at": "2026-05-12T00:00:00+00:00"},
            {"id": 3, "content_type": "thread", "status": "pending", "created_at": "2026-05-15T06:00:00+00:00"},
        ],
        max_age_hours=24,
        now=NOW,
    )

    assert report["totals"]["total_pending_items"] == 2
    assert report["totals"]["aged_pending_count"] == 1
    assert report["totals"]["oldest_pending_age_hours"] == 51
    assert report["aged_items"][0]["content_id"] == "1"
    assert "pending review for 51 hours" in report["aged_items"][0]["reason"]
    assert "Generation Review Queue Aging" in format_generation_review_queue_aging_text(report)


def test_configurable_age_threshold_changes_aged_count():
    rows = [{"id": "draft-1", "status": "pending", "created_at": "2026-05-15T00:00:00+00:00"}]

    assert build_generation_review_queue_aging_report(rows, max_age_hours=6, now=NOW)["totals"]["aged_pending_count"] == 1
    assert build_generation_review_queue_aging_report(rows, max_age_hours=24, now=NOW)["totals"]["aged_pending_count"] == 0


def test_missing_expected_tables_returns_empty_report():
    conn = sqlite3.connect(":memory:")

    report = build_generation_review_queue_aging_report_from_db(conn, now=NOW)

    assert report["totals"]["rows_scanned"] == 0
    assert report["empty_state"]["is_empty"] is True


def test_cli_supports_json_and_text_output(monkeypatch, capsys):
    rows = [{"id": 10, "status": "pending", "content_type": "blog", "created_at": "2026-05-13T00:00:00+00:00"}]
    monkeypatch.setattr(script, "script_context", lambda: _script_context(SimpleNamespace(conn=sqlite3.connect(":memory:"))))
    monkeypatch.setattr(
        script,
        "build_generation_review_queue_aging_report_from_db",
        lambda _db, **kwargs: build_generation_review_queue_aging_report(rows, now=NOW, **kwargs),
    )

    assert script.main(["--max-age-hours", "24", "--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "generation_review_queue_aging"
    assert payload["totals"]["aged_pending_count"] == 1

    assert script.main(["--format", "text"]) == 0
    assert "Generation Review Queue Aging" in capsys.readouterr().out
