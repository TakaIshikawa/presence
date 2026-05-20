"""Tests for publish queue hold reason freshness reporting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.publish_queue_hold_reason_freshness import (
    build_publish_queue_hold_reason_freshness_report,
    build_publish_queue_hold_reason_freshness_report_from_db,
    format_publish_queue_hold_reason_freshness_json,
    format_publish_queue_hold_reason_freshness_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publish_queue_hold_reason_freshness.py"
spec = importlib.util.spec_from_file_location("publish_queue_hold_reason_freshness_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _ts(days: int) -> str:
    return (NOW - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")


def test_builder_flags_missing_generic_and_stale_hold_reasons():
    report = build_publish_queue_hold_reason_freshness_report(
        [
            {"id": 1, "status": "held", "updated_at": _ts(10)},
            {"id": 2, "status": "blocked", "hold_reason": "tbd", "updated_at": _ts(1)},
            {"id": 3, "status": "needs_review", "hold_reason": "legal approval required", "updated_at": _ts(20)},
            {"id": 4, "status": "queued", "updated_at": _ts(100)},
        ],
        stale_days=7,
        now=NOW,
    )

    flags = {item["queue_id"]: item["reason_flags"] for item in report["stale_holds"]}
    assert "missing_reason" in flags[1]
    assert "generic_or_too_short_reason" in flags[2]
    assert "stale_hold" in flags[3]
    assert report["artifact_type"] == "publish_queue_hold_reason_freshness"
    assert report["age_buckets"]["8-30"] == 2


def test_metadata_reason_extraction_and_missing_schema_and_cli(tmp_path, capsys):
    empty = sqlite3.connect(":memory:")
    assert build_publish_queue_hold_reason_freshness_report_from_db(empty, now=NOW)["missing_schema"]["missing_tables"] == ["publish_queue"]

    db_path = tmp_path / "queue.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE publish_queue (id INTEGER PRIMARY KEY, status TEXT, metadata TEXT, updated_at TEXT)")
    conn.execute("INSERT INTO publish_queue VALUES (?, ?, ?, ?)", (1, "held", json.dumps({"hold_reason": "Needs editor review"}), _ts(30)))
    conn.commit()
    conn.close()

    assert script.main(["--db", str(db_path), "--format", "json", "--stale-days", "7"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["stale_holds"][0]["reason_source"] == "metadata.hold_reason"
    assert "stale_hold" in payload["stale_holds"][0]["reason_flags"]
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Publish Queue Hold Reason Freshness" in capsys.readouterr().out


def test_formatters_and_invalid_arguments():
    report = build_publish_queue_hold_reason_freshness_report([], now=NOW)
    assert json.loads(format_publish_queue_hold_reason_freshness_json(report))["artifact_type"] == "publish_queue_hold_reason_freshness"
    assert "Stale holds" in format_publish_queue_hold_reason_freshness_text(report)
    with pytest.raises(ValueError, match="stale_days must be positive"):
        build_publish_queue_hold_reason_freshness_report([], stale_days=0)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_publish_queue_hold_reason_freshness_report([], limit=0)
