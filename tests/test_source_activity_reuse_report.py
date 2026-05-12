"""Tests for source activity reuse reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from evaluation.source_activity_reuse import (
    build_source_activity_reuse_report,
    format_source_activity_reuse_json,
    format_source_activity_reuse_text,
)


NOW = datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "source_activity_reuse.py"
spec = importlib.util.spec_from_file_location("source_activity_reuse_script", SCRIPT_PATH)
source_activity_reuse_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(source_activity_reuse_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, source_activity_ids, *, created_at: str = "2026-05-12T12:00:00+00:00") -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        source_activity_ids=source_activity_ids,
        content="Generated post",
        eval_score=8,
        eval_feedback="ok",
    )
    db.conn.execute("UPDATE generated_content SET created_at = ? WHERE id = ?", (created_at, content_id))
    db.conn.commit()
    return content_id


def test_handles_empty_null_and_malformed_source_activity_ids(db):
    _content(db, None)
    _content(db, "")
    malformed = _content(db, "not-json")
    _content(db, {"bad": "shape"})

    report = build_source_activity_reuse_report(db, now=NOW)

    assert report.rows == ()
    assert {row["content_id"] for row in report.malformed_rows} == {malformed, malformed + 1}
    assert report.totals["malformed_rows"] == 2


def test_flags_warning_above_warning_threshold(db):
    ids = [_content(db, ["42"]) for _ in range(3)]

    report = build_source_activity_reuse_report(
        db,
        warning_threshold=3,
        critical_threshold=5,
        now=NOW,
    )

    assert len(report.rows) == 1
    row = report.rows[0]
    assert row.source_activity_id == "42"
    assert row.reuse_count == 3
    assert row.content_ids == tuple(ids)
    assert row.severity == "warning"


def test_flags_critical_above_critical_threshold(db):
    for _ in range(5):
        _content(db, ["99"])
    for _ in range(3):
        _content(db, ["42"])

    report = build_source_activity_reuse_report(
        db,
        warning_threshold=3,
        critical_threshold=5,
        now=NOW,
    )

    by_id = {row.source_activity_id: row for row in report.rows}
    assert by_id["99"].severity == "critical"
    assert by_id["42"].severity == "warning"
    assert [row.source_activity_id for row in report.rows] == ["99", "42"]


def test_lookback_window_filters_old_generated_content(db):
    _content(db, ["old"], created_at="2026-01-01T12:00:00+00:00")
    _content(db, ["old"], created_at="2026-01-02T12:00:00+00:00")
    _content(db, ["old"], created_at="2026-05-12T12:00:00+00:00")

    report = build_source_activity_reuse_report(
        db,
        days=30,
        warning_threshold=2,
        critical_threshold=4,
        now=NOW,
    )

    assert report.rows == ()


def test_json_text_and_cli_output(db, capsys, monkeypatch):
    _content(db, ["7"])
    _content(db, ["7"])
    report = build_source_activity_reuse_report(
        db,
        warning_threshold=2,
        critical_threshold=4,
        now=NOW,
    )

    payload = json.loads(format_source_activity_reuse_json(report))
    text = format_source_activity_reuse_text(report)
    assert payload["artifact_type"] == "source_activity_reuse"
    assert "activity=7" in text

    monkeypatch.setattr(source_activity_reuse_script, "script_context", lambda: _script_context(db))
    assert source_activity_reuse_script.main(
        ["--format", "json", "--warning-threshold", "2", "--critical-threshold", "4"]
    ) == 0
    assert json.loads(capsys.readouterr().out)["rows"][0]["source_activity_id"] == "7"


def test_invalid_thresholds_raise(db):
    with pytest.raises(ValueError, match="critical_threshold"):
        build_source_activity_reuse_report(db, warning_threshold=5, critical_threshold=3)
