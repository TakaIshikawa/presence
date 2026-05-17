"""Tests for content calendar spacing reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.content_calendar_spacing import (
    build_content_calendar_spacing_report,
    format_content_calendar_spacing_json,
    format_content_calendar_spacing_table,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_calendar_spacing.py"
spec = importlib.util.spec_from_file_location("content_calendar_spacing_script", SCRIPT_PATH)
content_calendar_spacing_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(content_calendar_spacing_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _publication(db, channel: str, when: datetime) -> None:
    content_id = db.insert_generated_content("x_post", [], [], "post", 8.0, "ok")
    db.conn.execute(
        """INSERT INTO content_publications (content_id, platform, status, published_at)
           VALUES (?, ?, 'published', ?)""",
        (content_id, channel, when.isoformat()),
    )
    db.conn.commit()


def test_flags_long_gaps_bursts_and_uneven_cadence(db):
    _publication(db, "x", NOW - timedelta(days=10))
    _publication(db, "x", NOW - timedelta(days=1, hours=2))
    _publication(db, "x", NOW - timedelta(days=1))
    _publication(db, "newsletter", NOW - timedelta(days=2))
    _publication(db, "newsletter", NOW - timedelta(days=1))

    report = build_content_calendar_spacing_report(
        db,
        lookback_days=30,
        long_gap_hours=72,
        burst_threshold=2,
        uneven_ratio=1.5,
        now=NOW,
    )
    rows = {row.channel: row.to_dict() for row in report.rows}

    assert rows["x"]["publication_count"] == 3
    assert rows["x"]["burst_day_count"] == 1
    assert rows["x"]["spacing_status"] == "long_gap,same_day_burst,uneven_cadence"
    assert rows["newsletter"]["spacing_status"] == "healthy"


def test_json_table_cli_and_sorting(db, monkeypatch, capsys):
    _publication(db, "b", NOW - timedelta(hours=4))
    _publication(db, "a", NOW - timedelta(hours=4))
    report = build_content_calendar_spacing_report(db, now=NOW)
    payload = json.loads(format_content_calendar_spacing_json(report))

    assert [row["channel"] for row in payload["rows"]] == ["a", "b"]
    assert "Content Calendar Spacing" in format_content_calendar_spacing_table(report)

    monkeypatch.setattr(content_calendar_spacing_script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        content_calendar_spacing_script,
        "build_content_calendar_spacing_report",
        lambda db, **kwargs: build_content_calendar_spacing_report(db, now=NOW, **kwargs),
    )
    assert content_calendar_spacing_script.main(["--format", "table", "--long-gap-hours", "12"]) == 0
    assert "channel | publication_count" in capsys.readouterr().out
