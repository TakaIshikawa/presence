"""Tests for content idea snooze wake-up reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.content_idea_snooze_wakeup import (
    build_content_idea_snooze_wakeup_report,
    format_content_idea_snooze_wakeup_json,
    format_content_idea_snooze_wakeup_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "content_idea_snooze_wakeup.py"
)
spec = importlib.util.spec_from_file_location(
    "content_idea_snooze_wakeup_script",
    SCRIPT_PATH,
)
content_idea_snooze_wakeup_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(content_idea_snooze_wakeup_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _idea(
    db,
    *,
    note: str,
    topic: str = "workflow",
    priority: str = "normal",
    source: str | None = "unit",
    snoozed_until: datetime | None = None,
    snooze_reason: str | None = "waiting for timing",
    status: str = "open",
) -> int:
    idea_id = db.add_content_idea(
        note=note,
        topic=topic,
        priority=priority,
        source=source,
        status=status,
    )
    if snoozed_until is not None:
        db.conn.execute(
            """UPDATE content_ideas
               SET snoozed_until = ?, snooze_reason = ?
               WHERE id = ?""",
            (snoozed_until.isoformat(), snooze_reason, idea_id),
        )
        db.conn.commit()
    return idea_id


def test_reports_only_open_snoozed_ideas_due_within_window(db):
    overdue = _idea(db, note="Overdue seed", snoozed_until=NOW - timedelta(days=1))
    due_today = _idea(
        db,
        note="Due later today",
        snoozed_until=NOW + timedelta(hours=2),
    )
    due_soon = _idea(db, note="Due soon", snoozed_until=NOW + timedelta(days=2))
    later = _idea(db, note="Due later", snoozed_until=NOW + timedelta(days=6))
    future = _idea(db, note="Future", snoozed_until=NOW + timedelta(days=30))
    closed = _idea(
        db,
        note="Closed",
        snoozed_until=NOW - timedelta(days=1),
        status="promoted",
    )
    unsnoozed = _idea(db, note="No snooze")

    report = build_content_idea_snooze_wakeup_report(db, days_ahead=7, now=NOW)

    assert [row.id for row in report.rows] == [overdue, due_today, due_soon, later]
    assert [row.bucket for row in report.rows] == [
        "overdue",
        "due_today",
        "due_soon",
        "later",
    ]
    assert future not in {row.id for row in report.rows}
    assert closed not in {row.id for row in report.rows}
    assert unsnoozed not in {row.id for row in report.rows}
    assert report.totals == {
        "idea_count": 4,
        "overdue": 1,
        "due_today": 1,
        "due_soon": 1,
        "later": 1,
    }


def test_overdue_toggle_excludes_expired_snoozes(db):
    overdue = _idea(db, note="Overdue", snoozed_until=NOW - timedelta(hours=1))
    upcoming = _idea(db, note="Upcoming", snoozed_until=NOW + timedelta(hours=1))

    report = build_content_idea_snooze_wakeup_report(
        db,
        days_ahead=1,
        include_overdue=False,
        now=NOW,
    )

    assert [row.id for row in report.rows] == [upcoming]
    assert overdue not in {row.id for row in report.rows}
    assert report.totals["overdue"] == 0


def test_priority_sorting_within_bucket_then_snoozed_until(db):
    low_early = _idea(
        db,
        note="Low early",
        priority="low",
        snoozed_until=NOW + timedelta(days=2),
    )
    high_late = _idea(
        db,
        note="High late",
        priority="high",
        snoozed_until=NOW + timedelta(days=3),
    )
    normal_mid = _idea(
        db,
        note="Normal mid",
        priority="normal",
        snoozed_until=NOW + timedelta(days=2, hours=12),
    )
    high_early = _idea(
        db,
        note="High early",
        priority="high",
        snoozed_until=NOW + timedelta(days=1),
    )

    report = build_content_idea_snooze_wakeup_report(db, days_ahead=3, now=NOW)

    assert [row.id for row in report.rows] == [
        high_early,
        high_late,
        normal_mid,
        low_early,
    ]


def test_invalid_days_ahead_raises_value_error(db):
    with pytest.raises(ValueError, match="days_ahead must be non-negative"):
        build_content_idea_snooze_wakeup_report(db, days_ahead=-1, now=NOW)


def test_null_snoozed_until_is_excluded(db):
    _idea(db, note="Unsnoozed open")

    report = build_content_idea_snooze_wakeup_report(db, days_ahead=7, now=NOW)

    assert report.rows == ()
    assert report.totals["idea_count"] == 0


def test_missing_content_ideas_schema_returns_structured_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_content_idea_snooze_wakeup_report(conn, now=NOW)
    payload = json.loads(format_content_idea_snooze_wakeup_json(report))

    assert report.rows == ()
    assert report.missing_tables == ("content_ideas",)
    assert report.totals["idea_count"] == 0
    assert payload["artifact_type"] == "content_idea_snooze_wakeup"
    assert payload["missing_tables"] == ["content_ideas"]


def test_text_output_includes_operational_fields_and_short_note(db):
    long_note = " ".join(["review"] * 40)
    _idea(
        db,
        note=long_note,
        topic="agents",
        priority="high",
        source="seed_gap_ideas",
        snoozed_until=NOW - timedelta(days=1),
        snooze_reason="launch timing",
    )

    text = format_content_idea_snooze_wakeup_text(
        build_content_idea_snooze_wakeup_report(db, days_ahead=7, now=NOW)
    )

    assert "Content Idea Snooze Wake-up" in text
    assert "Totals: ideas=1 overdue=1 due_today=0 due_soon=0 later=0" in text
    assert "id=1 bucket=overdue priority=high topic=agents" in text
    assert "snoozed_until=2026-04-30T12:00:00+00:00" in text
    assert "reason: launch timing" in text
    assert "note: review review review" in text
    assert "..." in text


def test_cli_outputs_text_with_overdue_flag(db, monkeypatch, capsys):
    _idea(db, note="CLI idea", snoozed_until=NOW - timedelta(days=1))
    monkeypatch.setattr(
        content_idea_snooze_wakeup_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        content_idea_snooze_wakeup_script,
        "build_content_idea_snooze_wakeup_report",
        lambda db, **kwargs: build_content_idea_snooze_wakeup_report(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    exit_code = content_idea_snooze_wakeup_script.main(
        ["--days-ahead", "7", "--include-overdue", "--format", "text"]
    )
    text = capsys.readouterr().out

    assert exit_code == 0
    assert "Content Idea Snooze Wake-up" in text
    assert "CLI idea" in text
    assert "include_overdue=True" in text
