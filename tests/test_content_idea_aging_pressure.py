"""Tests for content idea aging pressure reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.content_idea_aging_pressure import (
    build_content_idea_aging_pressure_report,
    format_content_idea_aging_pressure_csv,
    format_content_idea_aging_pressure_json,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "content_idea_aging_pressure.py"
)
spec = importlib.util.spec_from_file_location(
    "content_idea_aging_pressure_script",
    SCRIPT_PATH,
)
content_idea_aging_pressure_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(content_idea_aging_pressure_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _idea(
    db,
    *,
    note: str,
    topic: str | None = "workflow",
    priority: str = "normal",
    source: str | None = "unit",
    created_at: datetime,
    snoozed_until: datetime | None = None,
    status: str = "open",
) -> int:
    idea_id = db.add_content_idea(
        note=note,
        topic=topic,
        priority=priority,
        source=source,
        status=status,
    )
    # Update created_at and snoozed_until
    updates = {"created_at": created_at.isoformat()}
    if snoozed_until is not None:
        updates["snoozed_until"] = snoozed_until.isoformat()

    set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
    db.conn.execute(
        f"UPDATE content_ideas SET {set_clause} WHERE id = ?",
        (*updates.values(), idea_id),
    )
    db.conn.commit()
    return idea_id


def test_reports_open_ideas_with_pressure_scores(db):
    """Open ideas receive deterministic pressure scores based on age and priority."""
    high_old = _idea(
        db,
        note="High priority old idea",
        priority="high",
        created_at=NOW - timedelta(days=30),
    )
    normal_old = _idea(
        db,
        note="Normal priority old idea",
        priority="normal",
        created_at=NOW - timedelta(days=30),
    )
    low_old = _idea(
        db,
        note="Low priority old idea",
        priority="low",
        created_at=NOW - timedelta(days=30),
    )
    high_new = _idea(
        db,
        note="High priority new idea",
        priority="high",
        created_at=NOW - timedelta(days=5),
    )

    report = build_content_idea_aging_pressure_report(db, now=NOW)

    assert len(report.rows) == 4
    rows_by_id = {row.id: row for row in report.rows}

    # High priority: 3.0 * age_days
    assert rows_by_id[high_old].pressure_score == 3.0 * 30
    assert rows_by_id[high_new].pressure_score == 3.0 * 5

    # Normal priority: 1.0 * age_days
    assert rows_by_id[normal_old].pressure_score == 1.0 * 30

    # Low priority: 0.3 * age_days
    assert rows_by_id[low_old].pressure_score == 0.3 * 30

    # Rows should be sorted by pressure score descending
    assert report.rows[0].id == high_old  # 90.0
    assert report.rows[1].id == normal_old  # 30.0
    assert report.rows[2].id == high_new  # 15.0
    assert report.rows[3].id == low_old  # 9.0


def test_excludes_snoozed_by_default(db):
    """Snoozed ideas are excluded by default."""
    active = _idea(
        db,
        note="Active idea",
        created_at=NOW - timedelta(days=10),
    )
    snoozed = _idea(
        db,
        note="Snoozed idea",
        created_at=NOW - timedelta(days=10),
        snoozed_until=NOW + timedelta(days=7),
    )

    report = build_content_idea_aging_pressure_report(db, now=NOW)

    assert len(report.rows) == 1
    assert report.rows[0].id == active
    assert snoozed not in {row.id for row in report.rows}
    assert report.totals["active_count"] == 1
    assert report.totals["snoozed_count"] == 0


def test_includes_snoozed_when_requested(db):
    """Snoozed ideas are included when include_snoozed=True."""
    active = _idea(
        db,
        note="Active idea",
        created_at=NOW - timedelta(days=10),
    )
    snoozed = _idea(
        db,
        note="Snoozed idea",
        created_at=NOW - timedelta(days=10),
        snoozed_until=NOW + timedelta(days=7),
    )

    report = build_content_idea_aging_pressure_report(
        db,
        include_snoozed=True,
        now=NOW,
    )

    assert len(report.rows) == 2
    rows_by_id = {row.id: row for row in report.rows}
    assert rows_by_id[active].is_snoozed is False
    assert rows_by_id[snoozed].is_snoozed is True
    assert report.totals["active_count"] == 1
    assert report.totals["snoozed_count"] == 1


def test_excludes_promoted_and_dismissed_ideas(db):
    """Only open ideas are included in the report."""
    open_idea = _idea(
        db,
        note="Open idea",
        created_at=NOW - timedelta(days=10),
    )
    promoted = _idea(
        db,
        note="Promoted idea",
        created_at=NOW - timedelta(days=10),
        status="promoted",
    )
    dismissed = _idea(
        db,
        note="Dismissed idea",
        created_at=NOW - timedelta(days=10),
        status="dismissed",
    )

    report = build_content_idea_aging_pressure_report(db, now=NOW)

    assert len(report.rows) == 1
    assert report.rows[0].id == open_idea
    assert promoted not in {row.id for row in report.rows}
    assert dismissed not in {row.id for row in report.rows}


def test_filters_by_min_age_days(db):
    """Only ideas older than min_age_days are included."""
    old = _idea(
        db,
        note="Old idea",
        created_at=NOW - timedelta(days=20),
    )
    medium = _idea(
        db,
        note="Medium age idea",
        created_at=NOW - timedelta(days=10),
    )
    new = _idea(
        db,
        note="New idea",
        created_at=NOW - timedelta(days=5),
    )

    report = build_content_idea_aging_pressure_report(
        db,
        min_age_days=7,
        now=NOW,
    )

    assert len(report.rows) == 2
    assert {row.id for row in report.rows} == {old, medium}
    assert new not in {row.id for row in report.rows}


def test_filters_by_topic(db):
    """Only ideas matching the topic filter are included."""
    workflow = _idea(
        db,
        note="Workflow idea",
        topic="workflow",
        created_at=NOW - timedelta(days=10),
    )
    health = _idea(
        db,
        note="Health idea",
        topic="health",
        created_at=NOW - timedelta(days=10),
    )

    report = build_content_idea_aging_pressure_report(
        db,
        topic="workflow",
        now=NOW,
    )

    assert len(report.rows) == 1
    assert report.rows[0].id == workflow
    assert health not in {row.id for row in report.rows}


def test_grouping_by_topic_and_source(db):
    """Report includes grouped summaries by topic and source."""
    _idea(db, note="Workflow 1", topic="workflow", source="manual", created_at=NOW - timedelta(days=10))
    _idea(db, note="Workflow 2", topic="workflow", source="auto", created_at=NOW - timedelta(days=10))
    _idea(db, note="Health 1", topic="health", source="manual", created_at=NOW - timedelta(days=10))
    _idea(db, note="No topic", topic=None, source="manual", created_at=NOW - timedelta(days=10))
    _idea(db, note="No source", topic="workflow", source=None, created_at=NOW - timedelta(days=10))

    report = build_content_idea_aging_pressure_report(db, now=NOW)

    assert report.grouped_by_topic == {
        "workflow": 3,
        "health": 1,
        "(none)": 1,
    }
    assert report.grouped_by_source == {
        "manual": 3,
        "auto": 1,
        "(none)": 1,
    }


def test_json_output_format(db):
    """JSON output is valid and includes all required fields."""
    _idea(
        db,
        note="Test idea",
        priority="high",
        topic="workflow",
        source="manual",
        created_at=NOW - timedelta(days=10),
    )

    report = build_content_idea_aging_pressure_report(db, now=NOW)
    json_output = format_content_idea_aging_pressure_json(report)

    data = json.loads(json_output)
    assert data["artifact_type"] == "content_idea_aging_pressure"
    assert "generated_at" in data
    assert "filters" in data
    assert "totals" in data
    assert "rows" in data
    assert "grouped_by_topic" in data
    assert "grouped_by_source" in data

    assert len(data["rows"]) == 1
    row = data["rows"][0]
    assert row["age_days"] == 10
    assert row["priority"] == "high"
    assert row["pressure_score"] == 30.0
    assert row["is_snoozed"] is False


def test_csv_output_format(db):
    """CSV output includes headers and properly formatted rows."""
    _idea(
        db,
        note="Test idea with, comma",
        priority="high",
        topic="workflow",
        source="manual",
        created_at=NOW - timedelta(days=10),
    )

    report = build_content_idea_aging_pressure_report(db, now=NOW)
    csv_output = format_content_idea_aging_pressure_csv(report)

    lines = csv_output.split("\n")
    assert lines[0] == "id,age_days,priority,pressure_score,topic,source,is_snoozed,note_preview"
    assert len(lines) == 2  # header + 1 data row

    # CSV should properly escape commas
    assert '"Test idea with, comma"' in lines[1]


def test_empty_database(db):
    """Report handles empty database gracefully."""
    report = build_content_idea_aging_pressure_report(db, now=NOW)

    assert len(report.rows) == 0
    assert report.totals["idea_count"] == 0
    assert report.totals["active_count"] == 0
    assert report.totals["snoozed_count"] == 0
    assert report.grouped_by_topic == {}
    assert report.grouped_by_source == {}


def test_missing_content_ideas_table():
    """Report handles missing content_ideas table."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_content_idea_aging_pressure_report(conn, now=NOW)

    assert len(report.rows) == 0
    assert report.missing_tables == ("content_ideas",)


def test_script_json_output(db, monkeypatch):
    """Script produces valid JSON output."""
    monkeypatch.setattr(
        content_idea_aging_pressure_script,
        "script_context",
        lambda: _script_context(db),
    )

    _idea(
        db,
        note="Test idea",
        priority="high",
        created_at=NOW - timedelta(days=15),
    )

    exit_code = content_idea_aging_pressure_script.main(["--format", "json"])
    assert exit_code == 0


def test_script_csv_output(db, monkeypatch):
    """Script produces valid CSV output."""
    monkeypatch.setattr(
        content_idea_aging_pressure_script,
        "script_context",
        lambda: _script_context(db),
    )

    _idea(
        db,
        note="Test idea",
        priority="normal",
        created_at=NOW - timedelta(days=20),
    )

    exit_code = content_idea_aging_pressure_script.main(["--format", "csv"])
    assert exit_code == 0


def test_script_with_filters(db, monkeypatch):
    """Script applies filters correctly."""
    monkeypatch.setattr(
        content_idea_aging_pressure_script,
        "script_context",
        lambda: _script_context(db),
    )

    _idea(
        db,
        note="Old workflow idea",
        topic="workflow",
        created_at=NOW - timedelta(days=20),
    )
    _idea(
        db,
        note="New workflow idea",
        topic="workflow",
        created_at=NOW - timedelta(days=5),
    )
    _idea(
        db,
        note="Health idea",
        topic="health",
        created_at=NOW - timedelta(days=20),
    )

    exit_code = content_idea_aging_pressure_script.main([
        "--min-age-days", "10",
        "--topic", "workflow",
        "--format", "json",
    ])
    assert exit_code == 0


def test_script_with_snoozed_filter(db, monkeypatch):
    """Script includes snoozed ideas when requested."""
    monkeypatch.setattr(
        content_idea_aging_pressure_script,
        "script_context",
        lambda: _script_context(db),
    )

    _idea(
        db,
        note="Active idea",
        created_at=NOW - timedelta(days=10),
    )
    _idea(
        db,
        note="Snoozed idea",
        created_at=NOW - timedelta(days=10),
        snoozed_until=NOW + timedelta(days=7),
    )

    exit_code = content_idea_aging_pressure_script.main([
        "--include-snoozed",
        "--format", "json",
    ])
    assert exit_code == 0


def test_age_calculation_boundary_cases(db):
    """Age calculation handles boundary cases correctly."""
    same_day = _idea(
        db,
        note="Created same day",
        created_at=NOW - timedelta(hours=6),
    )
    just_under_day = _idea(
        db,
        note="Just under a day",
        created_at=NOW - timedelta(hours=23),
    )
    exactly_day = _idea(
        db,
        note="Exactly one day",
        created_at=NOW - timedelta(days=1),
    )

    report = build_content_idea_aging_pressure_report(db, now=NOW)
    rows_by_id = {row.id: row for row in report.rows}

    assert rows_by_id[same_day].age_days == 0
    assert rows_by_id[just_under_day].age_days == 0
    assert rows_by_id[exactly_day].age_days == 1
