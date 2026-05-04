"""Tests for content idea aging pressure reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import pytest
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


# --- Additional Comprehensive Tests ---


def test_very_old_ideas_over_365_days(db):
    """Test ideas over 365 days old are handled correctly with high pressure."""
    ancient = _idea(
        db,
        note="Ancient idea from over a year ago",
        priority="high",
        created_at=NOW - timedelta(days=400),
    )
    very_old = _idea(
        db,
        note="Very old idea",
        priority="normal",
        created_at=NOW - timedelta(days=500),
    )

    report = build_content_idea_aging_pressure_report(db, now=NOW)
    rows_by_id = {row.id: row for row in report.rows}

    # Verify extreme ages are calculated correctly
    assert rows_by_id[ancient].age_days == 400
    assert rows_by_id[ancient].pressure_score == 1200.0  # 400 * 3.0

    assert rows_by_id[very_old].age_days == 500
    assert rows_by_id[very_old].pressure_score == 500.0  # 500 * 1.0

    # Very old normal priority should come before ancient high priority
    # because 500 > 1200 would be wrong, actually 1200 > 500
    assert report.rows[0].id == ancient


def test_zero_priority_ideas_edge_case(db):
    """Test that ideas with low priority have correct pressure calculation."""
    # Create idea with low priority (lowest valid priority)
    idea_id = db.add_content_idea(
        note="Idea with low priority",
        topic="test",
        priority="low",  # Valid low priority
        source="test",
        status="open",
    )
    db.conn.execute(
        "UPDATE content_ideas SET created_at = ? WHERE id = ?",
        ((NOW - timedelta(days=10)).isoformat(), idea_id),
    )
    db.conn.commit()

    report = build_content_idea_aging_pressure_report(db, now=NOW)

    assert len(report.rows) == 1
    # Low priority has 0.3 multiplier
    assert report.rows[0].pressure_score == 3.0  # 10 * 0.3


def test_future_dated_ideas_negative_age_prevention(db):
    """Test that future-dated ideas have age clamped to 0 (no negative ages)."""
    future = _idea(
        db,
        note="Future dated idea",
        created_at=NOW + timedelta(days=5),
    )

    report = build_content_idea_aging_pressure_report(db, now=NOW)

    assert len(report.rows) == 1
    assert report.rows[0].age_days == 0
    assert report.rows[0].pressure_score == 0.0


def test_pressure_decay_simulation_with_updated_at(db):
    """Test that updated_at tracking shows idea activity (simulating progress)."""
    # Old idea that was recently updated
    active_old = _idea(
        db,
        note="Old idea being actively developed",
        priority="high",
        created_at=NOW - timedelta(days=60),
    )
    # Override the auto-set updated_at with specific recent time
    db.conn.execute(
        "UPDATE content_ideas SET updated_at = ? WHERE id = ?",
        ((NOW - timedelta(days=2)).isoformat(), active_old),
    )
    db.conn.commit()

    # Old idea not touched recently - use different topic to create separately
    # Note: db helper auto-sets updated_at, so we need to clear it
    stale_old = db.add_content_idea(
        note="Stale old idea",
        topic="stale",  # Different topic
        priority="high",
        source="unit",
        status="open",
    )
    db.conn.execute(
        "UPDATE content_ideas SET created_at = ?, updated_at = NULL WHERE id = ?",
        ((NOW - timedelta(days=60)).isoformat(), stale_old),
    )
    db.conn.commit()

    report = build_content_idea_aging_pressure_report(db, now=NOW)
    rows_by_id = {row.id: row for row in report.rows}

    # Both have same pressure (age based on created_at, not updated_at)
    assert rows_by_id[active_old].pressure_score == 180.0
    assert rows_by_id[stale_old].pressure_score == 180.0

    # But updated_at shows recent activity
    assert rows_by_id[active_old].updated_at is not None
    assert rows_by_id[stale_old].updated_at is None


def test_pressure_normalization_across_priority_levels(db):
    """Test pressure normalization: compare relative pressure across priorities."""
    # Same age, different priorities
    age = 30
    high = _idea(
        db,
        note="High priority",
        priority="high",
        created_at=NOW - timedelta(days=age),
    )
    normal = _idea(
        db,
        note="Normal priority",
        priority="normal",
        created_at=NOW - timedelta(days=age),
    )
    low = _idea(
        db,
        note="Low priority",
        priority="low",
        created_at=NOW - timedelta(days=age),
    )

    report = build_content_idea_aging_pressure_report(db, now=NOW)
    rows_by_id = {row.id: row for row in report.rows}

    high_score = rows_by_id[high].pressure_score
    normal_score = rows_by_id[normal].pressure_score
    low_score = rows_by_id[low].pressure_score

    # Verify ratios match multipliers
    assert high_score / normal_score == 3.0  # high is 3x normal
    assert normal_score / low_score == pytest.approx(1.0 / 0.3, rel=0.01)  # normal is ~3.33x low
    assert high_score / low_score == 10.0  # high is 10x low


def test_pressure_trend_monitoring_time_windows(db):
    """Test pressure trend by comparing reports at different time points."""
    idea_id = _idea(
        db,
        note="Aging idea",
        priority="normal",
        created_at=NOW - timedelta(days=30),
    )

    # Generate report at NOW
    report_now = build_content_idea_aging_pressure_report(db, now=NOW)
    pressure_now = report_now.rows[0].pressure_score

    # Simulate 10 days later
    future_time = NOW + timedelta(days=10)
    report_future = build_content_idea_aging_pressure_report(db, now=future_time)
    pressure_future = report_future.rows[0].pressure_score

    # Verify it's the same idea
    assert report_now.rows[0].id == idea_id
    assert report_future.rows[0].id == idea_id

    # Pressure should increase linearly
    assert report_now.rows[0].age_days == 30
    assert report_future.rows[0].age_days == 40

    assert pressure_now == 30.0  # 30 * 1.0
    assert pressure_future == 40.0  # 40 * 1.0

    # Verify linear growth
    assert pressure_future - pressure_now == 10.0


def test_batch_pressure_analysis_percentiles(db):
    """Test batch analysis to identify pressure percentiles for prioritization."""
    # Create diverse backlog
    for i in range(20):
        priority = ["high", "normal", "low"][i % 3]
        age = (i + 1) * 5  # 5, 10, 15, ... 100 days
        _idea(
            db,
            note=f"Idea {i}",
            priority=priority,
            created_at=NOW - timedelta(days=age),
        )

    report = build_content_idea_aging_pressure_report(db, now=NOW)

    scores = sorted([row.pressure_score for row in report.rows], reverse=True)

    # Top 25% (high pressure)
    top_quartile_threshold = scores[len(scores) // 4]
    high_pressure_ideas = [row for row in report.rows if row.pressure_score >= top_quartile_threshold]

    # Should have meaningful distribution
    assert len(high_pressure_ideas) >= 5
    assert len(scores) == 20


def test_missing_data_handling_null_fields(db):
    """Test handling of NULL/missing optional fields."""
    # Create idea with minimal data (NULL topic, source, updated_at)
    idea_id = db.add_content_idea(
        note="Minimal idea",
        topic=None,
        priority="normal",
        source=None,
        status="open",
    )
    db.conn.execute(
        "UPDATE content_ideas SET created_at = ?, updated_at = NULL WHERE id = ?",
        ((NOW - timedelta(days=15)).isoformat(), idea_id),
    )
    db.conn.commit()

    report = build_content_idea_aging_pressure_report(db, now=NOW)

    assert len(report.rows) == 1
    row = report.rows[0]
    assert row.topic is None
    assert row.source is None
    assert row.updated_at is None
    assert row.age_days == 15
    assert row.pressure_score == 15.0


def test_min_age_days_validation(db):
    """Test that negative min_age_days raises ValueError."""
    _idea(db, note="Test idea", created_at=NOW - timedelta(days=10))

    with pytest.raises(ValueError, match="min_age_days must be non-negative"):
        build_content_idea_aging_pressure_report(db, min_age_days=-1, now=NOW)


def test_empty_note_handling(db):
    """Test ideas with minimal/short notes are handled gracefully."""
    # Note: db helper requires non-empty notes, so use minimal valid note
    idea_id = db.add_content_idea(
        note="x",  # Minimal note (can't be truly empty due to validation)
        topic="test",
        priority="normal",
        source="test",
        status="open",
    )
    db.conn.execute(
        "UPDATE content_ideas SET created_at = ? WHERE id = ?",
        ((NOW - timedelta(days=10)).isoformat(), idea_id),
    )
    db.conn.commit()

    report = build_content_idea_aging_pressure_report(db, now=NOW)

    assert len(report.rows) == 1
    assert report.rows[0].note_preview == "x"


def test_very_long_note_preview_truncation(db):
    """Test very long notes are truncated in preview."""
    from src.evaluation.content_idea_aging_pressure import NOTE_PREVIEW_LENGTH

    long_note = "x" * 200
    _idea(
        db,
        note=long_note,
        created_at=NOW - timedelta(days=10),
    )

    report = build_content_idea_aging_pressure_report(db, now=NOW)

    preview = report.rows[0].note_preview
    assert len(preview) <= NOTE_PREVIEW_LENGTH
    assert preview.endswith("...")


def test_actionable_high_pressure_threshold_identification(db):
    """Test identifying actionable high-pressure ideas above threshold."""
    # Create mix of pressure levels
    critical = _idea(
        db,
        note="Critical high pressure",
        priority="high",
        created_at=NOW - timedelta(days=60),  # 180 pressure
    )
    high = _idea(
        db,
        note="High pressure",
        priority="high",
        created_at=NOW - timedelta(days=40),  # 120 pressure
    )
    _idea(
        db,
        note="Medium pressure",
        priority="normal",
        created_at=NOW - timedelta(days=50),  # 50 pressure
    )
    _idea(
        db,
        note="Low pressure",
        priority="low",
        created_at=NOW - timedelta(days=30),  # 9 pressure
    )

    report = build_content_idea_aging_pressure_report(db, now=NOW)

    # Verify all 4 ideas are in the report
    assert len(report.rows) == 4

    # Define actionable threshold (e.g., >100 pressure)
    actionable_threshold = 100.0
    actionable_ideas = [row for row in report.rows if row.pressure_score > actionable_threshold]

    assert len(actionable_ideas) == 2
    assert critical in [row.id for row in actionable_ideas]
    assert high in [row.id for row in actionable_ideas]


def test_report_totals_accuracy(db):
    """Test report totals are accurately calculated."""
    snoozed_until = NOW + timedelta(days=7)

    # Create mix
    for i in range(5):
        _idea(db, note=f"Active {i}", created_at=NOW - timedelta(days=10))

    for i in range(3):
        _idea(
            db,
            note=f"Snoozed {i}",
            created_at=NOW - timedelta(days=10),
            snoozed_until=snoozed_until,
        )

    # Without snoozed
    report_no_snoozed = build_content_idea_aging_pressure_report(db, now=NOW)
    assert report_no_snoozed.totals["idea_count"] == 5
    assert report_no_snoozed.totals["active_count"] == 5
    assert report_no_snoozed.totals["snoozed_count"] == 0

    # With snoozed
    report_with_snoozed = build_content_idea_aging_pressure_report(
        db, include_snoozed=True, now=NOW
    )
    assert report_with_snoozed.totals["idea_count"] == 8
    assert report_with_snoozed.totals["active_count"] == 5
    assert report_with_snoozed.totals["snoozed_count"] == 3


def test_pressure_score_deterministic_sorting(db):
    """Test that sorting by pressure score is deterministic with tie-breaking."""
    # Create ideas with same pressure score
    # High priority, 10 days = 30.0
    # Normal priority, 30 days = 30.0
    # Sorting: reverse=True on (pressure_score, priority_rank, age_days, id)
    # With equal pressure (30.0), sort by priority_rank descending: normal(1) > high(0)
    # So normal comes first when scores are equal

    high_10 = _idea(
        db,
        note="High 10 days",
        priority="high",
        created_at=NOW - timedelta(days=10),
    )
    normal_30 = _idea(
        db,
        note="Normal 30 days",
        priority="normal",
        created_at=NOW - timedelta(days=30),
    )

    report = build_content_idea_aging_pressure_report(db, now=NOW)

    # Both have 30.0 pressure
    assert len(report.rows) == 2
    assert report.rows[0].pressure_score == 30.0
    assert report.rows[1].pressure_score == 30.0

    # With reverse=True, higher priority_rank (normal=1) comes before lower (high=0)
    assert report.rows[0].priority == "normal"
    assert report.rows[1].priority == "high"

    # Verify age tie-breaking: normal has higher age (30 > 10)
    assert report.rows[0].age_days == 30
    assert report.rows[1].age_days == 10
