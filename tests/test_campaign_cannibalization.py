"""Tests for campaign cannibalization reports."""

from __future__ import annotations

from contextlib import contextmanager
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

from synthesis.campaign_cannibalization import (
    build_campaign_cannibalization_report,
    export_to_json,
    format_text_report,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "campaign_cannibalization.py"
spec = importlib.util.spec_from_file_location("campaign_cannibalization_script", SCRIPT_PATH)
campaign_cannibalization_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(campaign_cannibalization_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _campaign(db, name: str, *, status: str = "active") -> int:
    return db.create_campaign(
        name=name,
        goal=f"{name} campaign",
        start_date="2026-05-01",
        end_date="2026-05-31",
        status=status,
    )


def test_groups_similar_planned_topics_across_active_campaigns_with_actions(db):
    launch_id = _campaign(db, "Launch")
    reliability_id = _campaign(db, "Reliability")
    inactive_id = _campaign(db, "Paused", status="paused")
    keep_id = db.insert_planned_topic(
        topic="Database migration rollback testing",
        angle="Preflight rollback checks before deploy",
        source_material="schema rollback fixtures",
        campaign_id=launch_id,
        target_date="2026-05-03",
    )
    defer_id = db.insert_planned_topic(
        topic="Database migration rollback tests",
        angle="Preflight rollback checks before release",
        source_material="schema rollback fixtures",
        campaign_id=reliability_id,
        target_date="2026-05-04",
    )
    db.insert_planned_topic(
        topic="Database migration rollback testing",
        angle="Preflight rollback checks before deploy",
        campaign_id=launch_id,
        target_date="2026-05-05",
    )
    db.insert_planned_topic(
        topic="Database migration rollback testing",
        angle="Preflight rollback checks before deploy",
        campaign_id=inactive_id,
        target_date="2026-05-06",
    )

    report = build_campaign_cannibalization_report(db)

    assert report.campaign_count == 2
    assert report.considered_topic_count == 3
    assert report.overlap_group_count == 1
    group = report.groups[0]
    assert group.planned_topic_ids == [keep_id, defer_id]
    assert group.similarity_score >= 0.72
    assert {"database", "migration", "rollback", "preflight"} <= set(group.shared_tokens)
    assert group.suggested_actions == {str(keep_id): "keep", str(defer_id): "defer"}
    assert [topic.suggested_action for topic in group.topics] == ["keep", "defer"]
    assert group.pairs[0].date_proximity > 0.9


def test_campaign_id_limits_groups_to_overlaps_involving_that_campaign(db):
    first_id = _campaign(db, "First")
    second_id = _campaign(db, "Second")
    third_id = _campaign(db, "Third")
    db.insert_planned_topic(
        topic="Launch readiness checklist",
        angle="Preflight checks before release",
        campaign_id=first_id,
    )
    db.insert_planned_topic(
        topic="Support readiness checklist",
        angle="Preflight checks before release",
        campaign_id=second_id,
    )
    third_topic = db.insert_planned_topic(
        topic="Incident review workflow",
        angle="Debug failed retries after alerts",
        campaign_id=third_id,
    )
    fourth_topic = db.insert_planned_topic(
        topic="Incident review workflows",
        angle="Debug failed retry alerts",
        campaign_id=second_id,
    )

    report = build_campaign_cannibalization_report(
        db,
        campaign_id=third_id,
        min_similarity=0.7,
    )

    assert report.campaign_id == third_id
    assert report.overlap_group_count == 1
    assert report.groups[0].planned_topic_ids == sorted([fourth_topic, third_topic])


def test_generated_topics_are_excluded_unless_requested(db):
    first_id = _campaign(db, "First")
    second_id = _campaign(db, "Second")
    db.insert_planned_topic(
        topic="Source freshness audit",
        angle="Find stale references before publishing",
        campaign_id=first_id,
    )
    generated_topic = db.insert_planned_topic(
        topic="Source freshness audits",
        angle="Find stale references before publishing",
        campaign_id=second_id,
        status="generated",
    )
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Generated post",
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE planned_topics SET content_id = ? WHERE id = ?",
        (content_id, generated_topic),
    )
    db.conn.commit()

    default_report = build_campaign_cannibalization_report(db)
    included_report = build_campaign_cannibalization_report(db, include_generated=True)

    assert default_report.overlap_group_count == 0
    assert default_report.considered_topic_count == 1
    assert included_report.overlap_group_count == 1
    assert included_report.considered_topic_count == 2
    assert included_report.groups[0].topics[1].content_id == content_id


def test_stable_when_no_campaigns_no_topics_and_missing_optional_columns(db):
    empty_report = build_campaign_cannibalization_report(db)

    assert empty_report.groups == []
    assert "No overlapping planned topics" in format_text_report(empty_report)

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE content_campaigns (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            status TEXT,
            start_date TEXT,
            created_at TEXT
        )"""
    )
    conn.execute(
        """CREATE TABLE planned_topics (
            id INTEGER PRIMARY KEY,
            campaign_id INTEGER,
            topic TEXT NOT NULL
        )"""
    )
    conn.execute(
        "INSERT INTO content_campaigns VALUES (1, 'A', 'active', '2026-05-01', '2026-05-01')"
    )
    conn.execute(
        "INSERT INTO content_campaigns VALUES (2, 'B', 'active', '2026-05-01', '2026-05-01')"
    )
    conn.execute("INSERT INTO planned_topics VALUES (1, 1, 'Retry workflow')")
    conn.execute("INSERT INTO planned_topics VALUES (2, 2, 'Retry workflows')")
    conn.commit()

    report = build_campaign_cannibalization_report(conn, min_similarity=0.5)

    assert report.considered_topic_count == 2
    assert report.overlap_group_count == 1
    assert report.groups[0].topics[0].angle is None
    assert report.groups[0].pairs[0].date_proximity == 0.0


def test_missing_required_tables_returns_stable_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_campaign_cannibalization_report(conn)
    payload = json.loads(export_to_json(report))

    assert report.overlap_group_count == 0
    assert payload["missing_required_tables"] == ["content_campaigns", "planned_topics"]
    assert list(payload.keys()) == sorted(payload.keys())


def test_cli_outputs_text_and_json(db, capsys):
    first_id = _campaign(db, "First")
    second_id = _campaign(db, "Second")
    db.insert_planned_topic(
        topic="Release checklist",
        angle="Preflight checks before launch",
        campaign_id=first_id,
    )
    db.insert_planned_topic(
        topic="Release checklists",
        angle="Preflight checks before launch",
        campaign_id=second_id,
    )

    with patch.object(
        campaign_cannibalization_script,
        "script_context",
        return_value=_script_context(db),
    ):
        exit_code = campaign_cannibalization_script.main(
            ["--min-similarity", "0.7", "--format", "json"]
        )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "campaign_cannibalization"
    assert payload["overlap_group_count"] == 1
    assert payload["groups"][0]["suggested_actions"]

    with patch.object(
        campaign_cannibalization_script,
        "script_context",
        return_value=_script_context(db),
    ):
        exit_code = campaign_cannibalization_script.main(["--campaign-id", str(first_id)])

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "Campaign Cannibalization" in output
    assert "keep" in output
    assert "defer" in output
