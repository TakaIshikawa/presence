"""Tests for planned-topic collision reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.planned_topic_collisions import (
    build_planned_topic_collision_report,
    format_planned_topic_collisions_json,
    format_planned_topic_collisions_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "planned_topic_collisions.py"
spec = importlib.util.spec_from_file_location("planned_topic_collisions_script", SCRIPT_PATH)
planned_topic_collisions_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(planned_topic_collisions_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, text: str = "Generated campaign content") -> int:
    return db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=8.0,
        eval_feedback="usable",
    )


def _link_topic(db, topic_id: int, content_id: int) -> None:
    db.conn.execute(
        "UPDATE planned_topics SET content_id = ?, status = 'generated' WHERE id = ?",
        (content_id, topic_id),
    )
    db.conn.commit()


def test_groups_duplicate_topic_dates_and_shared_content_ids(db):
    campaign_id = db.create_campaign(
        name="Collision Arc",
        start_date="2026-05-01",
        end_date="2026-05-10",
        status="active",
    )
    duplicate_a = db.insert_planned_topic(
        "Launch proof",
        target_date="2026-05-03",
        campaign_id=campaign_id,
    )
    duplicate_b = db.insert_planned_topic(
        "  launch   proof  ",
        target_date="2026-05-03T09:00:00+00:00",
        campaign_id=campaign_id,
    )
    shared_a = db.insert_planned_topic("case study", target_date="2026-05-04", campaign_id=campaign_id)
    shared_b = db.insert_planned_topic("demo recap", target_date="2026-05-05", campaign_id=campaign_id)
    content_id = _content(db)
    _link_topic(db, shared_a, content_id)
    _link_topic(db, shared_b, content_id)

    report = build_planned_topic_collision_report(db, days=7, now=NOW)
    payload = json.loads(format_planned_topic_collisions_json(report))
    text = format_planned_topic_collisions_text(report)

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "planned_topic_collisions"
    assert payload["has_issues"] is True
    assert payload["totals"]["topics_scanned"] == 4
    assert payload["totals"]["duplicate_topic_date_collisions"] == 1
    assert payload["totals"]["content_id_collisions"] == 1

    duplicate = _finding(payload, "duplicate_topic_date")
    assert duplicate["planned_topic_ids"] == [duplicate_a, duplicate_b]
    assert duplicate["campaign_id"] == campaign_id
    assert duplicate["target_date"] == "2026-05-03"
    assert duplicate["recommended_action"] == "merge_or_reschedule_duplicate_topics"

    shared = _finding(payload, "shared_content_id")
    assert shared["planned_topic_ids"] == [shared_a, shared_b]
    assert shared["content_id"] == content_id
    assert shared["recommended_action"] == "split_planned_topics_or_keep_single_owner"
    assert "Planned Topic Collisions" in text
    assert "type=duplicate_topic_date" in text
    assert "type=shared_content_id" in text


def test_reports_inactive_campaigns_and_out_of_window_topics(db):
    paused_id = db.create_campaign(
        name="Paused Arc",
        start_date="2026-05-01",
        end_date="2026-05-10",
        status="paused",
    )
    active_id = db.create_campaign(
        name="Window Arc",
        start_date="2026-05-01",
        end_date="2026-05-05",
        status="active",
    )
    paused_topic = db.insert_planned_topic(
        "pause cleanup",
        target_date="2026-05-03",
        campaign_id=paused_id,
    )
    outside_topic = db.insert_planned_topic(
        "late proof",
        target_date="2026-05-09",
        campaign_id=active_id,
    )

    report = build_planned_topic_collision_report(db, days=7, now=NOW)
    payload = report.to_dict()

    inactive = _finding(payload, "inactive_campaign")
    assert inactive["planned_topic_ids"] == [paused_topic]
    assert inactive["campaign_id"] == paused_id
    assert inactive["campaign_status"] == "paused"
    assert inactive["target_date"] == "2026-05-03"
    assert inactive["recommended_action"] == "move_topic_to_active_campaign_or_mark_skipped"

    outside = _finding(payload, "target_date_outside_campaign_window")
    assert outside["planned_topic_ids"] == [outside_topic]
    assert outside["campaign_id"] == active_id
    assert outside["campaign_status"] == "active"
    assert outside["target_date"] == "2026-05-09"
    assert outside["campaign_end_date"] == "2026-05-05"
    assert outside["recommended_action"] == "reschedule_topic_within_campaign_window_or_move_campaign"


def test_missing_campaign_rows_do_not_crash_and_are_counted(db):
    topic_id = db.insert_planned_topic(
        "orphaned topic",
        target_date="2026-05-03",
        campaign_id=999,
    )

    report = build_planned_topic_collision_report(db, days=7, now=NOW)

    assert report.totals["topics_scanned"] == 1
    assert report.totals["orphaned_campaign_topics"] == 1
    assert report.findings == ()
    assert topic_id > 0


def test_missing_tables_return_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_planned_topic_collision_report(conn, now=NOW)
    text = format_planned_topic_collisions_text(report)

    assert report.missing_tables == ("planned_topics", "content_campaigns")
    assert report.totals["topics_scanned"] == 0
    assert report.findings == ()
    assert "Missing tables: planned_topics, content_campaigns" in text


def test_cli_json_validation_and_fail_on_issues(db, monkeypatch, capsys):
    campaign_id = db.create_campaign(
        name="CLI Arc",
        start_date="2026-05-01",
        end_date="2026-05-10",
        status="active",
    )
    db.insert_planned_topic("cli topic", target_date="2026-05-03", campaign_id=campaign_id)
    db.insert_planned_topic("CLI Topic", target_date="2026-05-03", campaign_id=campaign_id)
    monkeypatch.setattr(
        planned_topic_collisions_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        planned_topic_collisions_script,
        "build_planned_topic_collision_report",
        lambda db, **kwargs: build_planned_topic_collision_report(db, now=NOW, **kwargs),
    )

    assert planned_topic_collisions_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err

    exit_code = planned_topic_collisions_script.main(
        ["--days", "7", "--campaign-id", str(campaign_id), "--format", "json"]
    )
    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["filters"]["campaign_id"] == campaign_id
    assert payload["totals"]["findings"] == 1

    assert planned_topic_collisions_script.main(["--fail-on-issues"]) == 1
    assert "type=duplicate_topic_date" in capsys.readouterr().out


def _finding(payload: dict, finding_type: str) -> dict:
    matches = [
        finding
        for finding in payload["findings"]
        if finding["finding_type"] == finding_type
    ]
    assert len(matches) == 1
    return matches[0]
