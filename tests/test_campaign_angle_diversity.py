"""Tests for campaign planned-topic angle diversity reports."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from synthesis.campaign_angle_diversity import (
    build_campaign_angle_diversity_report,
    format_campaign_angle_diversity_json,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "campaign_angle_diversity.py"
spec = importlib.util.spec_from_file_location("campaign_angle_diversity_cli", SCRIPT_PATH)
campaign_angle_diversity_cli = importlib.util.module_from_spec(spec)
spec.loader.exec_module(campaign_angle_diversity_cli)

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


def _campaign(db, name: str = "Launch", *, status: str = "active") -> int:
    return db.create_campaign(
        name=name,
        goal="Ship a focused launch sequence",
        start_date="2026-05-01",
        end_date="2026-05-31",
        status=status,
    )


def test_detects_duplicate_groups_with_scores_tokens_and_action(db):
    campaign_id = _campaign(db)
    first = db.insert_planned_topic(
        topic="Database migration testing",
        angle="How to test rollback migrations before deploy",
        campaign_id=campaign_id,
        target_date="2026-05-03",
    )
    second = db.insert_planned_topic(
        topic="Database migration testing",
        angle="How to test database migration rollbacks before release",
        campaign_id=campaign_id,
        target_date="2026-05-04",
    )
    db.insert_planned_topic(
        topic="Launch notes",
        angle="Turn changelogs into customer-ready release notes",
        campaign_id=campaign_id,
        target_date="2026-05-05",
    )

    report = build_campaign_angle_diversity_report(db, campaign_id=campaign_id, now=NOW)

    assert report["summary"]["duplicate_group_count"] == 1
    group = report["duplicate_groups"][0]
    assert group["planned_topic_ids"] == [first, second]
    assert group["similarity_score"] >= 0.72
    assert {"database", "migration", "testing", "rollback"} <= set(group["shared_tokens"])
    assert group["recommended_action"] == "rewrite_angle"
    assert group["pairs"][0]["sequence_similarity"] > 0
    assert group["pairs"][0]["token_overlap"] > 0


def test_status_filtering_limits_considered_topics(db):
    campaign_id = _campaign(db)
    planned = db.insert_planned_topic(
        topic="API release checklist",
        angle="Preflight checks before public launch",
        campaign_id=campaign_id,
        status="planned",
    )
    db.insert_planned_topic(
        topic="API release checklist",
        angle="Preflight checks before public launch",
        campaign_id=campaign_id,
        status="generated",
    )

    planned_only = build_campaign_angle_diversity_report(
        db,
        campaign_id=campaign_id,
        statuses=["planned"],
        now=NOW,
    )
    all_selected = build_campaign_angle_diversity_report(
        db,
        campaign_id=campaign_id,
        statuses=["planned", "generated"],
        now=NOW,
    )

    assert planned_only["summary"]["considered_topic_count"] == 1
    assert planned_only["duplicate_groups"] == []
    assert all_selected["duplicate_groups"][0]["planned_topic_ids"][0] == planned
    assert all_selected["summary"]["duplicate_group_count"] == 1


def test_defaults_to_all_active_campaigns(db):
    active_id = _campaign(db, "Active", status="active")
    planned_id = _campaign(db, "Planned", status="planned")
    active_first = db.insert_planned_topic(
        topic="Retry workflow",
        angle="Debug flaky retry workers",
        campaign_id=active_id,
    )
    active_second = db.insert_planned_topic(
        topic="Retry workflow",
        angle="Debug flaky retry worker failures",
        campaign_id=active_id,
    )
    db.insert_planned_topic(
        topic="Skipped campaign duplicate",
        angle="This duplicate should not appear",
        campaign_id=planned_id,
    )
    db.insert_planned_topic(
        topic="Skipped campaign duplicate",
        angle="This duplicate should not appear",
        campaign_id=planned_id,
    )

    report = build_campaign_angle_diversity_report(db, now=NOW)

    assert report["summary"]["campaign_count"] == 1
    assert report["duplicate_groups"][0]["campaign_id"] == active_id
    assert report["duplicate_groups"][0]["planned_topic_ids"] == [active_first, active_second]


def test_threshold_behavior_and_healthy_summary(db):
    campaign_id = _campaign(db)
    db.insert_planned_topic(
        topic="Schema rollback fixtures",
        angle="Testing migration rollback safety",
        campaign_id=campaign_id,
    )
    db.insert_planned_topic(
        topic="Schema rollback fixtures",
        angle="Testing database migration rollback safety",
        campaign_id=campaign_id,
    )

    high_threshold = build_campaign_angle_diversity_report(
        db,
        campaign_id=campaign_id,
        similarity_threshold=0.99,
        now=NOW,
    )

    assert high_threshold["duplicate_groups"] == []
    assert high_threshold["summary"]["healthy"] is True
    assert "healthy" in high_threshold["summary"]["message"]


def test_diverse_campaign_produces_empty_duplicates_and_healthy_summary(db):
    campaign_id = _campaign(db)
    db.insert_planned_topic(
        topic="Migration testing",
        angle="Rollback fixtures for database deploys",
        campaign_id=campaign_id,
    )
    db.insert_planned_topic(
        topic="Launch messaging",
        angle="Position release notes for executives",
        campaign_id=campaign_id,
    )
    db.insert_planned_topic(
        topic="Support operations",
        angle="Triage inbound questions after launch",
        campaign_id=campaign_id,
    )

    report = build_campaign_angle_diversity_report(db, campaign_id=campaign_id, now=NOW)

    assert report["duplicate_groups"] == []
    assert report["campaigns"][0]["summary"]["healthy"] is True
    assert "no near-duplicate planned topics" in report["summary"]["message"]


def test_cli_json_output(db, capsys):
    campaign_id = _campaign(db)
    db.insert_planned_topic(
        topic="Release readiness",
        angle="Preflight checklist before launch",
        campaign_id=campaign_id,
    )
    db.insert_planned_topic(
        topic="Release readiness",
        angle="Preflight checklist before launch",
        campaign_id=campaign_id,
    )

    @contextmanager
    def fake_script_context():
        yield SimpleNamespace(), db

    with patch.object(campaign_angle_diversity_cli, "script_context", fake_script_context), patch.object(
        campaign_angle_diversity_cli,
        "build_campaign_angle_diversity_report",
        wraps=lambda db, **kwargs: build_campaign_angle_diversity_report(
            db,
            now=NOW,
            **kwargs,
        ),
    ):
        assert campaign_angle_diversity_cli.main(
            [
                "--campaign-id",
                str(campaign_id),
                "--status",
                "planned",
                "--similarity-threshold",
                "0.7",
                "--limit",
                "5",
                "--format",
                "json",
            ]
        ) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"]["campaign_id"] == campaign_id
    assert payload["filters"]["statuses"] == ["planned"]
    assert payload["filters"]["similarity_threshold"] == 0.7
    assert payload["filters"]["limit"] == 5
    assert payload["duplicate_groups"][0]["recommended_action"] == "merge_topics"
    assert format_campaign_angle_diversity_json(payload) == format_campaign_angle_diversity_json(payload)
