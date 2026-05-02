"""Tests for campaign claim balance reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

from synthesis.campaign_claim_balance import (
    build_campaign_claim_balance_report,
    classify_claim_type,
    format_campaign_claim_balance_json,
    format_campaign_claim_balance_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "campaign_claim_balance.py"
spec = importlib.util.spec_from_file_location("campaign_claim_balance", SCRIPT_PATH)
campaign_claim_balance = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(campaign_claim_balance)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _campaign(db, name: str, status: str = "active") -> int:
    return db.create_campaign(
        name=name,
        goal=f"{name} goal",
        start_date="2026-04-01",
        end_date="2026-06-01",
        status=status,
    )


def _content(
    db,
    *,
    campaign_id: int,
    text: str,
    created_at: datetime | None = None,
    published_at: datetime | None = None,
    content_format: str | None = None,
) -> int:
    content_id = db.insert_generated_content(
        "x_post",
        [],
        [],
        text,
        7.0,
        "",
        content_format=content_format,
    )
    db.conn.execute(
        """UPDATE generated_content
           SET created_at = ?, published_at = ?, published = ?
           WHERE id = ?""",
        (
            (created_at or NOW - timedelta(days=1)).isoformat(),
            published_at.isoformat() if published_at else None,
            1 if published_at else 0,
            content_id,
        ),
    )
    planned_id = db.insert_planned_topic(
        topic="Reliability",
        angle=text[:80],
        target_date="2026-05-01",
        campaign_id=campaign_id,
        status="generated",
    )
    db.conn.execute(
        "UPDATE planned_topics SET content_id = ? WHERE id = ?",
        (content_id, planned_id),
    )
    db.conn.commit()
    return content_id


def test_heuristic_classification_priorities_are_deterministic():
    assert classify_claim_type("Did latency drop after the deploy?") == "question_claim"
    assert classify_claim_type("Latency fell 42% after removing one retry loop") == "metric_claim"
    assert classify_claim_type("The lesson: retries need a budget") == "lesson_claim"
    assert classify_claim_type("A three step workflow for safer releases") == "process_claim"
    assert classify_claim_type("This works, but only when queues are bounded") == "caveat_claim"
    assert classify_claim_type("When we shipped the migration, review got easier") == "story_claim"
    assert classify_claim_type("Any text", content_format="question_hook") == "question_claim"


def test_distribution_scoring_and_suggestions_favor_underrepresented_claims(db):
    campaign_id = _campaign(db, "Launch")
    first = _content(
        db,
        campaign_id=campaign_id,
        text="Latency fell 42% after the launch",
    )
    _content(db, campaign_id=campaign_id, text="The rollout saved 12 hours per week")
    _content(db, campaign_id=campaign_id, text="We cut incidents by 3x in one month")
    _content(db, campaign_id=campaign_id, text="The lesson: smallest checks found the bug")
    _content(db, campaign_id=campaign_id, text="How did the migration change reviews?")

    report = build_campaign_claim_balance_report(db, now=NOW, lookback_days=30)
    campaign = report.campaigns[0]

    assert campaign.campaign_id == campaign_id
    assert campaign.claim_type_distribution["metric_claim"] == 3
    assert campaign.claim_type_distribution["lesson_claim"] == 1
    assert campaign.claim_type_distribution["question_claim"] == 1
    assert campaign.dominant_claim_type == "metric_claim"
    assert campaign.imbalance_score == 0.52
    assert campaign.suggested_next_claim_types == (
        "process_claim",
        "caveat_claim",
        "story_claim",
    )
    assert first in campaign.sample_content_ids
    assert report.totals == {"campaign_count": 1, "content_count": 5}


def test_campaign_filter_and_lookback_window_are_deterministic(db):
    included_campaign = _campaign(db, "Included")
    excluded_campaign = _campaign(db, "Excluded")
    included_id = _content(
        db,
        campaign_id=included_campaign,
        text="A checklist kept the deploy quiet",
        created_at=NOW - timedelta(days=2),
    )
    _content(
        db,
        campaign_id=included_campaign,
        text="Latency dropped 30%",
        created_at=NOW - timedelta(days=45),
    )
    _content(
        db,
        campaign_id=included_campaign,
        text="Old generated item, but published this week with 18% better clicks",
        created_at=NOW - timedelta(days=45),
        published_at=NOW - timedelta(days=1),
    )
    _content(
        db,
        campaign_id=excluded_campaign,
        text="Did this other campaign work?",
        created_at=NOW - timedelta(days=1),
    )

    report = build_campaign_claim_balance_report(
        db,
        campaign_id=included_campaign,
        lookback_days=30,
        now=NOW,
    )

    assert [campaign.campaign_id for campaign in report.campaigns] == [included_campaign]
    campaign = report.campaigns[0]
    assert campaign.content_count == 2
    assert included_id in campaign.sample_content_ids
    assert campaign.claim_type_distribution["process_claim"] == 1
    assert campaign.claim_type_distribution["metric_claim"] == 1


def test_empty_data_and_missing_schema_return_stable_empty_reports(db):
    campaign_id = _campaign(db, "Sparse")
    report = build_campaign_claim_balance_report(
        db,
        campaign_id=campaign_id,
        lookback_days=30,
        now=NOW,
    )

    assert report.totals == {"campaign_count": 1, "content_count": 0}
    assert report.campaigns[0].dominant_claim_type is None
    assert report.campaigns[0].imbalance_score == 0.0
    assert report.campaigns[0].suggested_next_claim_types == (
        "metric_claim",
        "lesson_claim",
        "process_claim",
    )

    conn = sqlite3.connect(":memory:")
    try:
        missing = build_campaign_claim_balance_report(conn, now=NOW)
    finally:
        conn.close()
    assert missing.campaigns == ()
    assert missing.missing_required_tables == (
        "content_campaigns",
        "generated_content",
        "planned_topics",
    )


def test_json_text_and_cli_outputs_are_stable(db, capsys):
    campaign_id = _campaign(db, "Renderer")
    _content(db, campaign_id=campaign_id, text="This saved 20 minutes")
    report = build_campaign_claim_balance_report(db, now=NOW, lookback_days=30)

    assert format_campaign_claim_balance_json(report) == format_campaign_claim_balance_json(report)
    payload = json.loads(format_campaign_claim_balance_json(report))
    assert sorted(payload) == [
        "campaigns",
        "filters",
        "generated_at",
        "missing_required_columns",
        "missing_required_tables",
        "totals",
    ]
    assert payload["campaigns"][0]["dominant_claim_type"] == "metric_claim"
    text = format_campaign_claim_balance_text(report)
    assert "Campaign Claim Balance" in text
    assert "dominant=metric_claim" in text
    assert "suggested_next_claim_types" in text

    with patch.object(
        campaign_claim_balance,
        "script_context",
        wraps=lambda: _script_context(db),
    ):
        exit_code = campaign_claim_balance.main(
            ["--campaign-id", str(campaign_id), "--days", "30", "--format", "json"]
        )

    assert exit_code == 0
    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["filters"]["campaign_id"] == campaign_id
    assert cli_payload["totals"]["content_count"] == 1
