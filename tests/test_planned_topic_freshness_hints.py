"""Tests for planned topic source freshness hints."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from planned_topic_freshness_hints import main  # noqa: E402
from synthesis.planned_topic_freshness_hints import (  # noqa: E402
    build_planned_topic_freshness_hints,
    build_planned_topic_freshness_hints_report,
)


TODAY = date(2026, 4, 25)
NOW = datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc)


def _knowledge(db, *, published_at: str | None, approved: int = 1) -> int:
    cursor = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, approved, published_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            "curated_article",
            f"source-{published_at or 'missing'}",
            "https://example.test/source",
            "Ada",
            "Source material for planned topic freshness.",
            approved,
            published_at,
        ),
    )
    db.conn.commit()
    return cursor.lastrowid


def test_explicit_source_dates_drive_fresh_aging_and_stale_hints():
    rows = [
        {
            "id": 1,
            "topic": "fresh",
            "angle": None,
            "campaign_id": None,
            "campaign_name": None,
            "target_date": "2000-01-01",
            "source_material": json.dumps({"source_date": "2026-04-23"}),
        },
        {
            "id": 2,
            "topic": "aging",
            "angle": None,
            "campaign_id": None,
            "campaign_name": None,
            "target_date": "2026-04-26",
            "source_material": json.dumps({"published_at": "2026-04-15T09:00:00Z"}),
        },
        {
            "id": 3,
            "topic": "stale",
            "angle": None,
            "campaign_id": None,
            "campaign_name": None,
            "target_date": "2026-05-30",
            "source_material": json.dumps({"source_date": "2026-04-01"}),
        },
    ]

    hints = build_planned_topic_freshness_hints(
        rows,
        days_stale=20,
        now=TODAY,
    )

    by_id = {hint.planned_topic_id: hint for hint in hints}
    assert by_id[1].hints == ["fresh"]
    assert by_id[1].days_since_source == 2
    assert by_id[2].hints == ["aging"]
    assert by_id[2].days_since_source == 10
    assert by_id[3].hints == ["stale_source", "refresh_recommended"]
    assert by_id[3].days_since_source == 24
    assert by_id[3].target_date == "2026-05-30"


def test_linked_knowledge_published_at_is_used_when_source_material_has_no_date():
    topic = {
        "id": 10,
        "topic": "knowledge backed",
        "angle": None,
        "campaign_id": None,
        "campaign_name": None,
        "target_date": "2026-04-01",
        "source_material": json.dumps({"knowledge_ids": [7]}),
    }

    hints = build_planned_topic_freshness_hints(
        [topic],
        [{"id": 7, "published_at": "2026-04-05T08:30:00+00:00"}],
        days_stale=14,
        now=TODAY,
    )

    hint = hints[0]
    assert hint.source_date == "2026-04-05"
    assert hint.source_date_origin == "knowledge.published_at"
    assert hint.linked_knowledge_ids == [7]
    assert hint.hints == ["stale_source", "refresh_recommended"]


def test_missing_dates_are_distinct_from_old_planned_target_dates():
    hints = build_planned_topic_freshness_hints(
        [
            {
                "id": 20,
                "topic": "old target only",
                "angle": None,
                "campaign_id": None,
                "campaign_name": None,
                "target_date": "2000-01-01",
                "source_material": json.dumps({"notes": "no date here"}),
            }
        ],
        days_stale=7,
        now=TODAY,
    )

    hint = hints[0]
    assert hint.hints == ["missing_source_date", "refresh_recommended"]
    assert hint.days_since_source is None
    assert hint.source_date is None
    assert "target" not in hint.reason


def test_threshold_configuration_changes_stale_classification():
    row = {
        "id": 30,
        "topic": "threshold",
        "angle": None,
        "campaign_id": None,
        "campaign_name": None,
        "target_date": None,
        "source_material": "Source note from 2026-04-10",
    }

    lenient = build_planned_topic_freshness_hints([row], days_stale=30, now=TODAY)
    strict = build_planned_topic_freshness_hints([row], days_stale=10, now=TODAY)

    assert lenient[0].hints == ["aging"]
    assert strict[0].hints == ["stale_source", "refresh_recommended"]


def test_report_supports_campaign_filtering_limit_and_json_cli(db, capsys):
    campaign_id = db.create_campaign(name="Freshness Campaign", status="active")
    other_campaign_id = db.create_campaign(name="Other Campaign", status="active")
    knowledge_id = _knowledge(db, published_at="2026-04-20T12:00:00+00:00")
    included_id = db.insert_planned_topic(
        "testing",
        target_date="2000-01-01",
        source_material=json.dumps({"knowledge_ids": [knowledge_id]}),
        campaign_id=campaign_id,
    )
    db.insert_planned_topic(
        "workflow",
        source_material=json.dumps({"source_date": "2026-04-01"}),
        campaign_id=campaign_id,
    )
    db.insert_planned_topic(
        "outside",
        source_material=json.dumps({"source_date": "2026-04-01"}),
        campaign_id=other_campaign_id,
    )
    fixed_report = build_planned_topic_freshness_hints_report(
        db,
        campaign="Freshness Campaign",
        days_stale=14,
        limit=1,
        now=NOW,
    )

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch(
        "planned_topic_freshness_hints.script_context",
        fake_script_context,
    ), patch(
        "planned_topic_freshness_hints.build_planned_topic_freshness_hints_report",
        return_value=fixed_report,
    ):
        result = main(
            [
                "--campaign",
                "Freshness Campaign",
                "--days-stale",
                "14",
                "--limit",
                "1",
                "--format",
                "json",
            ]
        )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["campaign"]["name"] == "Freshness Campaign"
    assert payload["thresholds"]["days_stale"] == 14
    assert payload["totals"]["planned_topics"] == 1
    assert payload["hints"][0]["planned_topic_id"] == included_id
    assert payload["hints"][0]["source_date_origin"] == "knowledge.published_at"
