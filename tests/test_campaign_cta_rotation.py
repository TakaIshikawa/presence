"""Tests for campaign CTA rotation reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.campaign_cta_rotation import (
    build_campaign_cta_rotation_report,
    extract_cta_families,
    format_campaign_cta_rotation_json,
    format_campaign_cta_rotation_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "campaign_cta_rotation.py"
spec = importlib.util.spec_from_file_location("campaign_cta_rotation_script", SCRIPT_PATH)
campaign_cta_rotation_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(campaign_cta_rotation_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _campaign_content(
    db,
    campaign_id: int | None,
    content: str,
    *,
    topic: str = "ops",
    created_at: datetime | None = None,
) -> int:
    planned_id = db.insert_planned_topic(topic=topic, campaign_id=campaign_id)
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=8.0,
        eval_feedback="usable",
    )
    db.mark_planned_topic_generated(planned_id, content_id)
    if created_at is not None:
        db.conn.execute(
            "UPDATE generated_content SET created_at = ? WHERE id = ?",
            (created_at.isoformat(), content_id),
        )
        db.conn.commit()
    return content_id


def test_extracts_deterministic_cta_families_from_simple_phrases():
    matches = extract_cta_families(
        "Read the full breakdown before launch. Reply with the edge case I missed. "
        "Share this with the release owner. Try this checklist on Friday. Subscribe for updates."
    )

    assert [family for family, _phrase in matches] == [
        "read",
        "reply",
        "share",
        "try",
        "subscribe",
    ]


def test_groups_campaign_content_and_flags_repeated_family(db):
    campaign_id = db.create_campaign(name="Launch", status="active")
    other_id = db.create_campaign(name="Reliability", status="active")
    first = _campaign_content(
        db,
        campaign_id,
        "The checklist is ready. Subscribe for the next release note.",
        topic="launch",
        created_at=NOW - timedelta(days=1),
    )
    second = _campaign_content(
        db,
        campaign_id,
        "The migration path is shorter now. Sign up for updates before rollout.",
        topic="migration",
        created_at=NOW - timedelta(days=2),
    )
    _campaign_content(
        db,
        other_id,
        "The fallback path changed. Reply with the state you want covered.",
        topic="fallbacks",
        created_at=NOW - timedelta(days=1),
    )

    report = build_campaign_cta_rotation_report(
        db,
        days=14,
        min_repeat=2,
        now=NOW,
    )
    payload = json.loads(format_campaign_cta_rotation_json(report))

    launch = next(campaign for campaign in payload["campaigns"] if campaign["campaign_id"] == campaign_id)
    reliability = next(campaign for campaign in payload["campaigns"] if campaign["campaign_id"] == other_id)
    assert launch["flagged"] is True
    assert launch["repeated_families"][0]["family"] == "subscribe"
    assert launch["repeated_families"][0]["count"] == 2
    assert launch["repeated_families"][0]["content_ids"] == sorted([first, second])
    assert reliability["flagged"] is False
    assert payload["flagged_campaign_count"] == 1


def test_filters_by_campaign_and_lookback_window(db):
    campaign_id = db.create_campaign(name="Window", status="active")
    other_id = db.create_campaign(name="Other", status="active")
    _campaign_content(
        db,
        campaign_id,
        "Subscribe for the new version.",
        created_at=NOW - timedelta(days=1),
    )
    _campaign_content(
        db,
        campaign_id,
        "Subscribe for the old version.",
        created_at=NOW - timedelta(days=40),
    )
    _campaign_content(
        db,
        other_id,
        "Subscribe for a different campaign.",
        created_at=NOW - timedelta(days=1),
    )

    report = build_campaign_cta_rotation_report(
        db,
        campaign_id=campaign_id,
        days=30,
        min_repeat=2,
        now=NOW,
    )

    assert [campaign.campaign_id for campaign in report.campaigns] == [campaign_id]
    assert report.campaigns[0].generated_count == 1
    assert report.campaigns[0].flagged is False


def test_handles_content_without_campaign_metadata_without_crashing(db):
    campaign_id = db.create_campaign(name="Mixed", status="active")
    _campaign_content(
        db,
        campaign_id,
        "Reply with the version that feels too risky.",
        created_at=NOW - timedelta(days=1),
    )
    _campaign_content(
        db,
        None,
        "Subscribe even though this planned topic is not campaign-linked.",
        created_at=NOW - timedelta(days=1),
    )

    report = build_campaign_cta_rotation_report(db, days=7, min_repeat=2, now=NOW)

    assert report.content_without_campaign_count == 1
    assert [campaign.campaign_id for campaign in report.campaigns] == [campaign_id]


def test_missing_campaign_table_still_groups_by_campaign_id():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            content TEXT NOT NULL,
            content_type TEXT,
            created_at TEXT
        )"""
    )
    conn.execute(
        """CREATE TABLE planned_topics (
            id INTEGER PRIMARY KEY,
            campaign_id INTEGER,
            topic TEXT,
            content_id INTEGER
        )"""
    )
    conn.execute(
        "INSERT INTO generated_content VALUES (1, 'Share this with the launch owner.', 'x_post', ?)",
        ((NOW - timedelta(days=1)).isoformat(),),
    )
    conn.execute("INSERT INTO planned_topics VALUES (1, 44, 'launch', 1)")

    report = build_campaign_cta_rotation_report(conn, days=7, min_repeat=1, now=NOW)

    assert report.missing_required_tables == ()
    assert report.campaigns[0].campaign_id == 44
    assert report.campaigns[0].campaign_name is None
    assert report.campaigns[0].repeated_families[0].family == "share"


def test_missing_required_tables_return_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE generated_content (id INTEGER PRIMARY KEY, content TEXT)")

    report = build_campaign_cta_rotation_report(conn, days=7, min_repeat=2, now=NOW)

    assert report.missing_required_tables == ("planned_topics",)
    assert report.campaigns == ()


def test_text_and_json_reports_are_deterministic(db):
    campaign_id = db.create_campaign(name="Table", status="active")
    _campaign_content(
        db,
        campaign_id,
        "Read more when the post is published.",
        created_at=NOW - timedelta(days=1),
    )

    report = build_campaign_cta_rotation_report(db, days=7, min_repeat=2, now=NOW)
    payload = json.loads(format_campaign_cta_rotation_json(report))
    text = format_campaign_cta_rotation_text(report)

    assert payload["artifact_type"] == "campaign_cta_rotation"
    assert list(payload) == sorted(payload)
    assert "Campaign CTA Rotation" in text
    assert "Campaign #" in text
    assert "read: 1 content item" in text
    assert format_campaign_cta_rotation_json(report) == format_campaign_cta_rotation_json(report)


def test_cli_supports_requested_flags_and_json_output(db, monkeypatch, capsys):
    campaign_id = db.create_campaign(name="CLI", status="active")
    _campaign_content(
        db,
        campaign_id,
        "Try this release checklist before the next deploy.",
        created_at=NOW - timedelta(days=1),
    )
    monkeypatch.setattr(
        campaign_cta_rotation_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        campaign_cta_rotation_script,
        "build_campaign_cta_rotation_report",
        lambda db, **kwargs: build_campaign_cta_rotation_report(db, now=NOW, **kwargs),
    )

    exit_code = campaign_cta_rotation_script.main(
        ["--days", "7", "--campaign-id", str(campaign_id), "--min-repeat", "1", "--json"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["campaign_id"] == campaign_id
    assert payload["campaigns"][0]["repeated_families"][0]["family"] == "try"
