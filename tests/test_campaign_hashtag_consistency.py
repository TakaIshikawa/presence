"""Tests for campaign hashtag consistency reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.campaign_hashtag_consistency import (
    build_campaign_hashtag_consistency_report,
    canonicalize_hashtag,
    extract_hashtags,
    format_campaign_hashtag_consistency_json,
    format_campaign_hashtag_consistency_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "campaign_hashtag_consistency.py"
spec = importlib.util.spec_from_file_location("campaign_hashtag_consistency_script", SCRIPT_PATH)
campaign_hashtag_consistency_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(campaign_hashtag_consistency_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _campaign_content(
    db,
    campaign_id: int,
    content: str,
    *,
    queued: bool = True,
    created_at: datetime | None = None,
) -> int:
    planned_id = db.insert_planned_topic(topic="launch", campaign_id=campaign_id)
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=8.0,
        eval_feedback="usable",
    )
    db.mark_planned_topic_generated(planned_id, content_id)
    if queued:
        db.conn.execute(
            """INSERT INTO publish_queue (content_id, scheduled_at, platform, status)
               VALUES (?, ?, 'x', 'queued')""",
            (content_id, (NOW + timedelta(hours=1)).isoformat()),
        )
    if created_at is not None:
        db.conn.execute(
            "UPDATE generated_content SET created_at = ? WHERE id = ?",
            (created_at.isoformat(), content_id),
        )
    db.conn.commit()
    return content_id


def test_extracts_hashtags_case_insensitively_while_preserving_examples():
    hashtags = extract_hashtags("Ship #LaunchAI today. Repeat #launchai and keep #Launch_AI.")

    assert hashtags == ("#LaunchAI", "#Launch_AI")
    assert canonicalize_hashtag("#LaunchAI") == "#launchai"


def test_flags_missing_variant_and_over_limit_findings_per_content_item(db):
    campaign_id = db.create_campaign(name="Launch", status="active")
    first = _campaign_content(db, campaign_id, "The rollout note is ready. #LaunchAI #BuildInPublic")
    second = _campaign_content(db, campaign_id, "The checklist is ready. #launchai")
    variant = _campaign_content(db, campaign_id, "The migration caveat is ready. #Launch_AI")
    missing = _campaign_content(db, campaign_id, "The fallback story is ready.")
    noisy = _campaign_content(
        db,
        campaign_id,
        "The summary is ready. #LaunchAI #Ops #Reliability #Deploy",
    )

    report = build_campaign_hashtag_consistency_report(
        db,
        max_hashtags=3,
        now=NOW,
    )
    payload = json.loads(format_campaign_hashtag_consistency_json(report))
    campaign = payload["campaigns"][0]
    findings = {(row["content_id"], row["finding_type"]) for row in campaign["findings"]}

    assert campaign["required_hashtags"] == ["#launchai"]
    assert campaign["hashtag_usage"]["#launchai"] == 3
    assert campaign["examples"]["#launchai"] == ["#LaunchAI", "#launchai"]
    assert (variant, "variant") in findings
    assert (missing, "missing_required") in findings
    assert (noisy, "over_limit") in findings
    assert (first, "missing_required") not in findings
    assert (second, "missing_required") not in findings
    assert payload["findings_by_type"] == {
        "missing_required": 1,
        "over_limit": 1,
        "variant": 1,
    }


def test_groups_multiple_campaigns_and_filters_by_campaign_name(db):
    launch_id = db.create_campaign(name="Launch", status="active")
    reliability_id = db.create_campaign(name="Reliability", status="active")
    _campaign_content(db, launch_id, "Launch copy #LaunchAI")
    _campaign_content(db, launch_id, "More launch copy #LaunchAI")
    _campaign_content(db, reliability_id, "Reliability copy #OpsReady")
    _campaign_content(db, reliability_id, "Another reliability copy without the tag")

    all_report = build_campaign_hashtag_consistency_report(db, max_hashtags=3, now=NOW)
    filtered = build_campaign_hashtag_consistency_report(
        db,
        campaign="Reliability",
        max_hashtags=3,
        now=NOW,
    )

    assert [campaign.campaign_id for campaign in all_report.campaigns] == [
        launch_id,
        reliability_id,
    ]
    assert [campaign.campaign_id for campaign in filtered.campaigns] == [reliability_id]
    assert filtered.campaigns[0].required_hashtags == ()


def test_configurable_hashtag_limit_changes_over_limit_findings(db):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            content TEXT NOT NULL,
            content_type TEXT,
            published INTEGER DEFAULT 0,
            created_at TEXT
        );
        CREATE TABLE planned_topics (
            id INTEGER PRIMARY KEY,
            campaign_id INTEGER,
            topic TEXT,
            content_id INTEGER
        );
        CREATE TABLE content_campaigns (
            id INTEGER PRIMARY KEY,
            name TEXT,
            status TEXT
        );
        """
    )
    conn.execute("INSERT INTO content_campaigns VALUES (7, 'Manual', 'active')")
    conn.execute(
        "INSERT INTO generated_content VALUES (1, 'Copy #One #Two #Three', 'x_post', 0, ?)",
        (NOW.isoformat(),),
    )
    conn.execute("INSERT INTO planned_topics VALUES (1, 7, 'manual', 1)")

    strict = build_campaign_hashtag_consistency_report(conn, max_hashtags=2, now=NOW)
    relaxed = build_campaign_hashtag_consistency_report(conn, max_hashtags=3, now=NOW)

    assert [row.finding_type for row in strict.campaigns[0].findings] == ["over_limit"]
    assert relaxed.campaigns[0].findings == ()


def test_missing_required_tables_and_invalid_args_are_reported():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_campaign_hashtag_consistency_report(conn, now=NOW)

    assert report.campaigns == ()
    assert report.missing_required_tables == ("generated_content", "planned_topics")
    with pytest.raises(ValueError, match="max_hashtags must be positive"):
        build_campaign_hashtag_consistency_report(conn, max_hashtags=0, now=NOW)


def test_text_and_json_formatters_are_deterministic(db):
    campaign_id = db.create_campaign(name="Text", status="active")
    _campaign_content(db, campaign_id, "Text copy #Docs")
    _campaign_content(db, campaign_id, "More text copy #Docs")

    report = build_campaign_hashtag_consistency_report(db, max_hashtags=3, now=NOW)
    payload = json.loads(format_campaign_hashtag_consistency_json(report))
    text = format_campaign_hashtag_consistency_text(report)

    assert payload["artifact_type"] == "campaign_hashtag_consistency"
    assert list(payload) == sorted(payload)
    assert "Campaign Hashtag Consistency" in text
    assert "Campaign #" in text
    assert "Required: #docs" in text
    assert format_campaign_hashtag_consistency_json(report) == format_campaign_hashtag_consistency_json(report)


def test_cli_supports_campaign_and_max_hashtag_options(db, monkeypatch, capsys):
    campaign_id = db.create_campaign(name="CLI", status="active")
    _campaign_content(db, campaign_id, "CLI copy #One #Two #Three")
    monkeypatch.setattr(
        campaign_hashtag_consistency_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        campaign_hashtag_consistency_script,
        "build_campaign_hashtag_consistency_report",
        lambda db, **kwargs: build_campaign_hashtag_consistency_report(db, now=NOW, **kwargs),
    )

    exit_code = campaign_hashtag_consistency_script.main(
        ["--campaign", str(campaign_id), "--max-hashtags", "2", "--format", "json"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["campaign"] == str(campaign_id)
    assert payload["campaigns"][0]["findings"][0]["finding_type"] == "over_limit"
