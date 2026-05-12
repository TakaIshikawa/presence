"""Tests for campaign format fatigue reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.campaign_format_fatigue import (
    build_campaign_format_fatigue_report,
    format_campaign_format_fatigue_json,
    format_campaign_format_fatigue_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "campaign_format_fatigue.py"
spec = importlib.util.spec_from_file_location("campaign_format_fatigue_script", SCRIPT_PATH)
campaign_format_fatigue_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(campaign_format_fatigue_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(
    db,
    *,
    content_format: str | None,
    created_at: datetime,
    campaign_id: int | None = None,
    topic: str = "ops",
    content_type: str = "x_post",
) -> int:
    planned_id = db.insert_planned_topic(topic=topic, campaign_id=campaign_id)
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=f"{topic} content",
        eval_score=8.0,
        eval_feedback="usable",
        content_format=content_format,
    )
    db.mark_planned_topic_generated(planned_id, content_id)
    db.conn.execute(
        "UPDATE generated_content SET created_at = ? WHERE id = ?",
        (created_at.isoformat(), content_id),
    )
    db.conn.commit()
    return content_id


def test_flags_campaign_exceeding_dominant_format_threshold(db):
    campaign_id = db.create_campaign(name="Launch", status="active")
    first = _content(db, campaign_id=campaign_id, topic="launch", content_format="tip", created_at=NOW - timedelta(days=1))
    second = _content(db, campaign_id=campaign_id, topic="launch", content_format="tip", created_at=NOW - timedelta(days=2))
    third = _content(db, campaign_id=campaign_id, topic="launch", content_format="question", created_at=NOW - timedelta(days=3))
    _content(db, campaign_id=None, topic="other", content_format="tip", created_at=NOW - timedelta(days=1))

    report = build_campaign_format_fatigue_report(
        db,
        days=7,
        min_count=3,
        dominant_share=0.66,
        now=NOW,
    )
    payload = json.loads(format_campaign_format_fatigue_json(report))
    text = format_campaign_format_fatigue_text(report)

    assert payload["groups"][0]["group_type"] == "campaign"
    assert payload["groups"][0]["group_key"] == f"{campaign_id}:Launch"
    assert payload["groups"][0]["format_counts"] == {"question": 1, "tip": 2}
    assert payload["groups"][0]["dominant_format"] == "tip"
    assert payload["groups"][0]["dominant_share"] == 0.6667
    assert [example["content_id"] for example in payload["groups"][0]["examples"]] == [first, second, third]
    assert "dominant_format=tip" in text
    assert f"content_ids={first}, {second}, {third}" in text


def test_thresholds_control_flagging(db):
    campaign_id = db.create_campaign(name="Balanced", status="active")
    _content(db, campaign_id=campaign_id, topic="balance", content_format="tip", created_at=NOW - timedelta(days=1))
    _content(db, campaign_id=campaign_id, topic="balance", content_format="question", created_at=NOW - timedelta(days=2))
    _content(db, campaign_id=campaign_id, topic="balance", content_format="story", created_at=NOW - timedelta(days=3))

    report = build_campaign_format_fatigue_report(
        db,
        days=7,
        min_count=3,
        dominant_share=0.75,
        now=NOW,
    )

    assert report.groups == ()
    assert report.totals["group_count"] == 1


def test_metadata_parsing_handles_null_malformed_and_alternate_keys(db):
    db.conn.execute("ALTER TABLE generated_content ADD COLUMN metadata JSON")
    first = _content(db, campaign_id=None, topic="ignored", content_format=None, created_at=NOW - timedelta(days=1))
    second = _content(db, campaign_id=None, topic="ignored", content_format=None, created_at=NOW - timedelta(days=2))
    third = _content(db, campaign_id=None, topic="ignored", content_format="question", created_at=NOW - timedelta(days=3))
    bad = _content(db, campaign_id=None, topic="fallback", content_format="tip", created_at=NOW - timedelta(days=1))
    db.conn.execute(
        "UPDATE generated_content SET metadata = ? WHERE id = ?",
        (json.dumps({"campaign_id": "meta-campaign", "format": "micro_story"}), first),
    )
    db.conn.execute(
        "UPDATE generated_content SET metadata = ? WHERE id = ?",
        (json.dumps({"campaign": "meta-campaign", "content_format": "micro_story"}), second),
    )
    db.conn.execute(
        "UPDATE generated_content SET metadata = ? WHERE id = ?",
        (json.dumps({"theme": "fallback-theme"}), third),
    )
    db.conn.execute(
        "UPDATE generated_content SET metadata = ? WHERE id = ?",
        ("{not-json", bad),
    )
    db.conn.commit()

    report = build_campaign_format_fatigue_report(
        db,
        days=7,
        min_count=2,
        dominant_share=1.0,
        now=NOW,
    )
    payload = json.loads(format_campaign_format_fatigue_json(report))

    assert report.totals["malformed_metadata_count"] == 1
    assert payload["groups"][0]["group_key"] == "meta-campaign"
    assert payload["groups"][0]["dominant_format"] == "micro_story"


def test_cli_supports_json_output(db, monkeypatch, capsys):
    campaign_id = db.create_campaign(name="CLI", status="active")
    _content(db, campaign_id=campaign_id, topic="cli", content_format="tip", created_at=NOW - timedelta(days=1))
    _content(db, campaign_id=campaign_id, topic="cli", content_format="tip", created_at=NOW - timedelta(days=2))

    monkeypatch.setattr(
        campaign_format_fatigue_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        campaign_format_fatigue_script,
        "build_campaign_format_fatigue_report",
        lambda db, **kwargs: build_campaign_format_fatigue_report(db, now=NOW, **kwargs),
    )

    exit_code = campaign_format_fatigue_script.main(
        ["--days", "7", "--min-count", "2", "--dominant-share", "1", "--format", "json"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["groups"][0]["group_key"] == f"{campaign_id}:CLI"
    assert payload["groups"][0]["dominant_format"] == "tip"


def test_cli_returns_nonzero_on_database_error(monkeypatch, capsys):
    monkeypatch.setattr(
        campaign_format_fatigue_script,
        "script_context",
        lambda: _script_context(SimpleNamespace()),
    )
    monkeypatch.setattr(
        campaign_format_fatigue_script,
        "build_campaign_format_fatigue_report",
        lambda *args, **kwargs: (_ for _ in ()).throw(sqlite3.Error("db failed")),
    )

    exit_code = campaign_format_fatigue_script.main([])

    assert exit_code == 1
    assert "error: db failed" in capsys.readouterr().err
