"""Tests for campaign audience overlap reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from synthesis.campaign_audience_overlap import (
    CampaignAudienceItem,
    build_campaign_audience_overlap_report,
    build_campaign_audience_overlap_report_from_items,
    format_campaign_audience_overlap_json,
    format_campaign_audience_overlap_text,
)


NOW = datetime(2026, 5, 1, 9, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "campaign_audience_overlap.py"
spec = importlib.util.spec_from_file_location("campaign_audience_overlap_script", SCRIPT_PATH)
campaign_audience_overlap_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(campaign_audience_overlap_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _item(
    item_id: int,
    tags,
    planned_at: str,
    *,
    channel: str = "x",
    status: str = "active",
    campaign_id: int | None = None,
) -> CampaignAudienceItem:
    return CampaignAudienceItem(
        id=item_id,
        title=f"item {item_id}",
        audience_tags=tuple(tags) if isinstance(tags, list | tuple) else (tags,),
        planned_at=planned_at,
        channel=channel,
        status=status,
        campaign_id=campaign_id,
        campaign_name=f"Campaign {campaign_id}" if campaign_id else None,
    )


def test_normalizes_tags_and_flags_only_above_threshold():
    report = build_campaign_audience_overlap_report_from_items(
        [
            _item(1, [" Developer Teams ", "ops"], "2026-05-01", campaign_id=11),
            _item(2, ["developer_teams"], "2026-05-02", campaign_id=12),
            _item(3, ["developer teams"], "2026-05-03", campaign_id=13),
            _item(4, ["ops"], "2026-05-04", campaign_id=14),
        ],
        days_ahead=7,
        threshold=2,
        now=NOW,
    )

    assert [cluster.audience_tag for cluster in report.clusters] == ["developer-teams"]
    assert report.clusters[0].count == 3
    assert report.clusters[0].campaign_ids == [11, 12, 13]
    assert "ops" not in format_campaign_audience_overlap_text(report)


def test_lookahead_window_boundaries_are_inclusive():
    report = build_campaign_audience_overlap_report_from_items(
        [
            _item(1, ["founders"], "2026-05-01T23:59:00+00:00"),
            _item(2, ["founders"], "2026-05-03"),
            _item(3, ["founders"], "2026-05-04"),
        ],
        days_ahead=3,
        threshold=1,
        now=NOW,
    )

    assert report.window_start == "2026-05-01"
    assert report.window_end == "2026-05-03"
    assert report.clusters[0].planned_dates == ["2026-05-01", "2026-05-03"]
    assert [item.id for item in report.clusters[0].items] == [1, 2]


def test_inactive_campaign_items_are_ignored():
    report = build_campaign_audience_overlap_report_from_items(
        [
            _item(1, ["admins"], "2026-05-01", status="active"),
            _item(2, ["admins"], "2026-05-02", status="paused"),
            _item(3, ["admins"], "2026-05-03", status="completed"),
        ],
        days_ahead=7,
        threshold=1,
        now=NOW,
    )

    assert report.considered_item_count == 1
    assert report.clusters == []


def test_database_report_reads_active_planned_rows_and_multi_channel_overlaps(db):
    db.conn.execute("ALTER TABLE planned_topics ADD COLUMN audience_tags TEXT")
    db.conn.execute("ALTER TABLE planned_topics ADD COLUMN channel TEXT")
    first = db.create_campaign(name="Launch", status="active")
    second = db.create_campaign(name="Reliability", status="active")
    inactive = db.create_campaign(name="Paused", status="paused")
    first_topic = db.insert_planned_topic(
        "Launch checklist",
        target_date="2026-05-01",
        campaign_id=first,
    )
    second_topic = db.insert_planned_topic(
        "Reliability checklist",
        target_date="2026-05-02",
        campaign_id=second,
    )
    skipped_topic = db.insert_planned_topic(
        "Skipped checklist",
        target_date="2026-05-03",
        campaign_id=second,
        status="skipped",
    )
    inactive_topic = db.insert_planned_topic(
        "Paused checklist",
        target_date="2026-05-02",
        campaign_id=inactive,
    )
    db.conn.executemany(
        "UPDATE planned_topics SET audience_tags = ?, channel = ? WHERE id = ?",
        [
            ("platform teams", "x", first_topic),
            ('["Platform Teams"]', "newsletter", second_topic),
            ("platform teams", "x", skipped_topic),
            ("platform teams", "bluesky", inactive_topic),
        ],
    )
    db.conn.commit()

    report = build_campaign_audience_overlap_report(
        db,
        days_ahead=3,
        threshold=1,
        now=NOW,
    )
    payload = json.loads(format_campaign_audience_overlap_json(report))

    assert payload["artifact_type"] == "campaign_audience_overlap"
    assert list(payload) == sorted(payload)
    assert report.considered_item_count == 2
    assert report.clusters[0].audience_tag == "platform-teams"
    assert report.clusters[0].campaign_ids == [first, second]
    assert report.clusters[0].channels == ["newsletter", "x"]
    assert report.clusters[0].planned_dates == ["2026-05-01", "2026-05-02"]
    text = format_campaign_audience_overlap_text(report)
    assert "Audience: platform-teams" in text
    assert "Channels: newsletter, x" in text
    assert "Skipped checklist" not in text
    assert "Paused checklist" not in text


def test_missing_required_tables_returns_stable_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_campaign_audience_overlap_report(conn, now=NOW)

    assert report.clusters == []
    assert report.missing_required_tables == ["content_campaigns", "planned_topics"]


def test_cli_outputs_json(db, monkeypatch, capsys):
    monkeypatch.setattr(
        campaign_audience_overlap_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        campaign_audience_overlap_script,
        "build_campaign_audience_overlap_report",
        lambda db, **kwargs: build_campaign_audience_overlap_report_from_items(
            [
                _item(1, ["builders"], "2026-05-01", channel="x", campaign_id=1),
                _item(2, ["builders"], "2026-05-02", channel="newsletter", campaign_id=2),
            ],
            now=NOW,
            **kwargs,
        ),
    )

    exit_code = campaign_audience_overlap_script.main(
        ["--days-ahead", "7", "--threshold", "1", "--format", "json"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["clusters"][0]["audience_tag"] == "builders"
    assert payload["clusters"][0]["channels"] == ["newsletter", "x"]
