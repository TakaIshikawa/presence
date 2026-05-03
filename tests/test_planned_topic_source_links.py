"""Tests for planned topic source-link audits."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from synthesis.planned_topic_source_links import (
    build_planned_topic_source_link_report,
    format_planned_topic_source_link_json,
    format_planned_topic_source_link_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "planned_topic_source_links.py"
spec = importlib.util.spec_from_file_location("planned_topic_source_links_script", SCRIPT_PATH)
planned_topic_source_links_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(planned_topic_source_links_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _campaign(db, name: str) -> int:
    return db.create_campaign(
        name=name,
        goal=f"{name} work",
        start_date="2026-05-01",
        end_date="2026-05-31",
        status="active",
    )


def _content(
    db,
    *,
    commits: list[str] | None = None,
    messages: list[str] | None = None,
    activity_ids: list[str] | None = None,
) -> int:
    return db.insert_generated_content(
        content_type="x_post",
        source_commits=commits or [],
        source_messages=messages or [],
        source_activity_ids=activity_ids or [],
        content="Generated post",
        eval_score=8.0,
        eval_feedback="ready",
    )


def _topic(db, topic: str, **kwargs) -> int:
    return db.insert_planned_topic(topic=topic, angle=f"{topic} angle", **kwargs)


def _link_content(db, topic_id: int, content_id: int) -> None:
    db.conn.execute(
        "UPDATE planned_topics SET content_id = ?, status = 'generated' WHERE id = ?",
        (content_id, topic_id),
    )
    db.conn.commit()


def test_sourced_generated_content_is_not_flagged(db):
    campaign_id = _campaign(db, "Launch Arc")
    topic_id = _topic(db, "sourced", target_date="2026-05-03", campaign_id=campaign_id)
    _link_content(db, topic_id, _content(db, commits=["abc123"]))

    report = build_planned_topic_source_link_report(db, now=NOW)

    assert report.unsourced_generated == ()
    assert report.overdue_ungenerated == ()
    assert report.to_dict()["totals"]["issue_count"] == 0


def test_unsourced_generated_content_is_reported(db):
    campaign_id = _campaign(db, "Launch Arc")
    topic_id = _topic(db, "unsourced", target_date="2026-05-03", campaign_id=campaign_id)
    content_id = _content(db)
    _link_content(db, topic_id, content_id)

    report = build_planned_topic_source_link_report(db, now=NOW)

    assert len(report.unsourced_generated) == 1
    finding = report.unsourced_generated[0]
    assert finding.planned_topic_id == topic_id
    assert finding.campaign_id == campaign_id
    assert finding.campaign_name == "Launch Arc"
    assert finding.content_id == content_id
    assert finding.reason == "unsourced_generated_content"
    assert finding.source_commits == ()
    assert finding.source_messages == ()
    assert finding.source_activity_ids == ()


def test_overdue_ungenerated_topics_are_reported_separately(db):
    old_id = _topic(db, "old", target_date="2026-05-01")
    _topic(db, "today", target_date="2026-05-02")
    _topic(db, "future", target_date="2026-05-03")

    report = build_planned_topic_source_link_report(db, now=NOW)

    assert report.unsourced_generated == ()
    assert [finding.planned_topic_id for finding in report.overdue_ungenerated] == [old_id]
    assert report.overdue_ungenerated[0].reason == "overdue_ungenerated_topic"
    assert report.overdue_ungenerated[0].content_id is None


def test_campaign_filter_resolves_id_name_and_slugified_name(db):
    launch_id = _campaign(db, "Launch Arc")
    other_id = _campaign(db, "Other Campaign")
    launch_topic = _topic(db, "launch", target_date="2026-05-03", campaign_id=launch_id)
    other_topic = _topic(db, "other", target_date="2026-05-03", campaign_id=other_id)
    _link_content(db, launch_topic, _content(db))
    _link_content(db, other_topic, _content(db))

    by_slug = build_planned_topic_source_link_report(db, campaign="launch-arc", now=NOW)
    by_name = build_planned_topic_source_link_report(db, campaign="Launch Arc", now=NOW)
    by_id = build_planned_topic_source_link_report(db, campaign=str(launch_id), now=NOW)

    assert [finding.planned_topic_id for finding in by_slug.unsourced_generated] == [launch_topic]
    assert [finding.planned_topic_id for finding in by_name.unsourced_generated] == [launch_topic]
    assert [finding.planned_topic_id for finding in by_id.unsourced_generated] == [launch_topic]
    assert by_slug.filters["campaign_id"] == launch_id


def test_days_ahead_limits_future_generated_topics_unless_included(db):
    near_id = _topic(db, "near", target_date="2026-05-05")
    far_id = _topic(db, "far", target_date="2026-06-20")
    _link_content(db, near_id, _content(db))
    _link_content(db, far_id, _content(db))

    limited = build_planned_topic_source_link_report(db, days_ahead=7, now=NOW)
    included = build_planned_topic_source_link_report(
        db,
        days_ahead=7,
        include_future=True,
        now=NOW,
    )

    assert [finding.planned_topic_id for finding in limited.unsourced_generated] == [near_id]
    assert [finding.planned_topic_id for finding in included.unsourced_generated] == [
        near_id,
        far_id,
    ]


def test_json_and_text_output_are_deterministic(db):
    overdue_id = _topic(db, "old", target_date="2026-05-01")
    unsourced_id = _topic(db, "unsourced", target_date="2026-05-03")
    _link_content(db, unsourced_id, _content(db))

    report = build_planned_topic_source_link_report(db, now=NOW)
    payload = json.loads(format_planned_topic_source_link_json(report))
    text = format_planned_topic_source_link_text(report)

    assert payload["artifact_type"] == "planned_topic_source_links"
    assert payload["generated_at"] == "2026-05-02T12:00:00+00:00"
    assert payload["findings"]["unsourced_generated"][0]["planned_topic_id"] == unsourced_id
    assert payload["findings"]["overdue_ungenerated"][0]["planned_topic_id"] == overdue_id
    assert payload["totals"] == {
        "issue_count": 2,
        "overdue_ungenerated": 1,
        "unsourced_generated": 1,
    }
    assert "Unsourced generated content:" in text
    assert "Overdue ungenerated topics:" in text


def test_cli_supports_campaign_days_ahead_include_future_and_json(db, monkeypatch, capsys):
    campaign_id = _campaign(db, "CLI Campaign")
    far_id = _topic(db, "far cli", target_date="2026-06-20", campaign_id=campaign_id)
    _link_content(db, far_id, _content(db))
    monkeypatch.setattr(
        planned_topic_source_links_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        planned_topic_source_links_script,
        "build_planned_topic_source_link_report",
        lambda db, **kwargs: build_planned_topic_source_link_report(db, now=NOW, **kwargs),
    )

    assert planned_topic_source_links_script.main(["--days-ahead", "-1"]) == 2
    assert "value must be non-negative" in capsys.readouterr().err

    assert (
        planned_topic_source_links_script.main(
            [
                "--campaign",
                "cli-campaign",
                "--days-ahead",
                "1",
                "--include-future",
                "--format",
                "json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"]["campaign"] == "cli-campaign"
    assert payload["filters"]["campaign_id"] == campaign_id
    assert payload["filters"]["include_future"] is True
    assert payload["findings"]["unsourced_generated"][0]["planned_topic_id"] == far_id
