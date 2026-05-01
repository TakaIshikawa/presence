"""Tests for publish queue platform quota planning."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from output.publish_platform_quotas import (
    format_publish_platform_quotas_json,
    format_publish_platform_quotas_text,
    parse_quota_options,
    plan_publish_platform_quotas,
)


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "plan_publish_platform_quotas.py"
)
spec = importlib.util.spec_from_file_location("plan_publish_platform_quotas_script", SCRIPT_PATH)
plan_publish_platform_quotas_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(plan_publish_platform_quotas_script)


NOW = datetime(2026, 5, 1, 9, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, label: str = "Queued copy") -> int:
    return db.conn.execute(
        """INSERT INTO generated_content
           (content, content_type, eval_score, published)
           VALUES (?, 'x_post', 7.0, 0)""",
        (label,),
    ).lastrowid


def _publication(db, content_id: int, platform: str, status: str) -> None:
    db.conn.execute(
        """INSERT INTO content_publications
           (content_id, platform, status)
           VALUES (?, ?, ?)""",
        (content_id, platform, status),
    )


def _queue(
    db,
    *,
    scheduled_at: datetime,
    platform: str = "x",
    status: str = "queued",
    publications: dict[str, str] | None = None,
) -> tuple[int, int]:
    content_id = _content(db, f"{platform} {scheduled_at.isoformat()}")
    queue_id = db.conn.execute(
        """INSERT INTO publish_queue
           (content_id, scheduled_at, platform, status)
           VALUES (?, ?, ?, ?)""",
        (content_id, scheduled_at.isoformat(), platform, status),
    ).lastrowid
    for publication_platform, publication_status in (publications or {}).items():
        _publication(db, content_id, publication_platform, publication_status)
    db.conn.commit()
    return queue_id, content_id


def test_planner_identifies_multi_platform_quota_breaches(db):
    first, _ = _queue(db, scheduled_at=NOW + timedelta(hours=1), platform="all")
    second, _ = _queue(db, scheduled_at=NOW + timedelta(hours=2), platform="all")
    _queue(db, scheduled_at=NOW + timedelta(days=1), platform="x")

    report = plan_publish_platform_quotas(
        db,
        quotas={"x": 1, "bluesky": 1},
        days=3,
        now=NOW,
    )

    assert [(breach.platform, breach.local_date) for breach in report.breaches] == [
        ("bluesky", "2026-05-01"),
        ("x", "2026-05-01"),
    ]
    assert report.breaches[0].queue_ids == (first, second)
    assert [deferral.queue_id for deferral in report.deferrals] == [second, second]
    suggested = {deferral.platform: deferral.suggested_date for deferral in report.deferrals}
    assert suggested == {"bluesky": "2026-05-02", "x": "2026-05-03"}


def test_custom_quotas_and_platform_filter_limit_breaches(db):
    _queue(db, scheduled_at=NOW + timedelta(hours=1), platform="x")
    second, _ = _queue(db, scheduled_at=NOW + timedelta(hours=2), platform="x")
    _queue(db, scheduled_at=NOW + timedelta(hours=3), platform="bluesky")

    report = plan_publish_platform_quotas(
        db,
        platform="x",
        quotas={"x": 1, "bluesky": 1},
        days=2,
        now=NOW,
    )

    assert [breach.platform for breach in report.breaches] == ["x"]
    assert [deferral.queue_id for deferral in report.deferrals] == [second]
    assert report.deferrals[0].reason == (
        "daily_quota_exceeded: x 2026-05-01 has 2 queued items over quota 1"
    )


def test_no_breach_days_have_empty_deferrals_and_stable_output(db):
    _queue(db, scheduled_at=NOW + timedelta(hours=1), platform="x")
    _queue(db, scheduled_at=NOW + timedelta(days=1, hours=1), platform="x")

    report = plan_publish_platform_quotas(
        db,
        quotas={"x": 1},
        days=3,
        now=NOW,
    )

    assert report.breaches == ()
    assert report.deferrals == ()
    assert "No quota breaches found." in format_publish_platform_quotas_text(report)
    payload = json.loads(format_publish_platform_quotas_json(report))
    assert payload["breaches"] == []
    assert payload["deferrals"] == []


def test_published_and_failed_platform_targets_are_ignored_unless_other_target_queued(db):
    _queue(
        db,
        scheduled_at=NOW + timedelta(hours=1),
        platform="all",
        publications={"x": "published", "bluesky": "queued"},
    )
    second, _ = _queue(db, scheduled_at=NOW + timedelta(hours=2), platform="bluesky")
    _queue(
        db,
        scheduled_at=NOW + timedelta(hours=3),
        platform="x",
        publications={"x": "failed"},
    )

    report = plan_publish_platform_quotas(
        db,
        quotas={"x": 1, "bluesky": 1},
        days=2,
        now=NOW,
    )

    assert [breach.platform for breach in report.breaches] == ["bluesky"]
    assert [deferral.queue_id for deferral in report.deferrals] == [second]
    assert report.totals["expanded_items"] == 2


def test_deferral_order_is_original_schedule_order_across_platforms(db):
    _queue(db, scheduled_at=NOW + timedelta(hours=1), platform="x")
    _queue(db, scheduled_at=NOW + timedelta(hours=1, minutes=30), platform="bluesky")
    x_late, _ = _queue(db, scheduled_at=NOW + timedelta(hours=2), platform="x")
    bluesky_late, _ = _queue(db, scheduled_at=NOW + timedelta(hours=3), platform="bluesky")

    report = plan_publish_platform_quotas(
        db,
        quotas={"x": 1, "bluesky": 1},
        days=2,
        now=NOW,
    )

    assert [deferral.queue_id for deferral in report.deferrals] == [x_late, bluesky_late]


def test_cli_parses_repeated_quotas_and_json_output(db, capsys):
    _queue(db, scheduled_at=NOW + timedelta(hours=1), platform="linkedin")
    second, _ = _queue(db, scheduled_at=NOW + timedelta(hours=2), platform="linkedin")

    with patch.object(
        plan_publish_platform_quotas_script,
        "script_context",
        wraps=lambda: _script_context(db),
    ), patch.object(
        plan_publish_platform_quotas_script,
        "plan_publish_platform_quotas",
        wraps=lambda db, **kwargs: plan_publish_platform_quotas(db, now=NOW, **kwargs),
    ):
        assert plan_publish_platform_quotas_script.main(
            [
                "--platform",
                "linkedin",
                "--quota",
                "linkedin=1",
                "--days",
                "2",
                "--format",
                "json",
            ]
        ) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["deferrals"][0]["queue_id"] == second
    assert payload["quotas"] == {"linkedin": 1}


def test_parse_quota_options_validates_platform_and_limit():
    assert parse_quota_options(["x=2", "mastodon=1"]) == {"x": 2, "mastodon": 1}
