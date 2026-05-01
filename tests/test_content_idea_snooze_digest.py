"""Tests for content idea snooze digest reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from synthesis.content_idea_snooze_digest import (
    build_content_idea_snooze_digest,
    format_content_idea_snooze_digest_json,
    format_content_idea_snooze_digest_text,
)


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "content_idea_snooze_digest.py"
)
spec = importlib.util.spec_from_file_location(
    "content_idea_snooze_digest_cli",
    SCRIPT_PATH,
)
content_idea_snooze_digest_cli = importlib.util.module_from_spec(spec)
spec.loader.exec_module(content_idea_snooze_digest_cli)

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


def _idea(
    db,
    *,
    note: str,
    topic: str = "workflow",
    priority: str = "normal",
    source: str | None = "unit",
    source_metadata: dict | None = None,
    snoozed_until: datetime | None = None,
    status: str = "open",
) -> int:
    idea_id = db.add_content_idea(
        note=note,
        topic=topic,
        priority=priority,
        source=source,
        source_metadata=source_metadata,
        status=status,
    )
    if snoozed_until is not None:
        db.conn.execute(
            """UPDATE content_ideas
               SET snoozed_until = ?, snooze_reason = 'waiting for timing'
               WHERE id = ?""",
            (snoozed_until.isoformat(), idea_id),
        )
        db.conn.commit()
    return idea_id


def _idea_by_id(report: dict) -> dict[int, dict]:
    return {idea["id"]: idea for idea in report["ideas"]}


def _group(report: dict, name: str) -> dict[str, int]:
    return {item["value"]: item["count"] for item in report["groups"][name]}


def test_reports_expired_upcoming_and_group_counts(db):
    expired_high = _idea(
        db,
        note="Promote source-rich expired idea",
        topic="agents",
        priority="high",
        source="seed_gap_ideas",
        source_metadata={
            "source_count": 3,
            "latest_source_at": "2026-04-30T12:00:00+00:00",
        },
        snoozed_until=NOW - timedelta(days=2),
    )
    upcoming = _idea(
        db,
        note="Review this next week",
        topic="ops",
        priority="normal",
        source="issue_digest",
        source_metadata={"issue_id": "123"},
        snoozed_until=NOW + timedelta(days=3),
    )
    expired_old_low = _idea(
        db,
        note="Old low-priority idea with no source detail",
        topic="ops",
        priority="low",
        source=None,
        source_metadata={},
        snoozed_until=NOW - timedelta(days=40),
    )

    report = build_content_idea_snooze_digest(db, days_ahead=7, now=NOW)
    ideas = _idea_by_id(report)

    assert report["summary"]["idea_count"] == 3
    assert report["summary"]["expired_count"] == 2
    assert report["summary"]["upcoming_count"] == 1
    assert ideas[expired_high]["snooze_status"] == "expired"
    assert ideas[expired_high]["days_overdue"] == 2.0
    assert ideas[expired_high]["recommendation"] == "promote"
    assert ideas[upcoming]["snooze_status"] == "upcoming"
    assert ideas[upcoming]["days_until_due"] == 3.0
    assert ideas[upcoming]["recommendation"] == "keep_snoozed"
    assert ideas[expired_old_low]["recommendation"] == "dismiss_review"
    assert _group(report, "priority") == {"high": 1, "low": 1, "normal": 1}
    assert _group(report, "topic") == {"ops": 2, "agents": 1}
    assert _group(report, "source") == {
        "issue_digest": 1,
        "seed_gap_ideas": 1,
        "unknown": 1,
    }
    assert _group(report, "overdue_age") == {
        "overdue_1_7d": 1,
        "overdue_31d_plus": 1,
        "upcoming": 1,
    }


def test_future_snoozes_outside_window_are_excluded(db):
    included = _idea(
        db,
        note="Soon",
        snoozed_until=NOW + timedelta(days=2),
    )
    future = _idea(
        db,
        note="Much later",
        snoozed_until=NOW + timedelta(days=30),
    )

    report = build_content_idea_snooze_digest(db, days_ahead=7, now=NOW)

    assert [idea["id"] for idea in report["ideas"]] == [included]
    assert future not in _idea_by_id(report)


def test_unsnoozed_ideas_are_excluded_unless_requested(db):
    expired = _idea(
        db,
        note="Expired",
        snoozed_until=NOW - timedelta(days=1),
    )
    unsnoozed = _idea(
        db,
        note="Open but never snoozed",
        priority="low",
        source=None,
        source_metadata={},
    )

    default_report = build_content_idea_snooze_digest(db, days_ahead=7, now=NOW)
    included_report = build_content_idea_snooze_digest(
        db,
        days_ahead=7,
        include_unsnoozed=True,
        now=NOW,
    )

    assert [idea["id"] for idea in default_report["ideas"]] == [expired]
    ideas = _idea_by_id(included_report)
    assert set(ideas) == {expired, unsnoozed}
    assert ideas[unsnoozed]["snooze_status"] == "unsnoozed"
    assert ideas[unsnoozed]["recommendation"] == "dismiss_review"


def test_limit_and_json_output_are_deterministic(db):
    first = _idea(
        db,
        note="First",
        topic="b",
        snoozed_until=NOW - timedelta(hours=2),
    )
    _idea(
        db,
        note="Second",
        topic="a",
        snoozed_until=NOW + timedelta(days=1),
    )

    report = build_content_idea_snooze_digest(db, days_ahead=7, limit=1, now=NOW)
    first_json = format_content_idea_snooze_digest_json(report)
    second_json = format_content_idea_snooze_digest_json(report)

    assert [idea["id"] for idea in report["ideas"]] == [first]
    assert first_json == second_json
    payload = json.loads(first_json)
    assert payload["filters"]["limit"] == 1
    assert payload["ideas"][0]["overdue_age_bucket"] == "due_today"


def test_text_output_is_readable_for_cron_logs(db):
    _idea(
        db,
        note="Expired content idea",
        topic="workflow",
        snoozed_until=NOW - timedelta(days=1),
    )

    text = format_content_idea_snooze_digest_text(
        build_content_idea_snooze_digest(db, days_ahead=7, now=NOW)
    )

    assert "Content idea snooze digest" in text
    assert "Totals: ideas=1 expired=1 upcoming=0 unsnoozed=0" in text
    assert "workflow / Expired content idea" in text
    assert "unsnooze" in text


def test_cli_json_output_and_argument_validation(db, capsys):
    _idea(
        db,
        note="CLI idea",
        snoozed_until=NOW - timedelta(days=1),
    )

    @contextmanager
    def fake_script_context():
        yield SimpleNamespace(), db

    with patch.object(
        content_idea_snooze_digest_cli,
        "script_context",
        fake_script_context,
    ), patch.object(
        content_idea_snooze_digest_cli,
        "build_content_idea_snooze_digest",
        wraps=lambda db, **kwargs: build_content_idea_snooze_digest(
            db,
            now=NOW,
            **kwargs,
        ),
    ):
        assert content_idea_snooze_digest_cli.main(
            [
                "--days-ahead",
                "7",
                "--include-unsnoozed",
                "--limit",
                "10",
                "--format",
                "json",
            ]
        ) == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["filters"]["days_ahead"] == 7
        assert payload["filters"]["include_unsnoozed"] is True
        assert payload["summary"]["idea_count"] == 1

        assert content_idea_snooze_digest_cli.main(["--days-ahead", "-1"]) == 1
        assert "days_ahead must be non-negative" in capsys.readouterr().err
