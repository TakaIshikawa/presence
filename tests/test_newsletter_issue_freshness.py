"""Tests for newsletter issue source freshness reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import csv
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from output.newsletter_issue_freshness import (
    build_newsletter_issue_freshness_report,
    format_newsletter_issue_freshness_csv,
)


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "newsletter_issue_freshness.py"
)
spec = importlib.util.spec_from_file_location("newsletter_issue_freshness", SCRIPT_PATH)
newsletter_issue_freshness_cli = importlib.util.module_from_spec(spec)
spec.loader.exec_module(newsletter_issue_freshness_cli)

NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)


def _insert_content(
    db,
    *,
    created_at: str | None,
    published_at: str | None = None,
) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="newsletter source",
        eval_score=8.0,
        eval_feedback="good",
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ?, published_at = ? WHERE id = ?",
        (created_at, published_at, content_id),
    )
    db.conn.commit()
    return content_id


def _insert_issue(
    db,
    *,
    issue_id: str,
    content_ids: list[int],
    sent_at: str,
    subject: str = "Newsletter",
) -> int:
    send_id = db.insert_newsletter_send(
        issue_id=issue_id,
        subject=subject,
        content_ids=content_ids,
        status="sent",
    )
    db.conn.execute(
        "UPDATE newsletter_sends SET sent_at = ? WHERE id = ?",
        (sent_at, send_id),
    )
    db.conn.commit()
    return send_id


def _row_by_issue(report) -> dict[str, dict]:
    return {row.issue_id: row.to_dict() for row in report.rows}


def test_fresh_issue_reports_age_statistics(db):
    first = _insert_content(db, created_at="2026-05-01T12:00:00+00:00")
    second = _insert_content(db, created_at="2026-05-02T12:00:00+00:00")
    _insert_issue(
        db,
        issue_id="fresh",
        content_ids=[first, second],
        sent_at="2026-05-03T12:00:00+00:00",
    )

    report = build_newsletter_issue_freshness_report(db, stale_days=7, now=NOW)

    row = report.rows[0]
    assert row.section_count == 2
    assert row.source_timestamp_count == 2
    assert row.newest_source_age_days == 1.0
    assert row.oldest_source_age_days == 2.0
    assert row.median_source_age_days == 1.5
    assert row.stale_section_count == 0
    assert row.missing_source_count == 0
    assert row.missing_source_timestamp_count == 0
    assert row.warnings == ()


def test_stale_issue_counts_sections_over_threshold(db):
    fresh = _insert_content(db, created_at="2026-05-02T12:00:00+00:00")
    stale = _insert_content(db, created_at="2026-04-20T12:00:00+00:00")
    _insert_issue(
        db,
        issue_id="stale",
        content_ids=[fresh, stale],
        sent_at="2026-05-03T12:00:00+00:00",
    )

    report = build_newsletter_issue_freshness_report(db, stale_days=7, now=NOW)
    row = report.rows[0]

    assert row.stale_section_count == 1
    assert row.oldest_source_age_days == 13.0
    assert row.warnings == ("stale_sections",)
    assert report.summary["stale_section_count"] == 1


def test_mixed_issues_are_returned_one_row_per_issue(db):
    fresh = _insert_content(db, created_at="2026-05-02T12:00:00+00:00")
    stale = _insert_content(db, created_at="2026-04-01T12:00:00+00:00")
    _insert_issue(
        db,
        issue_id="fresh",
        content_ids=[fresh],
        sent_at="2026-05-03T12:00:00+00:00",
    )
    _insert_issue(
        db,
        issue_id="mixed",
        content_ids=[fresh, stale],
        sent_at="2026-05-02T12:00:00+00:00",
    )
    _insert_issue(
        db,
        issue_id="old-filtered",
        content_ids=[stale],
        sent_at="2026-04-01T12:00:00+00:00",
    )

    report = build_newsletter_issue_freshness_report(
        db,
        start_date="2026-05-01",
        end_date="2026-05-03",
        stale_days=14,
        now=NOW,
    )
    rows = _row_by_issue(report)

    assert list(rows) == ["fresh", "mixed"]
    assert rows["fresh"]["stale_section_count"] == 0
    assert rows["mixed"]["section_count"] == 2
    assert rows["mixed"]["stale_section_count"] == 1
    assert report.summary["issue_count"] == 2
    assert report.summary["section_count"] == 3


def test_missing_source_timestamps_and_rows_are_explicit(db):
    missing_timestamp = _insert_content(db, created_at=None, published_at=None)
    timestamped = _insert_content(db, created_at="2026-05-02T12:00:00+00:00")
    _insert_issue(
        db,
        issue_id="missing",
        content_ids=[missing_timestamp, timestamped, 9999],
        sent_at="2026-05-03T12:00:00+00:00",
    )

    report = build_newsletter_issue_freshness_report(db, stale_days=7, now=NOW)
    row = report.rows[0]

    assert row.section_count == 3
    assert row.source_timestamp_count == 1
    assert row.missing_source_count == 1
    assert row.missing_source_timestamp_count == 1
    assert row.newest_source_age_days == 1.0
    assert row.oldest_source_age_days == 1.0
    assert row.median_source_age_days == 1.0
    assert row.warnings == ("missing_source_row", "missing_source_timestamp")
    assert report.summary["missing_source_count"] == 1
    assert report.summary["missing_source_timestamp_count"] == 1


def test_published_at_takes_precedence_for_source_age(db):
    source = _insert_content(
        db,
        created_at="2026-04-01T12:00:00+00:00",
        published_at="2026-05-01T12:00:00+00:00",
    )
    _insert_issue(
        db,
        issue_id="published",
        content_ids=[source],
        sent_at="2026-05-03T12:00:00+00:00",
    )

    row = build_newsletter_issue_freshness_report(db, stale_days=7, now=NOW).rows[0]

    assert row.newest_source_age_days == 2.0
    assert row.stale_section_count == 0


def test_cli_json_and_csv_output(db, capsys):
    content_id = _insert_content(db, created_at="2026-05-01T12:00:00+00:00")
    _insert_issue(
        db,
        issue_id="cli",
        content_ids=[content_id],
        sent_at="2026-05-03T12:00:00+00:00",
    )

    @contextmanager
    def fake_script_context():
        yield SimpleNamespace(), db

    with patch.object(
        newsletter_issue_freshness_cli,
        "script_context",
        fake_script_context,
    ), patch.object(
        newsletter_issue_freshness_cli,
        "build_newsletter_issue_freshness_report",
        wraps=lambda db, **kwargs: build_newsletter_issue_freshness_report(
            db, now=NOW, **kwargs
        ),
    ):
        assert newsletter_issue_freshness_cli.main(
            [
                "--start-date",
                "2026-05-01",
                "--end-date",
                "2026-05-03",
                "--stale-days",
                "1",
                "--format",
                "json",
            ]
        ) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "newsletter_issue_freshness"
    assert payload["filters"]["stale_days"] == 1
    assert payload["rows"][0]["issue_id"] == "cli"
    assert payload["rows"][0]["stale_section_count"] == 1

    csv_text = format_newsletter_issue_freshness_csv(
        build_newsletter_issue_freshness_report(db, stale_days=1, now=NOW)
    )
    rows = list(csv.DictReader(csv_text.splitlines()))
    assert rows[0]["issue_id"] == "cli"
    assert rows[0]["source_content_ids"] == f"[{content_id}]"
    assert rows[0]["warnings"] == '["stale_sections"]'
