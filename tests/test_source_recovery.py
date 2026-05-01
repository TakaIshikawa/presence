"""Tests for curated source recovery planning."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

from knowledge.source_recovery import (
    build_source_recovery_plan,
    export_to_json,
    format_text_report,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "source_recovery.py"
spec = importlib.util.spec_from_file_location("source_recovery_script", SCRIPT_PATH)
source_recovery_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(source_recovery_script)

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_source(
    db,
    identifier: str,
    *,
    source_type: str = "blog",
    status: str = "paused",
    active: int = 0,
    last_fetch_status: str | None = "failure",
    failures: int = 1,
    failure_days_ago: int | None = 8,
    success_days_ago: int | None = 20,
    last_error: str | None = "timeout",
) -> int:
    db.sync_config_sources(
        [{"identifier": identifier, "name": identifier, "license": "open"}],
        source_type,
    )
    row = db.get_curated_source(source_type, identifier)
    last_failure_at = (
        (NOW - timedelta(days=failure_days_ago)).isoformat()
        if failure_days_ago is not None
        else None
    )
    last_success_at = (
        (NOW - timedelta(days=success_days_ago)).isoformat()
        if success_days_ago is not None
        else None
    )
    db.conn.execute(
        """UPDATE curated_sources
           SET status = ?,
               active = ?,
               last_fetch_status = ?,
               consecutive_failures = ?,
               last_failure_at = ?,
               last_success_at = ?,
               last_error = ?
           WHERE id = ?""",
        (
            status,
            active,
            last_fetch_status,
            failures,
            last_failure_at,
            last_success_at,
            last_error,
            row["id"],
        ),
    )
    db.conn.commit()
    return row["id"]


def test_buckets_paused_and_quarantined_sources_with_reason_codes(db):
    retry_id = _insert_source(db, "retry.example", failures=2, failure_days_ago=10)
    wait_id = _insert_source(
        db,
        "wait.example",
        status="active",
        active=1,
        last_fetch_status="quarantined",
        failures=2,
        failure_days_ago=2,
    )
    manual_id = _insert_source(
        db,
        "manual.example",
        failures=7,
        failure_days_ago=12,
    )
    _insert_source(
        db,
        "healthy.example",
        status="active",
        active=1,
        last_fetch_status="success",
        failures=0,
        failure_days_ago=None,
        success_days_ago=1,
        last_error=None,
    )

    plan = build_source_recovery_plan(db, stale_days=7, max_failures=5, now=NOW)

    assert [source.id for source in plan.retry] == [retry_id]
    assert plan.retry[0].reason_codes == [
        "stale_failure",
        "within_failure_budget",
        "success_history",
    ]
    assert [source.id for source in plan.wait] == [wait_id]
    assert plan.wait[0].reason_codes == ["failure_too_recent"]
    assert [source.id for source in plan.manual_review] == [manual_id]
    assert plan.manual_review[0].reason_codes == ["too_many_failures"]
    assert plan.bucket_counts == {"retry": 1, "wait": 1, "manual_review": 1}
    assert plan.considered_count == 3


def test_retry_candidates_sort_by_source_type_oldest_failure_and_lowest_failures(db):
    blog_newer = _insert_source(db, "b-new.example", failures=1, failure_days_ago=8)
    blog_old_low = _insert_source(db, "b-old-low.example", failures=1, failure_days_ago=20)
    blog_old_high = _insert_source(db, "b-old-high.example", failures=4, failure_days_ago=20)
    newsletter = _insert_source(
        db,
        "news.example",
        source_type="newsletter",
        failures=1,
        failure_days_ago=30,
    )
    x_account = _insert_source(
        db,
        "acct",
        source_type="x_account",
        failures=1,
        failure_days_ago=40,
    )

    plan = build_source_recovery_plan(db, stale_days=7, max_failures=5, now=NOW)

    assert [source.id for source in plan.retry] == [
        blog_old_low,
        blog_old_high,
        blog_newer,
        newsletter,
        x_account,
    ]


def test_manual_review_for_missing_failure_time_and_missing_success_history(db):
    missing_failure = _insert_source(db, "missing-failure.example", failure_days_ago=None)
    no_success = _insert_source(db, "no-success.example", success_days_ago=None)

    plan = build_source_recovery_plan(db, now=NOW)

    by_id = {source.id: source for source in plan.manual_review}
    assert by_id[missing_failure].reason_codes == ["missing_failure_at"]
    assert by_id[no_success].reason_codes == ["no_success_history"]


def test_source_type_filter_and_stable_json_output(db):
    _insert_source(db, "blog.example", source_type="blog")
    newsletter_id = _insert_source(db, "news.example", source_type="newsletter")

    plan = build_source_recovery_plan(
        db,
        source_type="newsletter",
        stale_days=7,
        max_failures=5,
        now=NOW,
    )
    payload = json.loads(export_to_json(plan))

    assert [source["id"] for source in payload["retry"]] == [newsletter_id]
    assert payload["source_type"] == "newsletter"
    assert list(payload.keys()) == sorted(payload.keys())


def test_text_report_is_stable_when_empty(db):
    report = format_text_report(build_source_recovery_plan(db, now=NOW))

    assert "Source Recovery Plan" in report
    assert "retry=0 wait=0 manual_review=0" in report
    assert "No paused or quarantined curated sources found." in report


def test_missing_table_returns_stable_empty_plan():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    plan = build_source_recovery_plan(conn, now=NOW)

    assert plan.missing_required_tables == ["curated_sources"]
    assert plan.considered_count == 0
    assert json.loads(export_to_json(plan))["missing_required_tables"] == [
        "curated_sources"
    ]


def test_cli_outputs_text_and_json(db, capsys):
    _insert_source(db, "retry.example", failures=2, failure_days_ago=10)

    with patch.object(
        source_recovery_script,
        "script_context",
        return_value=_script_context(db),
    ):
        exit_code = source_recovery_script.main(["--format", "json"])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "source_recovery_plan"
    assert payload["bucket_counts"]["retry"] == 1
    assert payload["retry"][0]["identifier"] == "retry.example"

    with patch.object(
        source_recovery_script,
        "script_context",
        return_value=_script_context(db),
    ):
        exit_code = source_recovery_script.main(["--source-type", "blog"])

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "Source Recovery Plan" in output
    assert "retry.example" in output
