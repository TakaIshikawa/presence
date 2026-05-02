"""Tests for claim-check staleness planning."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.claim_check_staleness import (
    MISSING_CLAIM_CHECK,
    STALE_PASSED_CHECK,
    UNRESOLVED_FAILED_CHECK,
    UNVERIFIABLE_CLAIM,
    build_claim_check_staleness_plan,
    format_claim_check_staleness_json,
    format_claim_check_staleness_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "plan_claim_check_staleness.py"
)
spec = importlib.util.spec_from_file_location("plan_claim_check_staleness_script", SCRIPT_PATH)
plan_claim_check_staleness_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(plan_claim_check_staleness_script)


def _content(
    db,
    text: str,
    *,
    content_type: str = "x_post",
    created_at: datetime = NOW,
    published: int = 0,
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ?, published = ? WHERE id = ?",
        (created_at.isoformat(), published, content_id),
    )
    db.conn.commit()
    return int(content_id)


def _claim_check(
    db,
    content_id: int,
    *,
    supported_count: int,
    unsupported_count: int,
    updated_at: datetime = NOW,
) -> None:
    db.save_claim_check_summary(
        content_id,
        supported_count=supported_count,
        unsupported_count=unsupported_count,
        annotation_text="needs review" if unsupported_count else None,
    )
    db.conn.execute(
        "UPDATE content_claim_checks SET created_at = ?, updated_at = ? WHERE content_id = ?",
        (updated_at.isoformat(), updated_at.isoformat(), content_id),
    )
    db.conn.commit()


def _items_by_id(report):
    return {item.content_id: item for item in report.items}


def test_classifies_missing_stale_failed_and_unverifiable_claim_states(db):
    missing = _content(db, "missing", created_at=NOW - timedelta(days=2))
    stale = _content(db, "stale", created_at=NOW - timedelta(days=3))
    failed = _content(db, "failed", created_at=NOW - timedelta(days=4))
    unverifiable = _content(db, "unverifiable", created_at=NOW - timedelta(days=5))
    current = _content(db, "current", created_at=NOW - timedelta(days=1))
    _claim_check(
        db,
        stale,
        supported_count=2,
        unsupported_count=0,
        updated_at=NOW - timedelta(days=8),
    )
    _claim_check(db, failed, supported_count=1, unsupported_count=1)
    _claim_check(db, unverifiable, supported_count=0, unsupported_count=0)
    _claim_check(db, current, supported_count=1, unsupported_count=0)

    report = build_claim_check_staleness_plan(
        db,
        days=30,
        stale_days=7,
        now=NOW,
    )
    items = _items_by_id(report)

    assert [item.claim_status for item in report.items] == [
        UNRESOLVED_FAILED_CHECK,
        MISSING_CLAIM_CHECK,
        UNVERIFIABLE_CLAIM,
        STALE_PASSED_CHECK,
    ]
    assert items[missing].claim_status == MISSING_CLAIM_CHECK
    assert items[missing].reason == "content has no claim-check summary"
    assert items[stale].claim_status == STALE_PASSED_CHECK
    assert items[stale].age_days == 3
    assert items[stale].supported_count == 2
    assert items[failed].claim_status == UNRESOLVED_FAILED_CHECK
    assert "unsupported claim" in items[failed].reason
    assert items[unverifiable].claim_status == UNVERIFIABLE_CLAIM
    assert items[unverifiable].recommended_action
    assert current not in items
    assert report.totals["by_claim_status"] == {
        MISSING_CLAIM_CHECK: 1,
        STALE_PASSED_CHECK: 1,
        UNRESOLVED_FAILED_CHECK: 1,
        UNVERIFIABLE_CLAIM: 1,
    }


def test_unpublished_content_is_default_and_include_published_opt_in(db):
    unpublished = _content(db, "unpublished")
    published = _content(db, "published", published=1)

    default_report = build_claim_check_staleness_plan(db, now=NOW)
    include_report = build_claim_check_staleness_plan(
        db,
        include_published=True,
        now=NOW,
    )

    assert [item.content_id for item in default_report.items] == [unpublished]
    assert {item.content_id for item in include_report.items} == {unpublished, published}
    assert _items_by_id(include_report)[published].published is True


def test_formatters_emit_text_and_deterministic_json(db):
    content_id = _content(db, "missing")
    report = build_claim_check_staleness_plan(db, now=NOW)

    text = format_claim_check_staleness_text(report)
    payload = json.loads(format_claim_check_staleness_json(report))

    assert "Claim Check Staleness Plan" in text
    assert f"content_id={content_id}" in text
    assert "action: Run claim check before publication." in text
    assert payload["artifact_type"] == "claim_check_staleness_plan"
    assert payload["items"][0]["content_id"] == content_id
    assert payload["items"][0]["claim_status"] == MISSING_CLAIM_CHECK
    assert payload["items"][0]["age_days"] == 0


def test_schema_without_claim_checks_degrades_gracefully():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE generated_content (
               id INTEGER PRIMARY KEY,
               content_type TEXT NOT NULL,
               content TEXT NOT NULL,
               created_at TEXT,
               published INTEGER DEFAULT 0
           )"""
    )
    conn.execute(
        "INSERT INTO generated_content (id, content_type, content, created_at) VALUES (1, 'x_post', 'x', ?)",
        (NOW.isoformat(),),
    )

    report = build_claim_check_staleness_plan(conn, now=NOW)

    assert report.items == ()
    assert report.missing_tables == ("content_claim_checks",)
    assert "content_claim_checks" in format_claim_check_staleness_text(report)
    assert json.loads(format_claim_check_staleness_json(report))["missing_tables"] == [
        "content_claim_checks"
    ]


def test_script_parses_db_and_json_format(file_db, capsys):
    content_id = _content(file_db, "missing")

    status = plan_claim_check_staleness_script.main(
        ["--db", str(file_db.db_path), "--days", "30", "--format", "json"]
    )
    output = capsys.readouterr().out
    payload = json.loads(output)

    assert status == 0
    assert payload["items"][0]["content_id"] == content_id
