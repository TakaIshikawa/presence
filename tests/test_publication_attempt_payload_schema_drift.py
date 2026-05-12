"""Tests for publication attempt payload schema drift reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3

from evaluation.publication_attempt_payload_schema_drift import (
    build_publication_attempt_payload_schema_drift_report,
    format_publication_attempt_payload_schema_drift_json,
    format_publication_attempt_payload_schema_drift_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)


def _content(db) -> int:
    return int(
        db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            content="payload",
            eval_score=8.0,
            eval_feedback="ok",
        )
    )


def _attempt(db, *, platform: str = "x", success: bool = True, payload=None) -> int:
    return int(
        db.record_publication_attempt(
            queue_id=None,
            content_id=_content(db),
            platform=platform,
            attempted_at="2026-05-01T10:00:00+00:00",
            success=success,
            response_metadata=payload,
        )
    )


def test_groups_key_observations_by_platform_and_status(db):
    _attempt(db, platform="x", success=True, payload={"id": "1", "url": "u", "text": "a"})
    _attempt(db, platform="x", success=True, payload={"id": "2", "url": "u", "debug": True})
    _attempt(db, platform="bluesky", success=False, payload={"uri": "at://1", "error": "bad"})

    report = build_publication_attempt_payload_schema_drift_report(
        db,
        common_key_ratio=0.5,
        rare_key_ratio=0.5,
        now=NOW,
    )

    assert [(row.platform, row.status) for row in report.observations] == [
        ("bluesky", "failure"),
        ("x", "success"),
    ]
    x_row = report.observations[1]
    assert x_row.common_keys == ("debug", "id", "text", "url")
    assert x_row.missing_common_keys == ("debug", "text")
    assert x_row.rare_keys == ("debug", "text")


def test_malformed_json_rows_are_counted_and_formatted(db):
    attempt_id = _attempt(db, payload={"ok": True})
    db.conn.execute(
        "UPDATE publication_attempts SET response_metadata = ? WHERE id = ?",
        ("{not-json", attempt_id),
    )
    db.conn.commit()

    report = build_publication_attempt_payload_schema_drift_report(db, now=NOW)
    payload = json.loads(format_publication_attempt_payload_schema_drift_json(report))
    text = format_publication_attempt_payload_schema_drift_text(report)

    assert payload["artifact_type"] == "publication_attempt_payload_schema_drift"
    assert payload["totals"]["malformed_payload_count"] == 1
    assert payload["malformed_payloads"][0]["attempt_id"] == attempt_id
    assert "Malformed payloads:" in text
    assert f"attempt={attempt_id}" in text


def test_missing_schema_is_reported_without_raising():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    try:
        report = build_publication_attempt_payload_schema_drift_report(conn, now=NOW)
    finally:
        conn.close()

    assert report.missing_tables == ("publication_attempts",)
    assert report.observations == ()
