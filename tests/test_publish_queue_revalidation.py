"""Tests for publish queue revalidation planning."""

from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from output.publish_queue_revalidation import (
    format_publish_queue_revalidation_json,
    format_publish_queue_revalidation_text,
    plan_publish_queue_revalidation,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "plan_queue_revalidation.py"
spec = importlib.util.spec_from_file_location("plan_queue_revalidation", SCRIPT_PATH)
plan_queue_revalidation = importlib.util.module_from_spec(spec)
sys.modules["plan_queue_revalidation"] = plan_queue_revalidation
spec.loader.exec_module(plan_queue_revalidation)

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


def _content(
    db,
    *,
    text: str = "Queued content",
    eval_score: float = 8.0,
    claim_check: dict | None = None,
    persona_guard: dict | None = None,
) -> int:
    return db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=eval_score,
        eval_feedback="ok",
        claim_check_summary=claim_check if claim_check is not None else {
            "supported_count": 1,
            "unsupported_count": 0,
        },
        persona_guard_summary=persona_guard if persona_guard is not None else {
            "checked": True,
            "passed": True,
            "status": "passed",
            "score": 0.9,
        },
    )


def _queue(
    db,
    *,
    content_id: int | None = None,
    platform: str = "x",
    status: str = "queued",
    scheduled_at: datetime | None = None,
) -> int:
    content_id = content_id or _content(db)
    return db.conn.execute(
        """INSERT INTO publish_queue
           (content_id, scheduled_at, platform, status, hold_reason)
           VALUES (?, ?, ?, ?, ?)""",
        (
            content_id,
            (scheduled_at or NOW - timedelta(hours=1)).isoformat(),
            platform,
            status,
            "manual review" if status == "held" else None,
        ),
    ).lastrowid


def _recommendations(report: dict) -> dict[int, str]:
    return {
        item["content_id"]: item["recommendation"]
        for item in report["items"]
    }


def _reason_codes(item: dict) -> list[str]:
    return [reason["code"] for reason in item["reasons"]]


def test_empty_database_returns_stable_empty_report(db):
    report = plan_publish_queue_revalidation(db, now=NOW)

    assert report["scanned_count"] == 0
    assert report["recommendation_counts"] == {
        "publish": 0,
        "re_evaluate": 0,
        "regenerate": 0,
        "cancel": 0,
    }
    assert "No queued or held publish queue items" in format_publish_queue_revalidation_text(report)


def test_mixed_queue_statuses_and_recommendations(db):
    publish_id = _content(db, text="Ready to publish")
    low_score_id = _content(db, text="Weak draft", eval_score=4.5)
    unsupported_id = _content(
        db,
        text="Unsupported claim",
        claim_check={"supported_count": 0, "unsupported_count": 1},
    )
    persona_id = _content(
        db,
        text="Persona drift",
        persona_guard={
            "checked": True,
            "passed": False,
            "status": "failed",
            "score": 0.2,
            "reasons": ["too generic"],
        },
    )
    old_id = _content(db, text="Old copy")
    repeated_id = _content(db, text="Repeated failures")
    published_id = _content(db, text="Already done")

    _queue(db, content_id=publish_id, status="queued")
    _queue(db, content_id=low_score_id, status="queued")
    _queue(db, content_id=unsupported_id, status="held")
    _queue(db, content_id=persona_id, status="queued")
    _queue(db, content_id=old_id, scheduled_at=NOW - timedelta(hours=80))
    _queue(db, content_id=repeated_id, status="queued")
    _queue(db, content_id=published_id, status="published")
    _queue(db, status="cancelled")

    db.conn.execute(
        """INSERT INTO content_publications
           (content_id, platform, status, attempt_count, error)
           VALUES (?, 'x', 'failed', 3, 'rate limit')""",
        (repeated_id,),
    )
    for index in range(3):
        db.conn.execute(
            """INSERT INTO publication_attempts
               (content_id, platform, attempted_at, success, error)
               VALUES (?, 'x', ?, 0, 'rate limit')""",
            (repeated_id, (NOW - timedelta(minutes=index)).isoformat()),
        )
    db.conn.commit()

    report = plan_publish_queue_revalidation(db, now=NOW)

    assert report["scanned_count"] == 6
    assert _recommendations(report) == {
        publish_id: "publish",
        low_score_id: "regenerate",
        unsupported_id: "regenerate",
        persona_id: "regenerate",
        old_id: "regenerate",
        repeated_id: "cancel",
    }
    by_content = {item["content_id"]: item for item in report["items"]}
    assert _reason_codes(by_content[low_score_id]) == ["low_eval_score"]
    assert _reason_codes(by_content[unsupported_id]) == ["unsupported_claims"]
    assert _reason_codes(by_content[persona_id]) == ["persona_guard_failed"]
    assert _reason_codes(by_content[old_id]) == ["excessive_age"]
    assert "repeated_publish_failures" in _reason_codes(by_content[repeated_id])


def test_filters_by_status_platform_age_and_limit(db):
    old_x = _queue(
        db,
        content_id=_content(db, text="old x"),
        platform="x",
        status="held",
        scheduled_at=NOW - timedelta(hours=5),
    )
    _queue(
        db,
        content_id=_content(db, text="new x"),
        platform="x",
        status="held",
        scheduled_at=NOW - timedelta(hours=1),
    )
    _queue(
        db,
        content_id=_content(db, text="old bsky"),
        platform="bluesky",
        status="held",
        scheduled_at=NOW - timedelta(hours=6),
    )

    report = plan_publish_queue_revalidation(
        db,
        status="held",
        platform="x",
        min_age_hours=2,
        limit=1,
        now=NOW,
    )

    assert [item["queue_id"] for item in report["items"]] == [old_x]


def test_missing_optional_quality_tables_degrade_gracefully():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            content TEXT,
            content_type TEXT,
            eval_score REAL,
            eval_feedback TEXT
        )"""
    )
    conn.execute(
        """CREATE TABLE publish_queue (
            id INTEGER PRIMARY KEY,
            content_id INTEGER NOT NULL,
            scheduled_at TEXT,
            platform TEXT,
            status TEXT,
            created_at TEXT
        )"""
    )
    conn.execute(
        """INSERT INTO generated_content
           (id, content, content_type, eval_score, eval_feedback)
           VALUES (1, 'Minimal row', 'x_post', 8.0, 'ok')"""
    )
    conn.execute(
        """INSERT INTO publish_queue
           (id, content_id, scheduled_at, platform, status, created_at)
           VALUES (1, 1, ?, 'x', 'queued', ?)""",
        ((NOW - timedelta(hours=1)).isoformat(), NOW.isoformat()),
    )

    report = plan_publish_queue_revalidation(conn, now=NOW)

    assert report["items"][0]["recommendation"] == "publish"
    assert report["items"][0]["reasons"] == []
    assert "content_claim_checks" in report["missing_optional_tables"]


def test_cli_json_output(db, capsys):
    queue_id = _queue(db, content_id=_content(db, text="Ready"), platform="x")

    @contextmanager
    def fake_script_context():
        yield SimpleNamespace(), db

    with patch.object(plan_queue_revalidation, "script_context", fake_script_context), patch.object(
        plan_queue_revalidation,
        "plan_publish_queue_revalidation",
        wraps=lambda db, **kwargs: plan_publish_queue_revalidation(db, now=NOW, **kwargs),
    ):
        assert plan_queue_revalidation.main(["--platform", "x", "--format", "json"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["items"][0]["queue_id"] == queue_id
    assert payload["items"][0]["recommendation"] == "publish"
    assert json.loads(format_publish_queue_revalidation_json(payload))["scanned_count"] == 1
