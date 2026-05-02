"""Tests for publish platform failover planning."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

from output.publish_failover import (
    build_publish_failover_report,
    format_publish_failover_json,
    format_publish_failover_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "plan_publish_failover.py"
spec = importlib.util.spec_from_file_location("plan_publish_failover_script", SCRIPT_PATH)
plan_publish_failover_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(plan_publish_failover_script)

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _iso(hours_ago: int) -> str:
    return (NOW - timedelta(hours=hours_ago)).isoformat()


def _content(db, text: str) -> int:
    cursor = db.conn.execute(
        """INSERT INTO generated_content
           (content_type, content, eval_score, retry_count, created_at)
           VALUES ('x_post', ?, 8, 0, ?)""",
        (text, _iso(72)),
    )
    return int(cursor.lastrowid)


def _variant(
    db,
    content_id: int,
    *,
    platform: str,
    variant_type: str = "post",
    selected: bool = False,
) -> int:
    variant_id = db.upsert_content_variant(
        content_id,
        platform=platform,
        variant_type=variant_type,
        content=f"{platform} failover copy",
    )
    if selected:
        db.select_content_variant(content_id, platform, variant_type)
    db.conn.execute(
        "UPDATE content_variants SET created_at = ? WHERE id = ?",
        (_iso(24), variant_id),
    )
    return int(variant_id)


def _publication(
    db,
    content_id: int,
    *,
    platform: str,
    status: str,
    error: str | None = "Gateway timeout",
    error_category: str | None = "network",
    attempt_count: int = 2,
    hours_ago: int = 1,
) -> int:
    cursor = db.conn.execute(
        """INSERT INTO content_publications
           (content_id, platform, status, error, error_category, attempt_count,
            last_error_at, published_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            content_id,
            platform,
            status,
            error,
            error_category,
            attempt_count,
            _iso(hours_ago) if status != "published" else None,
            _iso(hours_ago) if status == "published" else None,
            _iso(hours_ago),
        ),
    )
    return int(cursor.lastrowid)


def _queue(
    db,
    content_id: int,
    *,
    platform: str,
    status: str,
    hold_reason: str | None = None,
    error: str | None = None,
) -> int:
    cursor = db.conn.execute(
        """INSERT INTO publish_queue
           (content_id, scheduled_at, platform, status, hold_reason, error)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (content_id, _iso(2), platform, status, hold_reason, error),
    )
    return int(cursor.lastrowid)


def test_recommends_alternate_variant_for_failed_source_with_high_failure_rate(db):
    content_id = _content(db, "X failed but Bluesky copy exists")
    publication_id = _publication(db, content_id, platform="x", status="failed")
    variant_id = _variant(db, content_id, platform="bluesky", selected=True)
    db.conn.commit()

    report = build_publish_failover_report(db, now=NOW)

    assert report["artifact_type"] == "publish_failover_plan"
    assert report["items"] == [
        {
            "content_id": content_id,
            "source_platform": "x",
            "recommended_platform": "bluesky",
            "variant_id": variant_id,
            "variant_type": "post",
            "source_status": "failed",
            "failure_context": "network: Gateway timeout",
            "error_category": "network",
            "confidence_score": 0.99,
            "reason_codes": [
                "source_status_stuck",
                "alternate_variant_available",
                "source_failure_rate_high",
                "target_not_published",
                "selected_variant",
            ],
            "publication_id": publication_id,
            "queue_id": None,
            "attempt_count": 2,
            "last_error_at": _iso(1),
            "hold_reason": None,
            "source_failure_rate": 1.0,
            "source_failure_count": 1,
            "source_total_count": 1,
        }
    ]
    assert report["totals"]["by_source_platform"] == {"bluesky": 0, "x": 1}
    assert report["totals"]["by_recommended_platform"] == {"bluesky": 1, "x": 0}


def test_excludes_target_platform_that_already_published_same_content(db):
    content_id = _content(db, "Already succeeded elsewhere")
    _publication(db, content_id, platform="x", status="failed")
    _variant(db, content_id, platform="bluesky", selected=True)
    _publication(
        db,
        content_id,
        platform="bluesky",
        status="published",
        error=None,
        error_category=None,
    )
    db.conn.commit()

    report = build_publish_failover_report(db, now=NOW)

    assert report["items"] == []


def test_requires_recent_high_failure_rate_before_recommending_failover(db):
    content_id = _content(db, "One failure among many successes")
    _publication(db, content_id, platform="x", status="failed")
    _variant(db, content_id, platform="bluesky")
    for index in range(3):
        published = _content(db, f"Recent x success {index}")
        _publication(
            db,
            published,
            platform="x",
            status="published",
            error=None,
            error_category=None,
            hours_ago=2 + index,
        )
    db.conn.commit()

    report = build_publish_failover_report(db, now=NOW, min_confidence=0)

    assert report["items"] == []


def test_held_queue_items_include_failure_context_and_queue_id(db):
    content_id = _content(db, "Held on X")
    queue_id = _queue(
        db,
        content_id,
        platform="x",
        status="held",
        hold_reason="campaign paused",
    )
    _variant(db, content_id, platform="bluesky")
    db.conn.commit()

    report = build_publish_failover_report(db, platform="x", now=NOW)

    assert len(report["items"]) == 1
    item = report["items"][0]
    assert item["queue_id"] == queue_id
    assert item["publication_id"] is None
    assert item["source_status"] == "held"
    assert item["failure_context"] == "held: campaign paused"
    assert item["reason_codes"][:4] == [
        "source_status_stuck",
        "alternate_variant_available",
        "source_failure_rate_high",
        "target_not_published",
    ]


def test_days_platform_and_min_confidence_filters_are_applied(db):
    fresh = _content(db, "Fresh x failure")
    old = _content(db, "Old x failure")
    bluesky = _content(db, "Bluesky failure")
    _publication(db, fresh, platform="x", status="failed", hours_ago=2)
    _variant(db, fresh, platform="bluesky")
    _publication(db, old, platform="x", status="failed", hours_ago=24 * 10)
    _variant(db, old, platform="bluesky")
    _publication(db, bluesky, platform="bluesky", status="failed", hours_ago=2)
    _variant(db, bluesky, platform="x")
    db.conn.commit()

    report = build_publish_failover_report(
        db,
        platform="x",
        days=3,
        min_confidence=0.91,
        now=NOW,
    )

    assert [item["content_id"] for item in report["items"]] == []

    relaxed = build_publish_failover_report(
        db,
        platform="x",
        days=3,
        min_confidence=0.7,
        now=NOW,
    )

    assert [item["content_id"] for item in relaxed["items"]] == [fresh]


def test_json_text_and_cli_outputs_are_stable(db, capsys):
    content_id = _content(db, "Format me")
    _publication(db, content_id, platform="x", status="failed")
    _variant(db, content_id, platform="bluesky")
    db.conn.commit()

    report = build_publish_failover_report(db, now=NOW)

    payload = json.loads(format_publish_failover_json(report))
    assert payload["filters"]["platform"] == "all"
    assert format_publish_failover_text(report) == "\n".join(
        [
            "Publish Failover Planner",
            "Generated: 2026-05-01T12:00:00+00:00",
            "Filters: platform=all days=7 min_confidence=0.7",
            "Total: 1",
            "",
            "Items:",
            "  Content  From      To        Variant  Confidence  Context",
            "  -------  --------  --------  -------  ----------  ------------------------------",
            f"  {content_id:<7}  x         bluesky   {report['items'][0]['variant_id']:<7}  0.90        network: Gateway timeout",
        ]
    )

    with patch.object(
        plan_publish_failover_script,
        "script_context",
        wraps=lambda: _script_context(db),
    ):
        rc = plan_publish_failover_script.main(
            ["--platform", "x", "--days", "7", "--min-confidence", "0.7", "--json"]
        )

    assert rc == 0
    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["items"][0]["recommended_platform"] == "bluesky"


def test_missing_required_schema_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE generated_content (id INTEGER PRIMARY KEY, content TEXT)")

    report = build_publish_failover_report(conn, now=NOW)

    assert report["items"] == []
    assert report["missing_required"] == [
        "content_publications_or_publish_queue",
        "content_variants",
    ]
