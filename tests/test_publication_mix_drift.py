"""Tests for published content mix drift reporting."""

from __future__ import annotations

import json
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from evaluation.publication_mix_drift import (  # noqa: E402
    build_publication_mix_drift_report,
    format_publication_mix_drift_text,
)
from publication_mix_drift import main  # noqa: E402


BASE_TIME = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)


def _publish(
    db,
    *,
    content_type: str,
    content_format: str | None,
    platform: str,
    days_ago: float,
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=["abc123"],
        source_messages=["uuid-1"],
        content=f"{content_type} {content_format or 'unknown'} {platform}",
        eval_score=8.0,
        eval_feedback="ok",
        content_format=content_format,
    )
    db.upsert_publication_success(
        content_id,
        platform,
        published_at=(BASE_TIME - timedelta(days=days_ago)).isoformat(),
    )
    return content_id


def test_report_compares_recent_to_immediate_baseline_by_mix_dimensions(db):
    _publish(db, content_type="x_post", content_format="tip", platform="x", days_ago=1)
    _publish(db, content_type="x_thread", content_format="thread", platform="x", days_ago=2)
    _publish(
        db,
        content_type="x_visual",
        content_format="annotated",
        platform="bluesky",
        days_ago=8,
    )
    _publish(db, content_type="x_post", content_format="tip", platform="x", days_ago=9)

    report = build_publication_mix_drift_report(
        db,
        recent_days=7,
        baseline_days=7,
        drift_warning_points=40,
        now=BASE_TIME,
    )

    assert report["totals"] == {"recent": 2, "baseline": 2}
    assert report["windows"]["baseline"]["end"] == report["windows"]["recent"]["start"]

    by_type = {entry["value"]: entry for entry in report["dimensions"]["content_type"]}
    assert by_type["x_post"]["recent_count"] == 1
    assert by_type["x_post"]["baseline_count"] == 1
    assert by_type["x_thread"]["recent_share"] == 0.5
    assert by_type["x_thread"]["drift_points"] == 50.0

    by_format = {
        entry["value"]: entry for entry in report["dimensions"]["content_format"]
    }
    assert by_format["thread"]["recent_count"] == 1
    assert by_format["thread"]["baseline_count"] == 0

    by_platform = {entry["value"]: entry for entry in report["dimensions"]["platform"]}
    assert by_platform["x"]["recent_count"] == 2
    assert by_platform["bluesky"]["baseline_count"] == 1


def test_report_flags_missing_recent_type_and_high_positive_drift(db):
    _publish(db, content_type="x_thread", content_format="thread", platform="x", days_ago=1)
    _publish(db, content_type="x_thread", content_format="thread", platform="x", days_ago=2)
    _publish(db, content_type="x_visual", content_format="image", platform="x", days_ago=8)
    _publish(
        db,
        content_type="x_visual",
        content_format="image",
        platform="bluesky",
        days_ago=9,
    )

    report = build_publication_mix_drift_report(
        db,
        recent_days=7,
        baseline_days=7,
        drift_warning_points=30,
        now=BASE_TIME,
    )

    warnings = {(item["label"], item["dimension"], item["value"]) for item in report["warnings"]}
    assert ("missing_recent_type", "content_type", "x_visual") in warnings
    assert ("high_positive_drift", "content_type", "x_thread") in warnings
    assert ("high_positive_drift", "content_format", "thread") in warnings
    assert ("high_positive_drift", "platform", "x") in warnings

    missing_type = next(
        warning
        for warning in report["warnings"]
        if warning["label"] == "missing_recent_type"
    )
    assert missing_type["recent_count"] == 0
    assert missing_type["baseline_share"] == 1.0


def test_unpublished_and_outside_windows_are_excluded(db):
    _publish(db, content_type="x_post", content_format="tip", platform="x", days_ago=1)
    _publish(db, content_type="blog_post", content_format="essay", platform="x", days_ago=20)
    content_id = db.insert_generated_content(
        content_type="x_visual",
        source_commits=[],
        source_messages=[],
        content="queued only",
        eval_score=8.0,
        eval_feedback="ok",
        content_format="image",
    )
    db.conn.execute(
        """INSERT INTO content_publications
           (content_id, platform, status, published_at)
           VALUES (?, ?, ?, ?)""",
        (
            content_id,
            "bluesky",
            "queued",
            (BASE_TIME - timedelta(days=1)).isoformat(),
        ),
    )
    db.conn.commit()

    report = build_publication_mix_drift_report(
        db,
        recent_days=7,
        baseline_days=7,
        now=BASE_TIME,
    )

    assert report["totals"] == {"recent": 1, "baseline": 0}
    assert [entry["value"] for entry in report["dimensions"]["content_type"]] == [
        "x_post"
    ]


def test_text_output_is_stable_for_empty_database():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_publication_mix_drift_report(conn, now=BASE_TIME)
    text = format_publication_mix_drift_text(report)

    assert report["totals"] == {"recent": 0, "baseline": 0}
    assert "Publication content mix drift report" in text
    assert "No published content found in either window." in text


def test_cli_supports_json_format_and_threshold_flags(db, capsys):
    _publish(db, content_type="x_thread", content_format="thread", platform="x", days_ago=1)
    fixed_report = build_publication_mix_drift_report(
        db,
        recent_days=3,
        baseline_days=4,
        drift_warning_points=25,
        now=BASE_TIME,
    )

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("publication_mix_drift.script_context", fake_script_context), patch(
        "publication_mix_drift.build_publication_mix_drift_report",
        return_value=fixed_report,
    ):
        result = main(
            [
                "--recent-days",
                "3",
                "--baseline-days",
                "4",
                "--drift-warning-points",
                "25",
                "--format",
                "json",
            ]
        )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["recent_days"] == 3
    assert payload["baseline_days"] == 4
    assert payload["thresholds"]["drift_warning_points"] == 25
