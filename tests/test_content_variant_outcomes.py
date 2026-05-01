"""Tests for content variant outcome reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

from evaluation.content_variant_outcomes import (
    build_content_variant_outcome_report,
    format_content_variant_outcome_json,
    format_content_variant_outcome_text,
)


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "content_variant_outcomes.py"
)
spec = importlib.util.spec_from_file_location("content_variant_outcomes", SCRIPT_PATH)
content_variant_outcomes = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(content_variant_outcomes)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, content_type: str = "x_post") -> int:
    return db.conn.execute(
        """INSERT INTO generated_content
           (content, content_type, eval_score, published)
           VALUES (?, ?, 7.0, 1)""",
        (f"{content_type} body", content_type),
    ).lastrowid


def _variant(
    db,
    content_id: int,
    *,
    platform: str,
    variant_type: str,
    selected: bool = False,
    created_at: datetime | None = None,
) -> int:
    variant_id = db.upsert_content_variant(
        content_id,
        platform=platform,
        variant_type=variant_type,
        content=f"{platform} {variant_type}",
    )
    if selected:
        db.select_content_variant(content_id, platform, variant_type)
    db.conn.execute(
        "UPDATE content_variants SET created_at = ? WHERE id = ?",
        ((created_at or NOW - timedelta(days=1)).isoformat(), variant_id),
    )
    db.conn.commit()
    return variant_id


def _publish(db, content_id: int, platform: str, published_at: datetime | None = None) -> None:
    db.upsert_publication_success(
        content_id,
        platform=platform,
        platform_post_id=f"{platform}-{content_id}",
        published_at=(published_at or NOW - timedelta(hours=12)).isoformat(),
    )


def _x_engagement(db, content_id: int, score: float, fetched_at: datetime | None = None) -> None:
    db.insert_engagement(content_id, f"tweet-{content_id}", 0, 0, 0, 0, score)
    db.conn.execute(
        "UPDATE post_engagement SET fetched_at = ? WHERE content_id = ?",
        ((fetched_at or NOW).isoformat(), content_id),
    )
    db.conn.commit()


def test_selected_variant_groups_count_low_and_missing_engagement_separately(db):
    low_one = _content(db)
    low_two = _content(db)
    missing = _content(db)
    available = _content(db)

    for content_id in (low_one, low_two, missing):
        _variant(db, content_id, platform="x", variant_type="hook", selected=True)
        _publish(db, content_id, "x")
    _variant(db, available, platform="x", variant_type="question", selected=False)
    _publish(db, available, "x")

    _x_engagement(db, low_one, 2.0)
    _x_engagement(db, low_two, 3.0)

    report = build_content_variant_outcome_report(db, days=30, min_sample=3, now=NOW)

    assert [
        (group.platform, group.variant_type, group.selection_status)
        for group in report.groups
    ] == [
        ("x", "hook", "selected"),
        ("x", "question", "unselected"),
    ]
    selected = report.groups[0]
    assert selected.variant_count == 3
    assert selected.published_count == 3
    assert selected.engagement_snapshot_count == 2
    assert selected.no_engagement_count == 1
    assert selected.low_engagement_count == 2
    assert selected.average_engagement_score == 2.5
    assert selected.recommendation == "review_variant_type"
    assert report.totals["weak_group_count"] == 1
    assert report.recommendations == (
        "Review x/hook: 3 selected variants meet min_sample=3, "
        "avg engagement 2.50 is below 5.0.",
    )

    unselected = report.groups[1]
    assert unselected.selection_status == "unselected"
    assert unselected.engagement_snapshot_count == 0
    assert unselected.sample_status == "low_sample"


def test_filters_json_and_text_output_are_deterministic(db, capsys):
    x_content = _content(db)
    bluesky_content = _content(db)
    _variant(db, x_content, platform="x", variant_type="hook", selected=True)
    _variant(db, bluesky_content, platform="bluesky", variant_type="post", selected=True)
    _publish(db, x_content, "x")
    _publish(db, bluesky_content, "bluesky")
    _x_engagement(db, x_content, 8.0)
    db.insert_bluesky_engagement(bluesky_content, "at://test/post/1", 5, 1, 0, 0, 8.0)
    db.conn.execute(
        "UPDATE bluesky_engagement SET fetched_at = ? WHERE content_id = ?",
        (NOW.isoformat(), bluesky_content),
    )
    db.conn.commit()

    report = build_content_variant_outcome_report(
        db,
        days=30,
        platform="x",
        variant_type="hook",
        min_sample=1,
        now=NOW,
    )

    assert len(report.groups) == 1
    assert report.groups[0].platform == "x"
    assert report.groups[0].variant_type == "hook"
    assert report.groups[0].recommendation == "keep_using"
    assert format_content_variant_outcome_json(
        report
    ) == format_content_variant_outcome_json(report)
    payload = json.loads(format_content_variant_outcome_json(report))
    assert sorted(payload) == [
        "availability",
        "filters",
        "generated_at",
        "groups",
        "missing_columns",
        "missing_tables",
        "recommendations",
        "totals",
    ]
    assert payload["filters"]["platform"] == "x"
    assert payload["groups"][0]["average_engagement_score"] == 8.0
    text = format_content_variant_outcome_text(report)
    assert "Content Variant Outcomes" in text
    assert "x/hook/selected" in text

    with patch.object(
        content_variant_outcomes,
        "script_context",
        wraps=lambda: _script_context(db),
    ):
        exit_code = content_variant_outcomes.main(
            [
                "--days",
                "30",
                "--platform",
                "x",
                "--variant-type",
                "hook",
                "--min-sample",
                "1",
                "--format",
                "json",
            ]
        )

    assert exit_code == 0
    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["filters"]["variant_type"] == "hook"
    assert cli_payload["totals"]["variant_count"] == 1


def test_missing_optional_tables_reports_availability_metadata():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """CREATE TABLE content_variants (
            id INTEGER PRIMARY KEY,
            content_id INTEGER NOT NULL,
            platform TEXT NOT NULL,
            variant_type TEXT NOT NULL,
            selected INTEGER NOT NULL DEFAULT 0,
            created_at TEXT
        )"""
    )
    conn.execute(
        """INSERT INTO content_variants
           (id, content_id, platform, variant_type, selected, created_at)
           VALUES (1, 10, 'linkedin', 'post', 1, ?)""",
        ((NOW - timedelta(days=1)).isoformat(),),
    )
    conn.commit()
    try:
        report = build_content_variant_outcome_report(
            conn,
            days=30,
            min_sample=1,
            now=NOW,
        )
    finally:
        conn.close()

    assert report.groups[0].platform == "linkedin"
    assert report.groups[0].published_count == 0
    assert report.availability["content_publications"] is False
    assert "content_publications" in report.missing_tables
    assert "linkedin_engagement" in report.missing_tables
