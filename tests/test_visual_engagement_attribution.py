"""Tests for visual engagement attribution reporting."""

from __future__ import annotations

import json
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from visual_engagement_attribution import main  # noqa: E402
from evaluation.visual_engagement_attribution import (  # noqa: E402
    build_visual_engagement_attribution_report,
    format_visual_engagement_attribution_json,
    format_visual_engagement_attribution_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


def _content(
    db,
    *,
    content_type: str = "x_post",
    image_path: str | None = None,
    image_prompt: str | None = None,
    content_format: str | None = None,
    platform: str = "x",
    published_at: str = "2026-04-29T12:00:00+00:00",
    score: float = 0.0,
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=f"{content_type} content {score}",
        eval_score=8.0,
        eval_feedback="ok",
        content_format=content_format,
        image_path=image_path,
        image_prompt=image_prompt,
    )
    db.upsert_publication_success(
        content_id,
        platform,
        platform_post_id=f"{platform}-{content_id}",
        published_at=published_at,
    )
    if platform == "bluesky":
        db.insert_bluesky_engagement(
            content_id,
            f"at://example/post/{content_id}",
            0,
            0,
            0,
            0,
            score,
        )
    else:
        db.insert_engagement(
            content_id=content_id,
            tweet_id=f"tweet-{content_id}",
            like_count=0,
            retweet_count=0,
            reply_count=0,
            quote_count=0,
            engagement_score=score,
        )
    return content_id


def _cohort(report, group_by: str, **filters):
    for cohort in report.cohorts:
        if cohort.group_by != group_by:
            continue
        if all(getattr(cohort, key) == value for key, value in filters.items()):
            return cohort
    raise AssertionError(f"missing cohort {group_by} {filters}")


def test_report_compares_visual_and_non_visual_engagement_by_platform_and_type(db):
    visual_ids = [
        _content(
            db,
            content_type="x_visual",
            image_path=f"/tmp/visual-{index}.png",
            image_prompt="METRIC | dashboard tile",
            content_format="metric_card",
            score=10.0 + index,
        )
        for index in range(3)
    ]
    text_ids = [_content(db, content_type="x_post", score=3.0 + index) for index in range(3)]

    report = build_visual_engagement_attribution_report(db, days=30, min_sample=3, now=NOW)
    overall = _cohort(report, "overall")

    assert overall.status == "sufficient_sample"
    assert overall.visual_sample_count == 3
    assert overall.non_visual_sample_count == 3
    assert overall.visual_normalized_engagement_rate == 11.0
    assert overall.non_visual_normalized_engagement_rate == 4.0
    assert overall.engagement_delta == 7.0
    assert overall.visual_content_ids == tuple(visual_ids)
    assert overall.non_visual_content_ids == tuple(text_ids)
    assert report.totals["visual_sample_count"] == 3
    assert report.totals["non_visual_sample_count"] == 3
    assert "Totals: visual=3 non_visual=3 delta=+7.00" in format_visual_engagement_attribution_text(report)


def test_template_and_image_prompt_groups_are_present_when_metadata_exists(db):
    for index in range(2):
        _content(
            db,
            content_type="x_visual",
            image_path=f"/tmp/annotated-{index}.png",
            image_prompt="ANNOTATED | trace the workflow",
            content_format="annotated_card",
            score=8.0,
        )
        _content(db, content_type="x_post", content_format="annotated_card", score=4.0)

    report = build_visual_engagement_attribution_report(db, min_sample=2, now=NOW)
    template = _cohort(
        report,
        "template",
        platform="x",
        content_type="x_visual",
        template="annotated_card",
        age_bucket="0-2d",
    )
    prompt = _cohort(
        report,
        "image_prompt",
        platform="x",
        content_type="x_visual",
        image_prompt_group="annotated",
        age_bucket="0-2d",
    )

    assert template.visual_sample_count == 2
    assert template.non_visual_sample_count == 0
    assert template.status == "insufficient_sample"
    assert prompt.visual_sample_count == 2
    assert prompt.image_prompt_group == "annotated"
    assert format_visual_engagement_attribution_json(report) == format_visual_engagement_attribution_json(report)


def test_small_cohorts_are_labeled_insufficient(db):
    _content(db, content_type="x_visual", image_path="/tmp/one.png", score=9.0)
    _content(db, content_type="x_post", score=4.0)

    report = build_visual_engagement_attribution_report(db, min_sample=2, now=NOW)

    assert _cohort(report, "overall").status == "insufficient_sample"
    assert report.recommendations == (
        "Collect more paired visual and non-visual samples before changing visual strategy.",
    )


def test_missing_image_metadata_columns_degrades_gracefully():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            content_type TEXT,
            content TEXT,
            published INTEGER,
            published_at TEXT,
            created_at TEXT
        );
        CREATE TABLE post_engagement (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            tweet_id TEXT,
            engagement_score REAL,
            fetched_at TEXT
        );
        INSERT INTO generated_content
            (id, content_type, content, published, published_at, created_at)
        VALUES
            (1, 'x_visual', 'visual', 1, '2026-04-30T12:00:00+00:00', '2026-04-30T12:00:00+00:00'),
            (2, 'x_post', 'text', 1, '2026-04-30T12:00:00+00:00', '2026-04-30T12:00:00+00:00');
        INSERT INTO post_engagement
            (id, content_id, tweet_id, engagement_score, fetched_at)
        VALUES
            (1, 1, 'tweet-1', 7.0, '2026-05-01T12:00:00+00:00'),
            (2, 2, 'tweet-2', 5.0, '2026-05-01T12:00:00+00:00');
        """
    )

    report = build_visual_engagement_attribution_report(conn, min_sample=1, now=NOW)
    payload = json.loads(format_visual_engagement_attribution_json(report))

    assert _cohort(report, "overall").engagement_delta == 2.0
    assert "image_path" in report.missing_metadata_columns
    assert "image_prompt" in report.missing_metadata_columns
    assert all(cohort.group_by != "image_prompt" for cohort in report.cohorts)
    assert payload["totals"]["sample_count"] == 2


def test_platform_filter_and_cli_json(db, capsys):
    _content(db, content_type="x_visual", image_path="/tmp/x.png", platform="x", score=9.0)
    _content(db, content_type="x_visual", image_path="/tmp/b.png", platform="bluesky", score=5.0)
    _content(db, content_type="x_post", platform="bluesky", score=2.0)

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("visual_engagement_attribution.script_context", fake_script_context):
        exit_code = main(["--days", "30", "--platform", "bluesky", "--format", "json", "--min-sample", "1"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["platform"] == "bluesky"
    assert payload["totals"]["sample_count"] == 2
    assert payload["totals"]["platforms"] == ["bluesky"]
