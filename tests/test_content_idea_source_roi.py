"""Tests for content idea source ROI reporting."""

from __future__ import annotations

import json
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from content_idea_source_roi import main  # noqa: E402
from evaluation.content_idea_source_roi import (  # noqa: E402
    build_content_idea_source_roi_report,
    format_content_idea_source_roi_json,
    format_content_idea_source_roi_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


def _set_idea_created_at(db, idea_id: int, created_at: str = "2026-04-20T12:00:00+00:00") -> None:
    db.conn.execute(
        "UPDATE content_ideas SET created_at = ?, updated_at = ? WHERE id = ?",
        (created_at, created_at, idea_id),
    )
    db.conn.commit()


def _content(db, text: str, *, published: bool = True, auto_quality: str | None = None) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET published = ?, published_at = ?, auto_quality = ? WHERE id = ?",
        (
            1 if published else 0,
            "2026-04-25T12:00:00+00:00" if published else None,
            auto_quality,
            content_id,
        ),
    )
    db.conn.commit()
    return content_id


def _engagement(db, content_id: int, *, x: float | None = None, linkedin: float | None = None, bluesky: float | None = None) -> None:
    if x is not None:
        db.insert_engagement(
            content_id=content_id,
            tweet_id=f"tweet-{content_id}",
            like_count=0,
            retweet_count=0,
            reply_count=0,
            quote_count=0,
            engagement_score=x,
        )
        db.conn.execute(
            "UPDATE post_engagement SET fetched_at = ? WHERE content_id = ?",
            ("2026-04-26T12:00:00+00:00", content_id),
        )
    if linkedin is not None:
        db.insert_linkedin_engagement(
            content_id=content_id,
            post_id=f"li-{content_id}",
            engagement_score=linkedin,
            fetched_at="2026-04-27T12:00:00+00:00",
        )
    if bluesky is not None:
        db.insert_bluesky_engagement(
            content_id=content_id,
            bluesky_uri=f"at://post/{content_id}",
            like_count=0,
            repost_count=0,
            reply_count=0,
            quote_count=0,
            engagement_score=bluesky,
        )
        db.conn.execute(
            "UPDATE bluesky_engagement SET fetched_at = ? WHERE content_id = ?",
            ("2026-04-28T12:00:00+00:00", content_id),
        )
    db.conn.commit()


def test_report_ranks_sources_by_downstream_performance_from_fixture(db):
    strong_promoted = db.add_content_idea(
        "Promote release lesson",
        topic="release",
        source="release_digest",
    )
    _set_idea_created_at(db, strong_promoted)
    strong_direct = db.add_content_idea(
        "Direct generated release lesson",
        topic="release",
        source="release_digest",
    )
    _set_idea_created_at(db, strong_direct)
    weak_idea = db.add_content_idea(
        "Issue follow up",
        topic="issues",
        source="issue_digest",
    )
    _set_idea_created_at(db, weak_idea)
    stale_idea = db.add_content_idea(
        "Old source",
        topic="old",
        source="old_source",
    )
    _set_idea_created_at(db, stale_idea, "2025-12-01T12:00:00+00:00")

    strong_content = _content(db, "Published release post", auto_quality="resonated")
    _engagement(db, strong_content, x=6.0, linkedin=2.0, bluesky=1.0)
    planned_id = db.promote_content_idea(strong_promoted, "2026-04-24", topic="release")
    db.mark_planned_topic_generated(planned_id, strong_content)

    direct_content = _content(db, "Generated from metadata", published=False)
    db.conn.execute(
        "UPDATE content_ideas SET source_metadata = ? WHERE id = ?",
        (json.dumps({"content_id": direct_content}), strong_direct),
    )

    weak_content = _content(db, "Weak issue post")
    db.conn.execute(
        "UPDATE content_ideas SET source_metadata = ? WHERE id = ?",
        (json.dumps({"content_id": weak_content}), weak_idea),
    )

    report = build_content_idea_source_roi_report(db, days=60, now=NOW)

    assert [row.source for row in report.rows] == ["release_digest", "issue_digest"]
    release = report.rows[0]
    assert release.ideas_created == 2
    assert release.promoted_generated == 2
    assert release.published == 1
    assert release.average_engagement == 9.0
    assert release.resonance_rate == 1.0
    assert release.recommendation == "double_down"
    issue = report.rows[1]
    assert issue.average_engagement == 0.0
    assert issue.resonance_rate == 0.0
    assert issue.recommendation == "deprioritize"


def test_links_idea_source_metadata_planned_topic_to_generated_content(db):
    idea = db.add_content_idea(
        "Gap source idea",
        source="gap_report",
        source_metadata={"planned_topic_id": 12345},
    )
    _set_idea_created_at(db, idea)
    content_id = _content(db, "Gap content")
    planned_id = db.insert_planned_topic(topic="gaps", target_date="2026-04-24")
    db.conn.execute(
        "UPDATE content_ideas SET source_metadata = ? WHERE id = ?",
        (json.dumps({"planned_topic_id": planned_id}), idea),
    )
    db.mark_planned_topic_generated(planned_id, content_id)

    report = build_content_idea_source_roi_report(db, now=NOW)

    assert report.rows[0].source == "gap_report"
    assert report.rows[0].content_ids == (content_id,)
    assert report.rows[0].promoted_generated == 1
    assert report.rows[0].published == 1


def test_empty_dataset_returns_empty_rows_without_required_tables():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_content_idea_source_roi_report(conn, now=NOW)
    text = format_content_idea_source_roi_text(report)

    assert report.rows == ()
    assert json.loads(format_content_idea_source_roi_json(report))["rows"] == []
    assert "No content idea sources found." in text


def test_min_ideas_filters_sparse_sources(db):
    first = db.add_content_idea("One", source="manual")
    second = db.add_content_idea("Two", source="manual")
    other = db.add_content_idea("Solo", source="scratchpad")
    for idea_id in (first, second, other):
        _set_idea_created_at(db, idea_id)

    report = build_content_idea_source_roi_report(db, min_ideas=2, now=NOW)

    assert [row.source for row in report.rows] == ["manual"]
    assert report.rows[0].ideas_created == 2


def test_cli_prints_json_when_flag_is_passed(db, capsys):
    idea_id = db.add_content_idea("CLI idea", source="manual")
    _set_idea_created_at(db, idea_id)

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("content_idea_source_roi.script_context", fake_script_context):
        exit_code = main(["--days", "30", "--min-ideas", "1", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["days"] == 30
    assert payload["min_ideas"] == 1
    assert payload["rows"][0]["source"] == "manual"


def test_cli_prints_readable_table_by_default(db, capsys):
    idea_id = db.add_content_idea("Table idea", source="manual")
    _set_idea_created_at(db, idea_id)

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("content_idea_source_roi.script_context", fake_script_context):
        exit_code = main(["--days", "30"])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "Content Idea Source ROI" in output
    assert "manual" in output
    assert "Prom/gen" in output
