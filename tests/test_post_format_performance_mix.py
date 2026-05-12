"""Tests for post format performance mix reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from evaluation.post_format_performance_mix import (
    build_post_format_performance_mix_report,
    format_post_format_performance_mix_json,
    format_post_format_performance_mix_text,
)


NOW = datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "post_format_performance_mix.py"
spec = importlib.util.spec_from_file_location("post_format_performance_mix_script", SCRIPT_PATH)
post_format_performance_mix_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(post_format_performance_mix_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, content_format: str, *, published: bool = True) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=f"{content_format} post",
        eval_score=8,
        eval_feedback="ok",
        content_format=content_format,
    )
    db.conn.execute(
        "UPDATE generated_content SET published = ?, published_at = ? WHERE id = ?",
        (1 if published else 0, "2026-05-12T12:00:00+00:00" if published else None, content_id),
    )
    if published:
        db.conn.execute(
            """INSERT INTO content_publications
               (content_id, platform, status, published_at, updated_at)
               VALUES (?, 'x', 'published', ?, ?)""",
            (content_id, "2026-05-12T12:00:00+00:00", "2026-05-12T12:00:00+00:00"),
        )
    db.conn.commit()
    return content_id


def _engagement(db, content_id: int, score: float, fetched_at: str) -> None:
    db.conn.execute(
        """INSERT INTO post_engagement
           (content_id, tweet_id, engagement_score, fetched_at)
           VALUES (?, ?, ?, ?)""",
        (content_id, f"tweet-{content_id}-{fetched_at}", score, fetched_at),
    )
    db.conn.commit()


def test_groups_by_content_format_using_latest_engagement_snapshot(db):
    first = _content(db, "tip")
    second = _content(db, "tip")
    _engagement(db, first, 1.0, "2026-05-10T12:00:00+00:00")
    _engagement(db, first, 5.0, "2026-05-12T12:00:00+00:00")
    _engagement(db, second, 7.0, "2026-05-12T12:00:00+00:00")

    report = build_post_format_performance_mix_report(
        db,
        min_samples=2,
        delta=0.5,
        now=NOW,
    )

    row = report.scored_formats[0]
    assert row.content_format == "tip"
    assert row.sample_count == 2
    assert row.average_engagement_score == 6.0
    assert row.content_ids == (first, second)


def test_formats_below_min_samples_are_tracked_as_underused(db):
    _engagement(db, _content(db, "question"), 3.0, "2026-05-12T12:00:00+00:00")
    _engagement(db, _content(db, "tip"), 5.0, "2026-05-12T12:00:00+00:00")
    _engagement(db, _content(db, "tip"), 6.0, "2026-05-12T12:00:00+00:00")

    report = build_post_format_performance_mix_report(db, min_samples=2, now=NOW)

    assert [row.content_format for row in report.scored_formats] == ["tip"]
    assert report.underused_formats[0].content_format == "question"
    assert report.underused_formats[0].classification == "underused"


def test_classifies_overperforming_underperforming_and_stable_formats(db):
    for score in (10.0, 10.0):
        _engagement(db, _content(db, "bold_claim"), score, "2026-05-12T12:00:00+00:00")
    for score in (1.0, 1.0):
        _engagement(db, _content(db, "plain"), score, "2026-05-12T12:00:00+00:00")
    for score in (5.5, 5.5):
        _engagement(db, _content(db, "thread"), score, "2026-05-12T12:00:00+00:00")

    report = build_post_format_performance_mix_report(
        db,
        min_samples=2,
        delta=1.0,
        now=NOW,
    )

    by_format = {row.content_format: row for row in report.scored_formats}
    assert by_format["bold_claim"].classification == "overperforming"
    assert by_format["plain"].classification == "underperforming"
    assert by_format["thread"].classification == "stable"


def test_unpublished_and_old_posts_are_excluded(db):
    old = _content(db, "old")
    unpublished = _content(db, "draft", published=False)
    _engagement(db, old, 10.0, "2026-05-12T12:00:00+00:00")
    _engagement(db, unpublished, 10.0, "2026-05-12T12:00:00+00:00")
    db.conn.execute(
        "UPDATE content_publications SET published_at = ? WHERE content_id = ?",
        ("2026-01-01T12:00:00+00:00", old),
    )
    db.conn.execute(
        "UPDATE generated_content SET published_at = ? WHERE id = ?",
        ("2026-01-01T12:00:00+00:00", old),
    )
    db.conn.commit()

    report = build_post_format_performance_mix_report(db, days=30, min_samples=1, now=NOW)

    assert report.scored_formats == ()
    assert report.underused_formats == ()


def test_json_text_and_cli_output(db, capsys, monkeypatch):
    _engagement(db, _content(db, "tip"), 5.0, "2026-05-12T12:00:00+00:00")
    report = build_post_format_performance_mix_report(db, min_samples=1, now=NOW)
    payload = json.loads(format_post_format_performance_mix_json(report))
    text = format_post_format_performance_mix_text(report)

    assert payload["artifact_type"] == "post_format_performance_mix"
    assert payload["scored_formats"][0]["sample_count"] == 1
    assert payload["scored_formats"][0]["average_engagement_score"] == 5.0
    assert payload["scored_formats"][0]["classification"] == "stable"
    assert "Post Format Performance Mix" in text

    monkeypatch.setattr(post_format_performance_mix_script, "script_context", lambda: _script_context(db))
    assert post_format_performance_mix_script.main(["--format", "json", "--min-samples", "1"]) == 0
    assert json.loads(capsys.readouterr().out)["scored_formats"][0]["content_format"] == "tip"


def test_invalid_arguments_raise(db):
    with pytest.raises(ValueError, match="days"):
        build_post_format_performance_mix_report(db, days=0)
    with pytest.raises(ValueError, match="min_samples"):
        build_post_format_performance_mix_report(db, min_samples=0)
