"""Tests for X hashtag density reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from evaluation.x_hashtag_density import (
    analyze_x_hashtag_density,
    build_x_hashtag_density_report,
    canonicalize_hashtag,
    extract_hashtags,
    format_x_hashtag_density_json,
    format_x_hashtag_density_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "x_hashtag_density.py"
spec = importlib.util.spec_from_file_location("x_hashtag_density_script", SCRIPT_PATH)
x_hashtag_density_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(x_hashtag_density_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _post(
    db,
    content: str,
    *,
    days_ago: int = 0,
    published: bool = False,
    queued: bool = False,
) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=8.0,
        eval_feedback="usable",
    )
    created_at = NOW - timedelta(days=days_ago)
    db.conn.execute(
        "UPDATE generated_content SET created_at = ?, published = ?, published_at = ? WHERE id = ?",
        (
            created_at.isoformat(),
            1 if published else 0,
            created_at.isoformat() if published else None,
            content_id,
        ),
    )
    if published:
        db.conn.execute(
            """INSERT INTO content_publications
               (content_id, platform, status, published_at)
               VALUES (?, 'x', 'published', ?)""",
            (content_id, created_at.isoformat()),
        )
    if queued:
        db.conn.execute(
            """INSERT INTO publish_queue (content_id, scheduled_at, platform, status)
               VALUES (?, ?, 'x', 'queued')""",
            (content_id, created_at.isoformat()),
        )
    db.conn.commit()
    return content_id


def test_extracts_hashtags_while_ignoring_url_fragments_and_punctuation():
    hashtags = extract_hashtags(
        "Ship #LaunchAI, then read https://example.com/docs#install and www.example.com/#top. "
        "Keep #launchai and #Ops_2026."
    )

    assert hashtags == ("#LaunchAI", "#Ops_2026")
    assert canonicalize_hashtag("#LaunchAI") == "#launchai"


def test_report_flags_count_share_and_repeated_hashtag_sets(db, monkeypatch):
    monkeypatch.setattr("evaluation.x_hashtag_density.datetime", _FixedDateTime)
    first = _post(db, "Launch note #AI #Build #Ops #Ship", days_ago=1, queued=True)
    second = _post(db, "Follow-up #AI #Build #Ops #Ship", days_ago=2)
    third = _post(db, "Another #AI #Build #Ops #Ship", days_ago=3)
    _post(db, "A calm post with #Ops.", days_ago=20, published=True)

    report = build_x_hashtag_density_report(
        db,
        recent_days=7,
        baseline_days=30,
        max_hashtags=3,
        max_hashtag_char_share=0.35,
        repeated_set_threshold=3,
    )
    posts = {post.post_id: post for post in report.posts}

    assert report.flagged_posts == 3
    assert posts[first].warnings == (
        "excessive_hashtag_count",
        "high_hashtag_character_share",
        "repeated_hashtag_set",
    )
    assert "repeated_hashtag_set" in posts[second].warnings
    assert "repeated_hashtag_set" in posts[third].warnings
    assert report.repeated_clusters[0].canonical_hashtags == ("#ai", "#build", "#ops", "#ship")


def test_recent_window_compares_against_baseline_for_style_drift(db, monkeypatch):
    monkeypatch.setattr("evaluation.x_hashtag_density.datetime", _FixedDateTime)
    _post(db, "Recent one #AI #Ops #Ship", days_ago=1)
    _post(db, "Recent two #AI #Ops #Ship", days_ago=2)
    _post(db, "Baseline one #Ops", days_ago=20)
    _post(db, "Baseline two with no tag", days_ago=21)

    report = build_x_hashtag_density_report(db, recent_days=7, baseline_days=30)

    assert report.recent_baseline.post_count == 2
    assert report.historical_baseline.post_count == 2
    assert "hashtag_count_drift" in report.drift_warnings
    assert "hashtag_share_drift" in report.drift_warnings


def test_analyzer_accepts_plain_rows_and_json_is_sorted():
    rows = [
        {
            "id": 1,
            "content": "Generated #One #Two #Three #Four",
            "status": "generated",
            "timestamp": NOW.isoformat(),
            "window": "recent",
        }
    ]

    report = analyze_x_hashtag_density(rows, max_hashtags=3, generated_at=NOW.isoformat())
    payload = json.loads(format_x_hashtag_density_json(report))
    text = format_x_hashtag_density_text(report)

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "x_hashtag_density"
    assert payload["posts"][0]["warnings"] == [
        "excessive_hashtag_count",
        "high_hashtag_character_share",
    ]
    assert "X Hashtag Density" in text


def test_missing_table_and_invalid_args_are_reported(db):
    empty = db.conn
    empty.execute("DROP TABLE generated_content")

    report = build_x_hashtag_density_report(empty)

    assert report.missing_tables == ("generated_content",)
    with pytest.raises(ValueError, match="recent_days must be positive"):
        build_x_hashtag_density_report(empty, recent_days=0)


def test_cli_supports_window_threshold_and_json_options(db, monkeypatch, capsys):
    monkeypatch.setattr("evaluation.x_hashtag_density.datetime", _FixedDateTime)
    _post(db, "CLI copy #One #Two #Three #Four", days_ago=1)
    monkeypatch.setattr(
        x_hashtag_density_script,
        "script_context",
        lambda: _script_context(db),
    )

    exit_code = x_hashtag_density_script.main(
        [
            "--recent-days",
            "7",
            "--baseline-days",
            "30",
            "--max-hashtags",
            "3",
            "--format",
            "json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["recent_days"] == 7
    assert payload["baseline_days"] == 30
    assert payload["posts"][0]["warnings"][0] == "excessive_hashtag_count"


class _FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW if tz else NOW.replace(tzinfo=None)
