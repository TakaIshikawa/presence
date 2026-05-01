"""Tests for stale rhetorical pattern trend reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from synthesis.stale_pattern_trends import (
    build_stale_pattern_trends,
    format_stale_pattern_trends_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "stale_pattern_trends.py"
spec = importlib.util.spec_from_file_location("stale_pattern_trends_cli", SCRIPT_PATH)
stale_pattern_trends_cli = importlib.util.module_from_spec(spec)
spec.loader.exec_module(stale_pattern_trends_cli)

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


def _content(
    db,
    *,
    text: str,
    content_type: str = "x_post",
    content_format: str | None = "tip",
    created_at: datetime | None = None,
    published: int = 0,
    published_at: str | None = None,
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=8.0,
        eval_feedback="ok",
        content_format=content_format,
    )
    db.conn.execute(
        """UPDATE generated_content
           SET created_at = ?, published = ?, published_at = ?
           WHERE id = ?""",
        (
            (created_at or NOW - timedelta(days=1)).isoformat(),
            published,
            published_at,
            content_id,
        ),
    )
    db.conn.commit()
    return content_id


def _prediction(db, content_id: int, prompt_version: str) -> None:
    db.conn.execute(
        """INSERT INTO engagement_predictions
           (content_id, predicted_score, prompt_type, prompt_version, prompt_hash)
           VALUES (?, 0.7, 'x_post', ?, 'hash')""",
        (content_id, prompt_version),
    )
    db.conn.commit()


def _pattern(report: dict, regex: str) -> dict:
    return next(pattern for pattern in report["patterns"] if pattern["regex"] == regex)


def _dimension(report: dict, name: str) -> dict[str, dict]:
    return {item["value"]: item for item in report["dimensions"][name]}


def test_reports_multiple_patterns_and_publication_statuses(db):
    ai_id = _content(
        db,
        text="AI systems need sharper release gates.",
        content_format="observation",
        published=1,
        published_at=NOW.isoformat(),
    )
    breakthrough_id = _content(
        db,
        text="This breakthrough changed our deployment checklist.",
        content_type="x_thread",
        content_format="thread",
    )
    _prediction(db, ai_id, "v2")
    _prediction(db, breakthrough_id, "v1")
    db.conn.execute(
        """INSERT INTO publish_queue (content_id, scheduled_at, platform, status)
           VALUES (?, ?, 'x', 'queued')""",
        (breakthrough_id, (NOW + timedelta(hours=1)).isoformat()),
    )
    db.conn.commit()

    report = build_stale_pattern_trends(db, days=7, now=NOW)

    assert report["summary"]["scanned_count"] == 2
    assert report["summary"]["hit_content_count"] == 2
    assert _pattern(report, "(?i)^AI\\s")["hit_count"] == 1
    assert _pattern(report, "(?i)\\bbreakthrough\\b")["hit_count"] == 1
    assert _dimension(report, "content_type")["x_post"]["hit_rate"] == 1.0
    assert _dimension(report, "content_format")["thread"]["hit_count"] == 1
    assert _dimension(report, "prompt_version")["v2"]["hit_count"] == 1
    assert _dimension(report, "publication_status")["published"]["hit_count"] == 1
    assert _dimension(report, "publication_status")["queued"]["hit_count"] == 1


def test_no_hit_output_keeps_scanned_totals(db):
    _content(db, text="A specific note about a small deployment fix.")

    report = build_stale_pattern_trends(db, days=7, now=NOW)

    assert report["summary"]["scanned_count"] == 1
    assert report["summary"]["hit_content_count"] == 0
    assert report["summary"]["pattern_hit_count"] == 0
    assert "No stale patterns found." in format_stale_pattern_trends_text(report)


def test_examples_are_limited_per_pattern(db):
    for index in range(3):
        _content(db, text=f"AI example {index}", content_format="observation")

    report = build_stale_pattern_trends(db, days=7, limit_examples=2, now=NOW)

    ai_pattern = _pattern(report, "(?i)^AI\\s")
    assert ai_pattern["hit_count"] == 3
    assert len(ai_pattern["examples"]) == 2
    assert {example["content_id"] for example in ai_pattern["examples"]} <= {
        row[0] for row in db.conn.execute("SELECT id FROM generated_content").fetchall()
    }


def test_content_type_filter_and_lookback(db):
    included_id = _content(
        db,
        text="Unpopular opinion: release gates are product features.",
        content_type="x_post",
    )
    _content(
        db,
        text="Unpopular opinion: threads need a clearer spine.",
        content_type="x_thread",
    )
    _content(
        db,
        text="AI outside the lookback window.",
        content_type="x_post",
        created_at=NOW - timedelta(days=20),
    )

    report = build_stale_pattern_trends(
        db,
        days=7,
        content_type="x_post",
        now=NOW,
    )

    assert report["summary"]["scanned_count"] == 1
    assert report["patterns"][0]["examples"][0]["content_id"] == included_id
    assert set(_dimension(report, "content_type")) == {"x_post"}


def test_cli_json_output(db, capsys):
    _content(db, text="The secret to reliable output is boring checks.")

    @contextmanager
    def fake_script_context():
        yield SimpleNamespace(), db

    with patch.object(stale_pattern_trends_cli, "script_context", fake_script_context), patch.object(
        stale_pattern_trends_cli,
        "build_stale_pattern_trends",
        wraps=lambda db, **kwargs: build_stale_pattern_trends(db, now=NOW, **kwargs),
    ):
        assert stale_pattern_trends_cli.main(
            ["--days", "7", "--content-type", "x_post", "--limit-examples", "1", "--format", "json"]
        ) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"]["days"] == 7
    assert payload["filters"]["content_type"] == "x_post"
    assert payload["filters"]["limit_examples"] == 1
    assert payload["summary"]["pattern_hit_count"] == 1
