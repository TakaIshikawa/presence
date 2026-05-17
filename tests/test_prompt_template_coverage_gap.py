"""Tests for prompt template coverage gap reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.prompt_template_coverage_gap import (
    build_prompt_template_coverage_gap_report,
    build_prompt_template_coverage_gap_report_from_db,
    format_prompt_template_coverage_gap_text,
)


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "prompt_template_coverage_gap.py"
spec = importlib.util.spec_from_file_location("prompt_template_coverage_gap_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _candidate(content_id: int, template: str, version: str, days_ago: int) -> dict:
    return {
        "content_id": content_id,
        "prompt_template": template,
        "prompt_version": version,
        "candidate_at": (NOW - timedelta(days=days_ago)).isoformat(),
    }


def _outcome(content_id: int, days_ago: int) -> dict:
    return {"content_id": content_id, "outcome_at": (NOW - timedelta(days=days_ago)).isoformat()}


def _content(db, template: str, *, days_ago: int, version: str | None = "1") -> int:
    cid = db.insert_generated_content(
        content_type=template,
        source_commits=[],
        source_messages=[],
        content=f"{template} generated copy",
        eval_score=8,
        eval_feedback="ok",
    )
    created_at = (NOW - timedelta(days=days_ago)).isoformat()
    db.conn.execute("UPDATE generated_content SET created_at = ? WHERE id = ?", (created_at, cid))
    db.insert_prediction(content_id=cid, predicted_score=8, prompt_type=template, prompt_version=version)
    db.conn.execute("UPDATE engagement_predictions SET created_at = ? WHERE content_id = ?", (created_at, cid))
    db.conn.commit()
    return cid


def test_full_coverage_is_excluded_from_gap_rows():
    report = build_prompt_template_coverage_gap_report(
        [_candidate(1, "x_post", "2", 1)],
        [_outcome(1, 1)],
        [_outcome(1, 1)],
        days=7,
        now=NOW,
    )

    assert report["summary"]["candidate_group_count"] == 1
    assert report["summary"]["gap_group_count"] == 0
    assert report["rows"] == []


def test_missing_reviews_are_reported_by_prompt_template_and_version():
    report = build_prompt_template_coverage_gap_report(
        [_candidate(1, "x_thread", "3", 1), _candidate(2, "x_thread", "3", 2)],
        [],
        [_outcome(1, 1)],
        days=7,
        now=NOW,
    )

    assert report["rows"] == [
        {
            "prompt_template": "x_thread",
            "prompt_version": "3",
            "candidate_count": 2,
            "reviewed_count": 0,
            "published_count": 1,
            "last_candidate_at": (NOW - timedelta(days=1)).isoformat(),
            "last_reviewed_at": None,
            "last_published_at": (NOW - timedelta(days=1)).isoformat(),
            "gap_reason": "no_review",
            "candidate_content_ids": [1, 2],
        }
    ]


def test_missing_publishes_and_stale_outcomes_are_classified():
    report = build_prompt_template_coverage_gap_report(
        [
            _candidate(1, "blog_post", "1", 1),
            _candidate(2, "newsletter", "4", 1),
        ],
        [_outcome(1, 1), _outcome(2, 3)],
        [_outcome(2, 3)],
        days=7,
        now=NOW,
    )
    by_template = {row["prompt_template"]: row for row in report["rows"]}

    assert by_template["blog_post"]["gap_reason"] == "no_publish"
    assert by_template["blog_post"]["reviewed_count"] == 1
    assert by_template["blog_post"]["published_count"] == 0
    assert by_template["newsletter"]["gap_reason"] == "stale_outcome"


def test_lookback_filters_candidates_and_outcomes():
    report = build_prompt_template_coverage_gap_report(
        [
            _candidate(1, "old", "1", 20),
            _candidate(2, "recent", "1", 1),
        ],
        [_outcome(2, 20)],
        [],
        days=7,
        now=NOW,
    )

    assert [row["prompt_template"] for row in report["rows"]] == ["recent"]
    assert report["rows"][0]["gap_reason"] == "no_review"


def test_db_loader_and_cli_support_json_and_table_output(db, monkeypatch, capsys):
    covered_id = _content(db, "covered", days_ago=1, version="2")
    missing_id = _content(db, "missing_publish", days_ago=1, version="5")
    db.conn.execute(
        """INSERT INTO content_feedback (content_id, feedback_type, created_at)
           VALUES (?, 'prefer', ?), (?, 'prefer', ?)""",
        (
            covered_id,
            (NOW - timedelta(hours=3)).isoformat(),
            missing_id,
            (NOW - timedelta(hours=2)).isoformat(),
        ),
    )
    db.conn.execute(
        "UPDATE generated_content SET published = 1, published_at = ? WHERE id = ?",
        ((NOW - timedelta(hours=1)).isoformat(), covered_id),
    )
    db.conn.commit()

    report = build_prompt_template_coverage_gap_report_from_db(db, days=7, now=NOW)
    assert [row["prompt_template"] for row in report["rows"]] == ["missing_publish"]
    assert report["rows"][0]["prompt_version"] == "5"
    assert "Prompt Template Coverage Gap" in format_prompt_template_coverage_gap_text(report)

    monkeypatch.setattr(script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        script,
        "build_prompt_template_coverage_gap_report_from_db",
        lambda db, **kwargs: build_prompt_template_coverage_gap_report_from_db(db, now=NOW, **kwargs),
    )
    assert script.main(["--days", "7", "--limit", "10"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "prompt_template_coverage_gap"
    assert payload["rows"][0]["gap_reason"] == "no_publish"

    assert script.main(["--table"]) == 0
    assert "missing_publish" in capsys.readouterr().out
