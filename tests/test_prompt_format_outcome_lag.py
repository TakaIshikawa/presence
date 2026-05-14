"""Tests for prompt format outcome lag reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.prompt_format_outcome_lag import (
    build_prompt_format_outcome_lag_report,
    build_prompt_format_outcome_lag_report_from_db,
    format_prompt_format_outcome_lag_text,
)


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "prompt_format_outcome_lag.py"
spec = importlib.util.spec_from_file_location("prompt_format_outcome_lag_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _candidate(
    content_id: int,
    prompt_format: str,
    *,
    published_days_ago: int | None = None,
) -> dict:
    row = {"content_id": content_id, "prompt_format": prompt_format}
    if published_days_ago is not None:
        row["published"] = 1
        row["published_at"] = (NOW - timedelta(days=published_days_ago)).isoformat()
    return row


def _content(db, prompt_format: str, *, published_days_ago: int | None = None) -> int:
    cid = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=f"{prompt_format} copy",
        eval_score=8,
        eval_feedback="ok",
    )
    published = 1 if published_days_ago is not None else 0
    published_at = (NOW - timedelta(days=published_days_ago)).isoformat() if published_days_ago is not None else None
    db.conn.execute(
        """UPDATE generated_content
           SET content_format = ?, published = ?, published_at = ?, tweet_id = ?
           WHERE id = ?""",
        (prompt_format, published, published_at, f"tweet-{cid}" if published else None, cid),
    )
    db.conn.commit()
    return cid


def _metric(db, content_id: int, *, fetched_days_ago: int = 1) -> None:
    db.conn.execute(
        """INSERT INTO post_engagement
           (content_id, tweet_id, like_count, retweet_count, reply_count, quote_count, fetched_at)
           VALUES (?, ?, 1, 0, 0, 0, ?)""",
        (content_id, f"tweet-{content_id}", (NOW - timedelta(days=fetched_days_ago)).isoformat()),
    )
    db.conn.commit()


def test_empty_input_returns_empty_report():
    report = build_prompt_format_outcome_lag_report([], [], [], now=NOW)

    assert report["formats"] == []
    assert report["ranked_formats"] == []
    assert report["totals"]["candidate_count"] == 0
    assert report["empty_state"]["is_empty"] is True


def test_groups_candidates_by_prompt_format_and_tracks_outcomes():
    candidates = [
        _candidate(1, "question", published_days_ago=5),
        _candidate(2, "question", published_days_ago=1),
        _candidate(3, "tip", published_days_ago=4),
        _candidate(4, "tip"),
    ]
    metrics = [{"content_id": 2, "fetched_at": NOW.isoformat()}]

    report = build_prompt_format_outcome_lag_report(
        candidates,
        [],
        metrics,
        outcome_window_days=3,
        now=NOW,
    )

    by_format = {item["prompt_format"]: item for item in report["formats"]}
    assert by_format["question"]["candidate_count"] == 2
    assert by_format["question"]["published"] == 2
    assert by_format["question"]["metrics_fetched"] == 1
    assert by_format["question"]["pending"] == 1
    assert by_format["question"]["stale_pending"] == 1
    assert by_format["tip"]["candidate_count"] == 2
    assert by_format["tip"]["published"] == 1
    assert by_format["tip"]["stale_pending"] == 1
    assert report["totals"]["pending"] == 2


def test_configurable_window_controls_when_pending_becomes_late():
    candidates = [_candidate(1, "observation", published_days_ago=4)]

    short = build_prompt_format_outcome_lag_report(candidates, [], [], outcome_window_days=3, now=NOW)
    long = build_prompt_format_outcome_lag_report(candidates, [], [], outcome_window_days=7, now=NOW)

    assert short["formats"][0]["stale_pending"] == 1
    assert long["formats"][0]["stale_pending"] == 0


def test_ranks_by_late_outcome_rate_and_minimum_sample_size():
    candidates = [
        _candidate(1, "late", published_days_ago=8),
        _candidate(2, "late", published_days_ago=7),
        _candidate(3, "mixed", published_days_ago=8),
        _candidate(4, "mixed", published_days_ago=1),
        _candidate(5, "small", published_days_ago=9),
    ]
    metrics = [{"content_id": 4, "fetched_at": NOW.isoformat()}]

    report = build_prompt_format_outcome_lag_report(
        candidates,
        [],
        metrics,
        outcome_window_days=3,
        min_sample=2,
        now=NOW,
    )

    assert [item["prompt_format"] for item in report["ranked_formats"]] == ["late", "mixed"]
    assert report["ranked_formats"][0]["late_outcome_rate"] == 1.0
    assert all(item["prompt_format"] != "small" for item in report["ranked_formats"])
    assert "late_rate" in format_prompt_format_outcome_lag_text(report)


def test_publication_rows_and_cli_default_json_output(db, monkeypatch, capsys):
    metric_id = _content(db, "question", published_days_ago=5)
    stale_id = _content(db, "question")
    db.conn.execute(
        """INSERT INTO content_publications
           (content_id, platform, status, platform_post_id, published_at, updated_at)
           VALUES (?, 'bluesky', 'published', ?, ?, ?)""",
        (
            stale_id,
            f"post-{stale_id}",
            (NOW - timedelta(days=6)).isoformat(),
            (NOW - timedelta(days=6)).isoformat(),
        ),
    )
    db.conn.commit()
    _metric(db, metric_id)

    report = build_prompt_format_outcome_lag_report_from_db(db, outcome_window_days=3, now=NOW)
    question = report["formats"][0]
    assert question["prompt_format"] == "question"
    assert question["published"] == 2
    assert question["metrics_fetched"] == 1
    assert question["stale_pending_content_ids"] == [stale_id]

    monkeypatch.setattr(script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        script,
        "build_prompt_format_outcome_lag_report_from_db",
        lambda db, **kwargs: build_prompt_format_outcome_lag_report_from_db(db, now=NOW, **kwargs),
    )
    assert script.main(["--outcome-window-days", "3", "--min-sample", "1"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "prompt_format_outcome_lag"
    assert payload["ranked_formats"][0]["prompt_format"] == "question"

    assert script.main(["--table"]) == 0
    assert "Prompt Format Outcome Lag" in capsys.readouterr().out
