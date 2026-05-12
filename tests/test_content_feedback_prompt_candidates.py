from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.content_feedback_prompt_candidates import (
    build_content_feedback_prompt_candidates_report,
    format_content_feedback_prompt_candidates_json,
    format_content_feedback_prompt_candidates_text,
)


NOW = datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_feedback_prompt_candidates.py"
spec = importlib.util.spec_from_file_location("content_feedback_prompt_candidates_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, content_type: str = "x_post", *, days_ago: int = 1) -> int:
    cid = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content="draft",
        eval_score=7,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ? WHERE id = ?",
        ((NOW - timedelta(days=days_ago)).isoformat(), cid),
    )
    return cid


def _feedback(db, cid: int, feedback_type: str, notes: str, *, days_ago: int = 1) -> None:
    db.conn.execute(
        """INSERT INTO content_feedback (content_id, feedback_type, notes, created_at)
           VALUES (?, ?, ?, ?)""",
        (cid, feedback_type, notes, (NOW - timedelta(days=days_ago)).isoformat()),
    )
    db.conn.commit()


def test_groups_recent_feedback_by_normalized_motif_and_prompt_surface(db):
    first = _content(db, "x_post")
    second = _content(db, "x_thread")
    third = _content(db, "blog_post")
    _feedback(db, first, "reject", "Too much hype!!!")
    _feedback(db, second, "revise", "too much hype")
    _feedback(db, third, "prefer", "needs stronger sources")

    report = build_content_feedback_prompt_candidates_report(db, min_count=2, now=NOW)

    assert report["totals"]["rows_scanned"] == 3
    assert report["totals"]["candidate_count"] == 1
    candidate = report["candidates"][0]
    assert candidate["motif"] == "too much hype"
    assert candidate["count"] == 2
    assert candidate["feedback_type_counts"] == {"reject": 1, "revise": 1}
    assert candidate["affected_content_types"] == ["x_post", "x_thread"]
    assert candidate["prompt_surface_suggestions"] == ["x_post_v2", "x_thread_v2"]
    assert candidate["representative_content_ids"] == [first, second]


def test_rows_below_min_count_and_old_feedback_are_excluded(db):
    cid = _content(db)
    old = _content(db)
    _feedback(db, cid, "reject", "awkward opening")
    _feedback(db, old, "reject", "awkward opening", days_ago=60)

    report = build_content_feedback_prompt_candidates_report(db, days=30, min_count=2, now=NOW)

    assert report["candidates"] == []
    assert report["totals"]["motifs_below_min_count"] == 1


def test_formatters_and_cli_are_deterministic(db, monkeypatch, capsys):
    cid1 = _content(db, "x_long_post")
    cid2 = _content(db, "x_long_post")
    _feedback(db, cid1, "reject", "Burying the lede")
    _feedback(db, cid2, "prefer", "burying the lede")

    report = build_content_feedback_prompt_candidates_report(db, min_count=2, now=NOW)
    payload = json.loads(format_content_feedback_prompt_candidates_json(report))
    text = format_content_feedback_prompt_candidates_text(report)

    assert list(payload) == sorted(payload)
    assert "motif=burying lede count=2" in text

    monkeypatch.setattr(script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        script,
        "build_content_feedback_prompt_candidates_report",
        lambda db, **kwargs: build_content_feedback_prompt_candidates_report(db, now=NOW, **kwargs),
    )
    assert script.main(["--days", "30", "--limit", "1", "--min-count", "2", "--format", "json"]) == 0
    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["filters"]["limit"] == 1
    assert cli_payload["candidates"][0]["representative_content_ids"] == [cid1]
