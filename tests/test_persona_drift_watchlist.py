"""Tests for persona drift watchlist reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.persona_drift_watchlist import (
    build_persona_drift_watchlist_report,
    format_persona_drift_watchlist_json,
    format_persona_drift_watchlist_text,
)


NOW = datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "persona_drift_watchlist.py"
spec = importlib.util.spec_from_file_location("persona_drift_watchlist_script", SCRIPT_PATH)
persona_drift_watchlist_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(persona_drift_watchlist_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, text: str, *, days_ago: int = 1, published: int = 0) -> int:
    content_id = db.insert_generated_content(
        content_type="blog_post",
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=6,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ?, published = ? WHERE id = ?",
        ((NOW - timedelta(days=days_ago)).isoformat(), published, content_id),
    )
    db.conn.commit()
    return content_id


def test_no_candidates_returns_empty_watchlist(db):
    _content(db, "Precise release note with concrete tradeoffs.")

    report = build_persona_drift_watchlist_report(db, now=NOW)

    assert report.items == ()
    assert report.totals["candidates_scanned"] == 1
    assert "No persona drift candidates" in format_persona_drift_watchlist_text(report)


def test_stale_pattern_matches_are_ranked_with_reason_codes(db):
    first = _content(db, "This is a game changer. This is a game changer.", published=1)
    second = _content(db, "Unlock the future with a concrete migration note.")

    report = build_persona_drift_watchlist_report(db, min_severity=1, now=NOW)

    assert [item.content_id for item in report.items] == [first, second]
    assert "stale_pattern" in report.items[0].reason_codes
    assert "repeated_phrasing" in report.items[0].reason_codes
    assert report.items[0].published is True


def test_overlap_threshold_edge_cases_use_guard_metrics(db):
    below = _content(db, "Normal text")
    at_threshold = _content(db, "Normal text too")
    db.save_persona_guard_summary(
        below,
        {"checked": True, "passed": True, "status": "ok", "score": 0.9, "metrics": {"overlap_score": 0.339}},
    )
    db.save_persona_guard_summary(
        at_threshold,
        {"checked": True, "passed": True, "status": "ok", "score": 0.9, "metrics": {"overlap_score": 0.34}},
    )

    report = build_persona_drift_watchlist_report(db, overlap_threshold=0.34, now=NOW)

    assert [item.content_id for item in report.items] == [at_threshold]
    assert report.items[0].overlap_score == 0.34
    assert "high_voice_overlap" in report.items[0].reason_codes


def test_stable_ordering_and_cli_json(db, monkeypatch, capsys):
    first = _content(db, "Unlock reliable systems.")
    second = _content(db, "Delve into reliable systems.")
    monkeypatch.setattr(persona_drift_watchlist_script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        persona_drift_watchlist_script,
        "build_persona_drift_watchlist_report",
        lambda db, **kwargs: build_persona_drift_watchlist_report(db, now=NOW, **kwargs),
    )

    report = build_persona_drift_watchlist_report(db, now=NOW)
    payload = json.loads(format_persona_drift_watchlist_json(report))
    exit_code = persona_drift_watchlist_script.main(["--format", "json", "--min-severity", "1"])
    cli_payload = json.loads(capsys.readouterr().out)

    assert [item.content_id for item in report.items] == [first, second]
    assert payload["artifact_type"] == "persona_drift_watchlist"
    assert cli_payload["item_count"] == 2
    assert exit_code == 0
