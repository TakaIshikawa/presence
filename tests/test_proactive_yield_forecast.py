"""Tests for proactive action yield forecasting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from engagement.proactive_yield_forecast import (
    build_proactive_yield_forecast_report,
    format_proactive_yield_forecast_json,
    format_proactive_yield_forecast_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "proactive_yield_forecast.py"
spec = importlib.util.spec_from_file_location("proactive_yield_forecast_script", SCRIPT_PATH)
proactive_yield_forecast_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(proactive_yield_forecast_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_action(db, **kwargs) -> int:
    defaults = dict(
        action_type="reply",
        target_tweet_id="target-1",
        target_tweet_text="Useful thread on applied agents.",
        target_author_handle="alice",
        target_author_id="author-a",
        discovery_source="curated_timeline",
        relevance_score=0.8,
        draft_text="Good point.",
        relationship_context=json.dumps(
            {"tier_name": "Key Network", "dunbar_tier": 2},
            sort_keys=True,
        ),
        knowledge_ids=json.dumps([[7, 0.9], [11, 0.7]], sort_keys=True),
    )
    defaults.update(kwargs)
    return db.insert_proactive_action(**defaults)


def _set_state(
    db,
    action_id: int,
    *,
    status: str = "pending",
    created_at: str = "2026-05-01T10:00:00+00:00",
    reviewed_at: str | None = None,
    posted_at: str | None = None,
) -> None:
    db.conn.execute(
        """UPDATE proactive_actions
           SET status = ?, created_at = ?, reviewed_at = ?, posted_at = ?
           WHERE id = ?""",
        (status, created_at, reviewed_at, posted_at, action_id),
    )
    db.conn.commit()


def test_scores_pending_actions_sorts_by_expected_yield_and_exposes_components(db):
    prior_success = _insert_action(db, target_tweet_id="prior-success", target_author_handle="Alice")
    _set_state(
        db,
        prior_success,
        status="posted",
        created_at="2026-04-28T09:00:00+00:00",
        posted_at="2026-04-28T10:00:00+00:00",
    )
    high = _insert_action(db, target_tweet_id="high", target_author_handle="@Alice")
    _set_state(db, high, created_at="2026-05-02T08:00:00+00:00")
    low = _insert_action(
        db,
        target_tweet_id="low",
        action_type="like",
        target_author_handle="bob",
        discovery_source="config",
        relevance_score=0.2,
        relationship_context=json.dumps({"tier_name": "Outer Circle", "dunbar_tier": 4}),
        knowledge_ids=json.dumps([99]),
    )
    _set_state(db, low, created_at="2026-04-25T08:00:00+00:00")

    report = build_proactive_yield_forecast_report(db, days=14, limit=10, now=NOW)
    payload = json.loads(format_proactive_yield_forecast_json(report))

    assert payload["artifact_type"] == "proactive_yield_forecast"
    assert [item["id"] for item in payload["actions"]] == [high, low]
    first = payload["actions"][0]
    assert first["target_handle"] == "alice"
    assert first["recommended_next_step"] == "execute"
    assert first["score"] > payload["actions"][1]["score"]
    assert first["score_components"]["action_type"]["points"] == 18.0
    assert first["score_components"]["target_tier"]["value"] == "Key Network (tier 2)"
    assert first["score_components"]["prior_outcomes"]["counts"] == {"posted": 1}
    assert first["score_components"]["knowledge_ids"]["ids"] == [7, 11]
    assert first["score_components"]["knowledge_ids"]["average_relevance"] == 0.8
    assert payload["summary"]["rows_scored"] == 2


def test_missing_context_is_enrich_context_instead_of_execute(db):
    action_id = _insert_action(
        db,
        target_tweet_id="missing-context",
        target_tweet_text="",
        relationship_context=None,
        knowledge_ids=None,
        platform_metadata=json.dumps({"target_tier": 1}),
        relevance_score=1.0,
    )
    _set_state(db, action_id, created_at="2026-05-02T09:00:00+00:00")

    report = build_proactive_yield_forecast_report(db, now=NOW)
    item = report.actions[0]

    assert item.id == action_id
    assert item.score >= 55
    assert item.recommended_next_step == "enrich_context"
    assert item.context_gaps == (
        "missing_target_text",
        "missing_relationship_context",
        "missing_knowledge_ids",
    )


def test_days_limit_min_score_and_status_filter_are_applied(db):
    old = _insert_action(db, target_tweet_id="old")
    _set_state(db, old, created_at="2026-04-01T00:00:00+00:00")
    posted = _insert_action(db, target_tweet_id="posted")
    _set_state(db, posted, status="posted", created_at="2026-05-02T08:00:00+00:00")
    included = _insert_action(db, target_tweet_id="included")
    _set_state(db, included, created_at="2026-05-02T09:00:00+00:00")
    filtered_by_score = _insert_action(
        db,
        target_tweet_id="filtered-by-score",
        action_type="like",
        discovery_source="config",
        relevance_score=0.1,
        relationship_context=json.dumps({"dunbar_tier": 5}),
        knowledge_ids=json.dumps([]),
    )
    _set_state(db, filtered_by_score, created_at="2026-05-02T10:00:00+00:00")

    report = build_proactive_yield_forecast_report(
        db,
        days=3,
        limit=1,
        min_score=55,
        now=NOW,
    )

    assert [item.id for item in report.actions] == [included]
    assert old not in [item.id for item in report.actions]
    assert posted not in [item.id for item in report.actions]
    assert filtered_by_score not in [item.id for item in report.actions]
    assert report.filters["min_score"] == 55


def test_text_formatter_and_missing_schema_are_stable(db):
    action_id = _insert_action(db, target_tweet_id="text")
    _set_state(db, action_id, created_at="2026-05-02T09:00:00+00:00")

    report = build_proactive_yield_forecast_report(db, now=NOW)
    text = format_proactive_yield_forecast_text(report)

    assert "Proactive Yield Forecast" in text
    assert f"#{action_id} reply @alice" in text
    assert "next=execute" in text

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    missing = build_proactive_yield_forecast_report(conn, now=NOW)
    assert missing.total_actions == 0
    assert missing.missing_tables == ("proactive_actions",)
    assert "Missing tables: proactive_actions" in format_proactive_yield_forecast_text(missing)


def test_cli_supports_json_text_min_score_and_validation(db, monkeypatch, capsys):
    action_id = _insert_action(db, target_tweet_id="cli")
    _set_state(db, action_id, created_at="2026-05-02T09:00:00+00:00")
    monkeypatch.setattr(
        proactive_yield_forecast_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        proactive_yield_forecast_script,
        "build_proactive_yield_forecast_report",
        lambda db, **kwargs: build_proactive_yield_forecast_report(db, now=NOW, **kwargs),
    )

    exit_code = proactive_yield_forecast_script.main(
        ["--format", "json", "--days", "7", "--limit", "1", "--min-score", "50"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert list(payload) == sorted(payload)
    assert payload["filters"]["days"] == 7
    assert payload["filters"]["limit"] == 1
    assert payload["actions"][0]["id"] == action_id

    text_exit = proactive_yield_forecast_script.main(["--format", "text"])
    assert text_exit == 0
    assert "Proactive Yield Forecast" in capsys.readouterr().out

    invalid = proactive_yield_forecast_script.main(["--min-score", "-1"])
    captured = capsys.readouterr()
    assert invalid == 2
    assert "value must be non-negative" in captured.err
