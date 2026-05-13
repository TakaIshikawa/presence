"""Tests for proactive action lifecycle aging report."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from engagement.proactive_action_lifecycle_aging import (
    build_proactive_action_lifecycle_aging_report,
    format_proactive_action_lifecycle_aging_json,
    format_proactive_action_lifecycle_aging_text,
)


NOW = datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "proactive_action_lifecycle_aging.py"
)
spec = importlib.util.spec_from_file_location(
    "proactive_action_lifecycle_aging_script",
    SCRIPT_PATH,
)
proactive_action_lifecycle_aging_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(proactive_action_lifecycle_aging_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_action(db, **kwargs) -> int:
    defaults = dict(
        action_type="reply",
        target_tweet_id="target-1",
        target_tweet_text="Useful thought",
        target_author_handle="alice",
        target_author_id="author-a",
        discovery_source="search",
        relevance_score=0.8,
        draft_text="Good point.",
    )
    defaults.update(kwargs)
    return db.insert_proactive_action(**defaults)


def _set_state(
    db,
    action_id: int,
    *,
    status: str,
    created_at: str,
    reviewed_at: str | None = None,
    posted_at: str | None = None,
    posted_tweet_id: str | None = None,
) -> None:
    db.conn.execute(
        """UPDATE proactive_actions
           SET status = ?, created_at = ?, reviewed_at = ?, posted_at = ?, posted_tweet_id = ?
           WHERE id = ?""",
        (status, created_at, reviewed_at, posted_at, posted_tweet_id, action_id),
    )
    db.conn.commit()


def test_report_flags_lifecycle_findings_and_groups(db):
    stale = _insert_action(
        db,
        target_tweet_id="stale",
        target_author_handle="alice",
        relevance_score=0.2,
        discovery_source="search",
    )
    _set_state(db, stale, status="pending", created_at="2026-05-09T10:00:00+00:00")

    approved = _insert_action(
        db,
        target_tweet_id="approved",
        action_type="quote_tweet",
        target_author_handle="bob",
        discovery_source="cultivate",
    )
    _set_state(
        db,
        approved,
        status="approved",
        created_at="2026-05-10T10:00:00+00:00",
        reviewed_at="2026-05-11T08:00:00+00:00",
    )

    posted = _insert_action(
        db,
        target_tweet_id="posted",
        action_type="like",
        target_author_handle="carol",
        discovery_source="curated_timeline",
    )
    _set_state(
        db,
        posted,
        status="posted",
        created_at="2026-05-12T08:00:00+00:00",
        reviewed_at="2026-05-12T09:00:00+00:00",
        posted_at="2026-05-12T10:00:00+00:00",
    )

    dismissed = _insert_action(
        db,
        target_tweet_id="dismissed",
        target_author_handle="alice",
        discovery_source="search",
    )
    _set_state(
        db,
        dismissed,
        status="dismissed",
        created_at="2026-05-12T08:00:00+00:00",
        reviewed_at="2026-05-12T12:00:00+00:00",
    )

    report = build_proactive_action_lifecycle_aging_report(
        db,
        stale_pending_hours=72,
        approved_not_posted_hours=24,
        low_relevance_percent=30,
        now=NOW,
    )
    payload = json.loads(format_proactive_action_lifecycle_aging_json(report))
    labels = [finding["label"] for finding in payload["findings"]]
    by_id = {action["id"]: action for action in payload["actions"]}

    assert payload["artifact_type"] == "proactive_action_lifecycle_aging"
    assert labels == [
        "stale_pending",
        "approved_not_posted",
        "posted_missing_platform_id",
        "low_relevance_pending",
    ]
    assert by_id[stale]["age_hours"] == 98.0
    assert by_id[stale]["age_bucket"] == "3-7d"
    assert by_id[stale]["finding_labels"] == [
        "stale_pending",
        "low_relevance_pending",
    ]
    assert by_id[approved]["age_basis"] == "reviewed_at"
    assert by_id[posted]["finding_labels"] == ["posted_missing_platform_id"]
    assert payload["totals"]["pending_count"] == 1
    assert payload["totals"]["approved_count"] == 1
    assert payload["totals"]["posted_count"] == 1
    assert payload["totals"]["dismissed_count"] == 1

    status_groups = {group["status"]: group for group in payload["status_groups"]}
    assert status_groups["pending"]["age_buckets"] == {"3-7d": 1}
    assert status_groups["approved"]["discovery_sources"] == {"cultivate": 1}
    author_groups = {
        group["target_author_handle"]: group for group in payload["author_groups"]
    }
    assert author_groups["alice"]["statuses"] == {"dismissed": 1, "pending": 1}


def test_limit_sorting_text_and_missing_schema(db):
    fresh = _insert_action(db, target_tweet_id="fresh", target_author_handle="bob")
    _set_state(db, fresh, status="pending", created_at="2026-05-13T11:00:00+00:00")
    stale = _insert_action(db, target_tweet_id="old", target_author_handle="alice")
    _set_state(db, stale, status="pending", created_at="2026-05-08T11:00:00+00:00")

    report = build_proactive_action_lifecycle_aging_report(db, limit=1, now=NOW)
    text = format_proactive_action_lifecycle_aging_text(report)

    assert [action.id for action in report.actions] == [stale]
    assert report.totals["rows_scanned"] == 2
    assert "Proactive Action Lifecycle Aging" in text
    assert "stale_pending" in text

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    missing = build_proactive_action_lifecycle_aging_report(conn, now=NOW)
    assert missing.missing_tables == ("proactive_actions",)
    assert "Missing tables: proactive_actions" in (
        format_proactive_action_lifecycle_aging_text(missing)
    )


def test_invalid_thresholds_raise_value_error(db):
    with pytest.raises(ValueError, match="days must be positive"):
        build_proactive_action_lifecycle_aging_report(db, days=0, now=NOW)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_proactive_action_lifecycle_aging_report(db, limit=0, now=NOW)
    with pytest.raises(ValueError, match="stale_pending_hours must be positive"):
        build_proactive_action_lifecycle_aging_report(db, stale_pending_hours=0, now=NOW)
    with pytest.raises(ValueError, match="approved_not_posted_hours must be positive"):
        build_proactive_action_lifecycle_aging_report(db, approved_not_posted_hours=0, now=NOW)
    with pytest.raises(ValueError, match="low_relevance_percent must be positive"):
        build_proactive_action_lifecycle_aging_report(db, low_relevance_percent=0, now=NOW)


def test_cli_supports_json_format_and_positive_integer_validation(db, monkeypatch, capsys):
    action_id = _insert_action(db, target_tweet_id="cli", relevance_score=0.1)
    _set_state(db, action_id, status="pending", created_at="2026-05-09T10:00:00+00:00")
    monkeypatch.setattr(
        proactive_action_lifecycle_aging_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        proactive_action_lifecycle_aging_script,
        "build_proactive_action_lifecycle_aging_report",
        lambda db, **kwargs: build_proactive_action_lifecycle_aging_report(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    exit_code = proactive_action_lifecycle_aging_script.main(
        [
            "--format",
            "json",
            "--days",
            "14",
            "--limit",
            "5",
            "--stale-pending-hours",
            "48",
            "--approved-not-posted-hours",
            "12",
            "--low-relevance-percent",
            "20",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["filters"]["days"] == 14
    assert payload["filters"]["limit"] == 5
    assert payload["filters"]["stale_pending_hours"] == 48
    assert payload["filters"]["approved_not_posted_hours"] == 12
    assert payload["filters"]["low_relevance_percent"] == 20
    assert payload["actions"][0]["id"] == action_id

    invalid = proactive_action_lifecycle_aging_script.main(["--days", "0"])
    captured = capsys.readouterr()
    assert invalid == 2
    assert "value must be positive" in captured.err
