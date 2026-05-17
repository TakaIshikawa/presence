"""Tests for reply review stuck state reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from engagement.reply_review_stuck_states import build_reply_review_stuck_states_report


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_review_stuck_states.py"
spec = importlib.util.spec_from_file_location("reply_review_stuck_states_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_counts_stuck_reply_drafts_by_state_and_lists_oldest():
    report = build_reply_review_stuck_states_report(
        [
            {"draft_id": "r1", "current_state": "drafted", "last_transition_at": "2026-05-14T00:00:00+00:00"},
            {"draft_id": "r2", "current_state": "approved", "last_transition_at": "2026-05-13T00:00:00+00:00"},
            {"draft_id": "r3", "current_state": "sent", "last_transition_at": "2026-05-12T00:00:00+00:00"},
        ],
        max_age_hours=24,
        now=NOW,
    )

    assert report["totals"]["stuck_count"] == 2
    assert report["totals"]["stuck_by_state"] == {"approved": 1, "drafted": 1}
    assert report["oldest_stuck_items"][0]["draft_id"] == "r2"
    assert report["oldest_stuck_items"][0]["recommended_next_action"] == "send approved reply"


def test_threshold_controls_stuck_count():
    rows = [{"draft_id": "r1", "current_state": "reviewed", "last_transition_at": "2026-05-15T00:00:00+00:00"}]

    assert build_reply_review_stuck_states_report(rows, max_age_hours=6, now=NOW)["totals"]["stuck_count"] == 1
    assert build_reply_review_stuck_states_report(rows, max_age_hours=24, now=NOW)["totals"]["stuck_count"] == 0


def test_cli_supports_json_text_and_max_age(monkeypatch, capsys):
    monkeypatch.setattr(script, "script_context", lambda: _script_context(SimpleNamespace()))
    monkeypatch.setattr(
        script,
        "build_reply_review_stuck_states_report_from_db",
        lambda _db, **kwargs: build_reply_review_stuck_states_report(
            [{"draft_id": "r1", "current_state": "revised", "last_transition_at": "2026-05-14T00:00:00+00:00"}],
            now=NOW,
            **kwargs,
        ),
    )

    assert script.main(["--max-age-hours", "12", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "reply_review_stuck_states"
    assert script.main(["--format", "text"]) == 0
    assert "Reply Review Stuck States" in capsys.readouterr().out
