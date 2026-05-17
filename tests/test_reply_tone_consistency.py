"""Tests for reply tone consistency reporting."""

from __future__ import annotations

from contextlib import contextmanager
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from engagement.reply_tone_consistency import build_reply_tone_consistency_report


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_tone_consistency.py"
spec = importlib.util.spec_from_file_location("reply_tone_consistency_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_builds_baseline_from_approved_and_published_replies():
    report = build_reply_tone_consistency_report(
        [
            {"id": 1, "status": "approved", "draft_text": "I would frame this as a smaller operational tradeoff with one next step."},
            {"id": 2, "status": "published", "draft_text": "That matches what I have seen: start narrow, measure the result, then expand."},
            {"id": 3, "status": "draft", "draft_text": "Great point? Thanks for sharing? Amazing! Brilliant!"},
        ]
    )

    assert report["baseline"]["sample_count"] == 2
    assert report["totals"]["draft_count"] == 1
    assert report["flagged_drafts"][0]["feature_deltas"]["question_count"] > 0


def test_flags_terse_effusive_generic_and_question_heavy_drafts():
    report = build_reply_tone_consistency_report(
        [
            {"id": "b1", "status": "approved", "draft_text": "I would separate the policy question from the implementation path before deciding."},
            {"id": "b2", "status": "approved", "draft_text": "The useful next step is to test the assumption with a smaller audience."},
            {"id": "d1", "status": "draft", "draft_text": "Great point? Thanks for sharing? Amazing! Awesome!"},
        ]
    )

    reasons = set(report["flagged_drafts"][0]["drift_reasons"])
    assert {"unusually_terse", "unusually_effusive", "generic_language", "question_heavy"} <= reasons
    assert report["flagged_drafts"][0]["severity"] == "high"


def test_unflagged_draft_has_low_severity():
    report = build_reply_tone_consistency_report(
        [
            {"id": "b1", "status": "approved", "draft_text": "I would turn that into one concrete experiment and compare the result next week."},
            {"id": "d1", "status": "draft", "draft_text": "I would turn that into one concrete experiment and compare the result next week."},
        ]
    )

    assert report["flagged_drafts"] == []
    assert report["drafts"][0]["severity"] == "low"


def test_missing_baseline_is_reported_as_drift_reason():
    report = build_reply_tone_consistency_report([{"id": "d1", "status": "draft", "draft_text": "Draft only."}])

    assert report["flagged_drafts"][0]["drift_reasons"] == ["missing_baseline"]


def test_cli_supports_json_and_table(monkeypatch, capsys):
    monkeypatch.setattr(script, "script_context", lambda: _script_context(SimpleNamespace()))
    monkeypatch.setattr(
        script,
        "build_reply_tone_consistency_report_from_db",
        lambda _db, **kwargs: build_reply_tone_consistency_report(
            [
                {"id": "b1", "status": "approved", "draft_text": "A measured reply with concrete next steps and context."},
                {"id": "d1", "status": "draft", "draft_text": "Great point? Amazing! Awesome!"},
            ],
            **kwargs,
        ),
    )

    assert script.main(["--baseline-limit", "5", "--draft-limit", "5", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["flagged_drafts"][0]["reply_id"] == "d1"
    assert script.main(["--format", "table"]) == 0
    assert "reply_id | severity" in capsys.readouterr().out
