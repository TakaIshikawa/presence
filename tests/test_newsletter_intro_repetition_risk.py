"""Tests for newsletter intro repetition risk reporting."""

from __future__ import annotations

from contextlib import contextmanager
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.newsletter_intro_repetition_risk import build_newsletter_intro_repetition_risk_report


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_intro_repetition_risk.py"
spec = importlib.util.spec_from_file_location("newsletter_intro_repetition_risk_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_exact_repeats_are_high_risk():
    report = build_newsletter_intro_repetition_risk_report(
        [
            {"id": "n1", "intro": "This week we are tracking AI shipping lessons from product teams."},
            {"id": "n2", "intro": "This week we are tracking AI shipping lessons from product teams."},
        ],
        threshold=0.9,
    )

    assert report["totals"]["flagged_pair_count"] == 1
    pair = report["pairs"][0]
    assert pair["affected_newsletter_ids"] == ["n1", "n2"]
    assert pair["similarity"] == 1.0
    assert pair["risk_level"] == "high"
    assert report["groups"][0]["affected_newsletter_ids"] == ["n1", "n2"]


def test_near_repeats_are_grouped_above_threshold():
    report = build_newsletter_intro_repetition_risk_report(
        [
            {"id": "n1", "intro": "This week we are tracking product teams turning customer signals into better launch bets."},
            {"id": "n2", "intro": "This week we are tracking product teams turning customer signals into sharper launch bets."},
            {"id": "n3", "intro": "This week we are tracking product teams turning customer signals into stronger launch bets."},
        ],
        threshold=0.86,
    )

    assert report["totals"]["flagged_pair_count"] == 3
    assert report["groups"][0]["affected_newsletter_ids"] == ["n1", "n2", "n3"]
    assert all(pair["similarity"] >= 0.86 for pair in report["pairs"])


def test_distinct_intros_are_not_flagged():
    report = build_newsletter_intro_repetition_risk_report(
        [
            {"id": "n1", "intro": "A practical guide to launch notes and changelog pacing."},
            {"id": "n2", "intro": "Subscriber replies exposed three onboarding questions worth answering."},
        ],
        threshold=0.8,
    )

    assert report["pairs"] == []
    assert report["empty_state"]["is_empty"] is True


def test_missing_intro_text_is_counted_not_flagged():
    report = build_newsletter_intro_repetition_risk_report(
        [
            {"id": "n1", "intro": ""},
            {"id": "n2"},
            {"id": "n3", "intro": "A unique opening clause for this issue."},
        ]
    )

    assert report["totals"]["missing_intro_count"] == 2
    assert report["missing_intro_newsletter_ids"] == ["n1", "n2"]
    assert report["pairs"] == []


def test_cli_supports_json_and_table(monkeypatch, capsys):
    monkeypatch.setattr(script, "script_context", lambda: _script_context(SimpleNamespace()))
    monkeypatch.setattr(
        script,
        "build_newsletter_intro_repetition_risk_report_from_db",
        lambda _db, **kwargs: build_newsletter_intro_repetition_risk_report(
            [
                {"id": "n1", "intro": "Same opening line for a weekly digest."},
                {"id": "n2", "intro": "Same opening line for a weekly digest."},
            ],
            **kwargs,
        ),
    )

    assert script.main(["--threshold", "0.8", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["pairs"][0]["risk_level"] == "high"
    assert script.main(["--format", "table"]) == 0
    assert "left_id | right_id" in capsys.readouterr().out
