"""Tests for publication retry outcome cohort reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.publication_retry_outcome_cohorts import build_publication_retry_outcome_cohorts_report


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_retry_outcome_cohorts.py"
spec = importlib.util.spec_from_file_location("publication_retry_outcome_cohorts_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_classifies_retry_histories_into_cohorts():
    rows = [
        {"content_id": "a", "status": "failed", "attempted_at": "2026-05-14T00:00:00+00:00"},
        {"content_id": "a", "status": "published", "attempted_at": "2026-05-14T01:00:00+00:00"},
        {"content_id": "b", "status": "failed", "attempted_at": "2026-05-14T00:00:00+00:00"},
        {"content_id": "c", "status": "abandoned", "attempted_at": "2026-05-14T00:00:00+00:00"},
        {"content_id": "d", "status": "published", "attempted_at": "2026-05-14T00:00:00+00:00"},
        {"content_id": "d", "status": "failed", "attempted_at": "2026-05-14T01:00:00+00:00"},
    ]

    report = build_publication_retry_outcome_cohorts_report(rows, now=NOW)

    assert report["cohorts"]["recovered"]["count"] == 1
    assert report["cohorts"]["still_failing"]["count"] == 1
    assert report["cohorts"]["abandoned"]["count"] == 1
    assert report["cohorts"]["flaky"]["count"] == 1
    assert report["totals"]["recovery_rate"] == 0.5
    assert report["totals"]["median_attempts_to_recovery"] == 1.5


def test_lookback_days_filters_old_attempts():
    report = build_publication_retry_outcome_cohorts_report(
        [{"content_id": "old", "status": "failed", "attempted_at": "2026-04-01T00:00:00+00:00"}],
        lookback_days=7,
        now=NOW,
    )

    assert report["totals"]["item_count"] == 0
    assert report["empty_state"]["is_empty"] is True


def test_cli_supports_json_text_and_lookback(monkeypatch, capsys):
    monkeypatch.setattr(script, "script_context", lambda: _script_context(SimpleNamespace()))
    monkeypatch.setattr(
        script,
        "build_publication_retry_outcome_cohorts_report_from_db",
        lambda _db, **kwargs: build_publication_retry_outcome_cohorts_report(
            [{"content_id": "a", "status": "failed"}, {"content_id": "a", "status": "published"}],
            **kwargs,
        ),
    )

    assert script.main(["--lookback-days", "14", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "publication_retry_outcome_cohorts"
    assert script.main(["--format", "text"]) == 0
    assert "Publication Retry Outcome Cohorts" in capsys.readouterr().out
