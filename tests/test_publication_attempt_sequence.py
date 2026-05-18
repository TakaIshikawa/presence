"""Tests for publication attempt sequence reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path

from evaluation.publication_attempt_sequence import build_publication_attempt_sequence_report


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_attempt_sequence.py"
spec = importlib.util.spec_from_file_location("publication_attempt_sequence_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_groups_attempts_by_content_and_orders_by_attempted_at():
    rows = [
        {"content_id": "p1", "attempt_id": "late", "attempted_at": "2026-05-01T02:00:00Z", "status": "success", "channel": "linkedin"},
        {"content_id": "p1", "attempt_id": "early", "attempted_at": "2026-05-01T01:00:00Z", "status": "failed", "error": "rate_limit", "channel": "linkedin"},
    ]

    report = build_publication_attempt_sequence_report(rows, now=NOW)

    sequence = report["sequences"][0]
    assert [attempt["attempt_id"] for attempt in sequence["attempts"]] == ["early", "late"]
    assert report["summary"]["attempts"] == 2


def test_flags_repeated_errors_retry_after_success_channel_mismatch_and_long_gap():
    rows = [
        {
            "content_id": "p1",
            "attempt_id": "a1",
            "attempted_at": "2026-05-01T00:00:00Z",
            "status": "failed",
            "error": "timeout",
            "channel": "mastodon",
            "expected_channel": "mastodon",
        },
        {
            "content_id": "p1",
            "attempt_id": "a2",
            "attempted_at": "2026-05-01T01:00:00Z",
            "status": "failed",
            "error": "timeout",
            "channel": "mastodon",
            "expected_channel": "mastodon",
        },
        {
            "content_id": "p1",
            "attempt_id": "a3",
            "attempted_at": "2026-05-03T05:00:00Z",
            "status": "published",
            "channel": "linkedin",
            "expected_channel": "mastodon",
        },
        {
            "content_id": "p1",
            "attempt_id": "a4",
            "attempted_at": "2026-05-03T06:00:00Z",
            "status": "failed",
            "error": "duplicate",
            "channel": "linkedin",
            "expected_channel": "mastodon",
        },
    ]

    report = build_publication_attempt_sequence_report(rows, max_retry_gap_hours=24, now=NOW)
    types = {anomaly["type"] for anomaly in report["anomalies"]}

    assert {"repeated_same_error", "retry_after_success", "channel_mismatch", "excessive_retry_gap"}.issubset(types)
    assert report["summary"]["anomaly_counts"]["repeated_same_error"] == 1
    assert report["summary"]["anomaly_counts"]["retry_after_success"] == 1


def test_cli_accepts_gap_threshold_and_outputs_json_and_table(tmp_path, capsys):
    path = tmp_path / "attempts.json"
    path.write_text(
        json.dumps(
            [
                {"content_id": "p1", "attempted_at": "2026-05-01T00:00:00Z", "status": "failed", "error": "x"},
                {"content_id": "p1", "attempted_at": "2026-05-02T06:00:00Z", "status": "failed", "error": "y"},
            ]
        ),
        encoding="utf-8",
    )

    assert script.main(["--attempts-json", str(path), "--max-retry-gap-hours", "12", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "publication_attempt_sequence"
    assert script.main(["--attempts-json", str(path), "--table"]) == 0
    assert "Publication Attempt Sequence" in capsys.readouterr().out
