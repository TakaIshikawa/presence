"""Tests for publication channel outcome mix reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.publication_channel_outcome_mix import build_publication_channel_outcome_mix_report


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_channel_outcome_mix.py"
spec = importlib.util.spec_from_file_location("publication_channel_outcome_mix_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_counts_and_percentages_grouped_by_channel():
    report = build_publication_channel_outcome_mix_report(
        [
            {"channel": "x", "status": "published", "created_at": "2026-04-30T00:00:00+00:00"},
            {"channel": "x", "status": "failed", "created_at": "2026-04-30T00:00:00+00:00"},
            {"channel": "newsletter", "status": "pending", "created_at": "2026-04-30T00:00:00+00:00"},
        ],
        now=NOW,
    )

    x_row = next(row for row in report["channels"] if row["channel"] == "x")
    assert x_row["counts"] == {"success": 1, "failure": 1, "retry": 0, "pending": 0}
    assert x_row["percentages"]["failure"] == 0.5


def test_flags_channels_exceeding_failure_or_pending_thresholds():
    report = build_publication_channel_outcome_mix_report(
        [
            {"channel": "blog", "status": "failed"},
            {"channel": "blog", "status": "published"},
            {"channel": "newsletter", "status": "pending"},
            {"channel": "newsletter", "status": "published"},
        ],
        failure_threshold=0.5,
        pending_threshold=0.5,
        now=NOW,
    )

    flags = {row["channel"]: row["flags"] for row in report["channels"]}
    assert flags["blog"] == ["failure_rate_exceeded"]
    assert flags["newsletter"] == ["pending_rate_exceeded"]
    assert report["totals"]["flagged_channel_count"] == 2


def test_old_rows_are_excluded_from_recent_window():
    report = build_publication_channel_outcome_mix_report(
        [
            {"channel": "x", "status": "failed", "created_at": "2026-01-01T00:00:00+00:00"},
            {"channel": "x", "status": "published", "created_at": "2026-04-30T00:00:00+00:00"},
        ],
        days=30,
        now=NOW,
    )

    assert report["totals"]["rows_scanned"] == 1
    assert report["channels"][0]["counts"]["success"] == 1


def test_retry_status_is_counted_separately():
    report = build_publication_channel_outcome_mix_report([{"channel": "x", "status": "retrying"}], now=NOW)

    assert report["channels"][0]["counts"]["retry"] == 1
    assert report["channels"][0]["percentages"]["retry"] == 1.0


def test_cli_supports_json_and_table(monkeypatch, capsys):
    monkeypatch.setattr(script, "script_context", lambda: _script_context(SimpleNamespace()))
    monkeypatch.setattr(
        script,
        "build_publication_channel_outcome_mix_report_from_db",
        lambda _db, **kwargs: build_publication_channel_outcome_mix_report(
            [{"channel": "blog", "status": "failed"}],
            now=NOW,
            **kwargs,
        ),
    )

    assert script.main(["--days", "14", "--failure-threshold", "0.1", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["channels"][0]["flags"] == ["failure_rate_exceeded"]
    assert script.main(["--format", "table"]) == 0
    assert "channel | total" in capsys.readouterr().out
