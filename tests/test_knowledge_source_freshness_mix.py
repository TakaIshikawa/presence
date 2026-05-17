"""Tests for knowledge source freshness mix reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.knowledge_source_freshness_mix import build_knowledge_source_freshness_mix_report


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "knowledge_source_freshness_mix.py"
spec = importlib.util.spec_from_file_location("knowledge_source_freshness_mix_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_groups_sources_into_freshness_buckets_per_run():
    report = build_knowledge_source_freshness_mix_report(
        [
            {"run_id": "r1", "source_id": "s1", "published_at": "2026-04-25T00:00:00+00:00"},
            {"run_id": "r1", "source_id": "s2", "published_at": "2026-03-15T00:00:00+00:00"},
            {"run_id": "r1", "source_id": "s3", "published_at": "2025-12-01T00:00:00+00:00"},
        ],
        fresh_days=30,
        aging_days=90,
        now=NOW,
    )

    run = report["runs"][0]
    assert run["bucket_counts"] == {"fresh": 1, "aging": 1, "stale": 1, "unknown": 0}
    assert run["bucket_percentages"]["fresh"] == 0.3333


def test_flags_runs_dominated_by_old_material():
    report = build_knowledge_source_freshness_mix_report(
        [
            {"run_id": "old", "source_id": "s1", "published_at": "2025-01-01T00:00:00+00:00"},
            {"run_id": "old", "source_id": "s2", "published_at": "2025-02-01T00:00:00+00:00"},
            {"run_id": "old", "source_id": "s3", "published_at": "2026-04-25T00:00:00+00:00"},
        ],
        stale_dominance_threshold=0.5,
        now=NOW,
    )

    assert report["runs"][0]["run_id"] == "old"
    assert report["runs"][0]["stale_dominance_flag"] is True
    assert report["totals"]["stale_dominated_run_count"] == 1


def test_unknown_dates_are_reported_without_stale_flag():
    report = build_knowledge_source_freshness_mix_report([{"run_id": "r1", "source_id": "s1"}], now=NOW)

    assert report["runs"][0]["bucket_counts"]["unknown"] == 1
    assert report["runs"][0]["stale_dominance_flag"] is False


def test_distinct_recent_mix_is_not_flagged():
    report = build_knowledge_source_freshness_mix_report(
        [
            {"run_id": "fresh", "source_id": "s1", "published_at": "2026-04-28T00:00:00+00:00"},
            {"run_id": "fresh", "source_id": "s2", "published_at": "2026-04-20T00:00:00+00:00"},
        ],
        now=NOW,
    )

    assert report["runs"][0]["bucket_percentages"]["fresh"] == 1.0
    assert report["runs"][0]["stale_dominance_flag"] is False


def test_cli_supports_json_and_table(monkeypatch, capsys):
    monkeypatch.setattr(script, "script_context", lambda: _script_context(SimpleNamespace()))
    monkeypatch.setattr(
        script,
        "build_knowledge_source_freshness_mix_report_from_db",
        lambda _db, **kwargs: build_knowledge_source_freshness_mix_report(
            [{"run_id": "r1", "source_id": "s1", "published_at": "2025-01-01T00:00:00+00:00"}],
            now=NOW,
            **kwargs,
        ),
    )

    assert script.main(["--fresh-days", "30", "--aging-days", "90", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["runs"][0]["bucket_counts"]["stale"] == 1
    assert script.main(["--format", "table"]) == 0
    assert "run_id | sources" in capsys.readouterr().out
