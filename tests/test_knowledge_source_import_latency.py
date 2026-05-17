"""Tests for knowledge source import latency reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from knowledge.source_import_latency import build_source_import_latency_report


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "knowledge_source_import_latency.py"
spec = importlib.util.spec_from_file_location("knowledge_source_import_latency_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_summarizes_latency_and_flags_slow_or_incomplete_sources():
    report = build_source_import_latency_report(
        [
            {
                "source_id": "done-fast",
                "discovered_at": "2026-05-15T00:00:00+00:00",
                "ingested_at": "2026-05-15T04:00:00+00:00",
                "embedded_at": "2026-05-15T06:00:00+00:00",
            },
            {"source_id": "pending", "discovered_at": "2026-05-14T00:00:00+00:00"},
            {
                "source_id": "late",
                "discovered_at": "2026-05-13T00:00:00+00:00",
                "ingested_at": "2026-05-15T06:00:00+00:00",
                "embedded_at": "2026-05-15T08:00:00+00:00",
            },
            {"source_id": "no-embeddings", "discovered_at": "2026-05-14T00:00:00+00:00", "ingested_at": "2026-05-14T02:00:00+00:00"},
        ],
        max_latency_hours=24,
        now=NOW,
    )

    statuses = {item["source_id"]: item["status"] for item in report["flagged_sources"]}
    assert statuses["pending"] == "pending_ingestion"
    assert statuses["no-embeddings"] == "ingested_without_embeddings"
    assert statuses["late"] == "completed"
    assert report["totals"]["latency_percentiles"]["p50"] == 56


def test_lookback_filters_old_sources():
    report = build_source_import_latency_report(
        [{"source_id": "old", "discovered_at": "2026-04-01T00:00:00+00:00"}],
        lookback_days=7,
        now=NOW,
    )

    assert report["totals"]["source_count"] == 0
    assert report["empty_state"]["is_empty"] is True


def test_cli_supports_json_text_and_options(monkeypatch, capsys):
    monkeypatch.setattr(script, "script_context", lambda: _script_context(SimpleNamespace()))
    monkeypatch.setattr(
        script,
        "build_source_import_latency_report_from_db",
        lambda _db, **kwargs: build_source_import_latency_report(
            [{"source_id": "pending", "discovered_at": "2026-05-14T00:00:00+00:00"}],
            now=NOW,
            **kwargs,
        ),
    )

    assert script.main(["--lookback-days", "10", "--max-latency-hours", "12", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "source_import_latency"
    assert script.main(["--format", "text"]) == 0
    assert "Knowledge Source Import Latency" in capsys.readouterr().out
