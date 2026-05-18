"""Tests for curated ingestion lag reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path

from evaluation.curated_ingestion_lag import build_curated_ingestion_lag_report


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "curated_ingestion_lag.py"
spec = importlib.util.spec_from_file_location("curated_ingestion_lag_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_computes_item_lags_and_per_source_stats():
    rows = [
        {
            "id": "a",
            "source": "docs",
            "published_at": "2026-05-01T00:00:00+00:00",
            "ingested_at": "2026-05-01T06:00:00+00:00",
            "embedding_at": "2026-05-01T09:00:00+00:00",
            "first_used_at": "2026-05-02T06:00:00+00:00",
        },
        {
            "id": "b",
            "source": "docs",
            "published_at": "2026-05-03T00:00:00Z",
            "ingested_at": "2026-05-04T00:00:00Z",
            "embedding_at": "",
            "first_used_at": "",
        },
        {
            "id": "c",
            "source": "blog",
            "published_at": "2026-05-01T00:00:00+00:00",
            "ingested_at": "2026-05-01T01:00:00+00:00",
            "embedding_at": "2026-05-01T02:00:00+00:00",
            "first_used_at": "2026-05-01T05:00:00+00:00",
        },
    ]

    report = build_curated_ingestion_lag_report(rows, lag_threshold_hours=12, now=NOW)

    assert report["items"][0]["publish_to_ingest_hours"] == 6
    docs = next(source for source in report["per_source"] if source["source"] == "docs")
    assert docs["publish_to_ingest_average_hours"] == 15
    assert docs["ingest_to_embedding_average_hours"] == 3
    assert docs["ingest_to_first_use_average_hours"] == 24
    assert docs["missing_embedding_count"] == 1
    assert docs["never_used_count"] == 1
    assert "publish_to_ingest_p95" in docs["late_reasons"]
    assert report["summary"]["missing_embedding_count"] == 1
    assert report["summary"]["never_used_count"] == 1


def test_flags_sources_that_are_consistently_missing_or_never_used():
    rows = [
        {"id": "a", "source": "slow", "published_at": "2026-05-01T00:00:00Z", "ingested_at": "2026-05-03T00:00:00Z"},
        {"id": "b", "source": "slow", "published_at": "2026-05-02T00:00:00Z", "ingested_at": "2026-05-04T00:00:00Z"},
    ]

    report = build_curated_ingestion_lag_report(rows, lag_threshold_hours=24, now=NOW)

    slow = report["late_sources"][0]
    assert slow["source"] == "slow"
    assert "publish_to_ingest_p95" in slow["late_reasons"]
    assert "all_missing_embedding" in slow["late_reasons"]
    assert "all_never_used" in slow["late_reasons"]


def test_cli_supports_threshold_and_json_table_output(tmp_path, capsys):
    rows_path = tmp_path / "rows.json"
    rows_path.write_text(
        json.dumps(
            [
                {
                    "id": "a",
                    "source": "docs",
                    "published_at": "2026-05-01T00:00:00Z",
                    "ingested_at": "2026-05-02T12:00:00Z",
                }
            ]
        ),
        encoding="utf-8",
    )

    assert script.main(["--rows-json", str(rows_path), "--lag-threshold-hours", "12", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "curated_ingestion_lag"
    assert script.main(["--rows-json", str(rows_path), "--table"]) == 0
    assert "Curated Ingestion Lag" in capsys.readouterr().out
