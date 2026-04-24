"""Tests for model usage anomaly diagnostics."""

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from evaluation.model_usage_anomalies import (
    build_model_usage_anomaly_report,
    format_model_usage_anomaly_report,
)
from model_usage_anomalies import main


NOW = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)


def _ts(days_ago: float) -> str:
    return (NOW - timedelta(days=days_ago)).strftime("%Y-%m-%d %H:%M:%S")


def _usage(db, operation, model, tokens, cost, days_ago):
    usage_id = db.record_model_usage(
        model,
        operation,
        input_tokens=tokens,
        output_tokens=0,
        estimated_cost=cost,
    )
    db.conn.execute(
        "UPDATE model_usage SET created_at = ? WHERE id = ?",
        (_ts(days_ago), usage_id),
    )
    db.conn.commit()
    return usage_id


def test_no_baseline_reports_current_operation_without_anomaly(db):
    for _ in range(3):
        _usage(db, "synthesis.generate_x_post", "claude-sonnet", 100, 0.01, 1)

    report = build_model_usage_anomaly_report(db, days=3, min_samples=3, now=NOW)

    assert report["status"] == "ok"
    row = report["rows"][0]
    assert row["operation_name"] == "synthesis.generate_x_post"
    assert row["model_name"] == "claude-sonnet"
    assert row["status"] == "no_baseline"
    assert row["severity"] == "info"
    assert row["current_avg_tokens"] == 100
    assert row["baseline_avg_tokens"] is None


def test_normal_usage_is_not_anomalous(db):
    for _ in range(4):
        _usage(db, "synthesis.evaluate", "claude-sonnet", 100, 0.01, 4)
        _usage(db, "synthesis.evaluate", "claude-sonnet", 110, 0.011, 1)

    report = build_model_usage_anomaly_report(
        db, days=3, min_samples=3, threshold=2.0, now=NOW
    )

    row = report["rows"][0]
    assert report["status"] == "ok"
    assert row["status"] == "normal"
    assert row["sample_count"] == 4
    assert row["baseline_sample_count"] == 4
    assert row["token_ratio"] == 1.1
    assert row["cost_ratio"] == 1.1


def test_token_spike_is_anomalous(db):
    for _ in range(3):
        _usage(db, "synthesis.refine", "claude-sonnet", 100, 0.01, 4)
        _usage(db, "synthesis.refine", "claude-sonnet", 350, 0.012, 1)

    report = build_model_usage_anomaly_report(
        db, days=3, min_samples=3, threshold=2.0, now=NOW
    )

    row = report["rows"][0]
    assert report["status"] == "warning"
    assert row["status"] == "anomalous"
    assert row["severity"] == "warning"
    assert row["current_avg_tokens"] == 350
    assert row["baseline_avg_tokens"] == 100
    assert row["token_ratio"] == 3.5


def test_cost_spike_is_anomalous(db):
    for _ in range(3):
        _usage(db, "synthesis.generate_thread", "claude-opus", 200, 0.02, 4)
        _usage(db, "synthesis.generate_thread", "claude-opus", 210, 0.08, 1)

    report = build_model_usage_anomaly_report(
        db, days=3, min_samples=3, threshold=2.0, now=NOW
    )

    row = report["rows"][0]
    assert row["status"] == "anomalous"
    assert row["cost_ratio"] == 4.0


def test_insufficient_current_samples_reported_not_anomalous(db):
    for _ in range(4):
        _usage(db, "synthesis.rank", "claude-sonnet", 100, 0.01, 4)
    _usage(db, "synthesis.rank", "claude-sonnet", 1000, 0.1, 1)

    report = build_model_usage_anomaly_report(db, days=3, min_samples=3, now=NOW)

    row = report["rows"][0]
    assert report["status"] == "ok"
    assert row["status"] == "insufficient_data"
    assert row["severity"] == "info"


def test_operation_filter_limits_report(db):
    for _ in range(3):
        _usage(db, "included", "claude-sonnet", 100, 0.01, 4)
        _usage(db, "included", "claude-sonnet", 300, 0.03, 1)
        _usage(db, "excluded", "claude-sonnet", 100, 0.01, 1)

    report = build_model_usage_anomaly_report(
        db,
        days=3,
        min_samples=3,
        operations=["included"],
        now=NOW,
    )

    assert [row["operation_name"] for row in report["rows"]] == ["included"]


def test_format_text_report_includes_status_and_rows(db):
    for _ in range(3):
        _usage(db, "synthesis.refine", "claude-sonnet", 100, 0.01, 4)
        _usage(db, "synthesis.refine", "claude-sonnet", 300, 0.03, 1)

    report = build_model_usage_anomaly_report(db, days=3, min_samples=3, now=NOW)
    output = format_model_usage_anomaly_report(report)

    assert "MODEL USAGE ANOMALIES" in output
    assert "synthesis.refine" in output
    assert "anomalous" in output


def test_main_prints_json_report_with_operation_filter(db, capsys):
    for _ in range(3):
        _usage(db, "included", "claude-sonnet", 100, 0.01, 4)
        _usage(db, "included", "claude-sonnet", 300, 0.03, 1)
        _usage(db, "excluded", "claude-sonnet", 100, 0.01, 1)

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("model_usage_anomalies.script_context", fake_script_context), patch(
        "model_usage_anomalies.build_model_usage_anomaly_report"
    ) as build_report, patch(
        "sys.argv",
        [
            "model_usage_anomalies.py",
            "--days",
            "3",
            "--min-samples",
            "3",
            "--threshold",
            "2",
            "--operation",
            "included",
            "--json",
        ],
    ):
        build_report.return_value = build_model_usage_anomaly_report(
            db,
            days=3,
            min_samples=3,
            threshold=2,
            operations=["included"],
            now=NOW,
        )
        main()

    build_report.assert_called_once()
    _, kwargs = build_report.call_args
    assert kwargs["operations"] == ["included"]
    payload = json.loads(capsys.readouterr().out)
    assert [row["operation_name"] for row in payload["rows"]] == ["included"]
