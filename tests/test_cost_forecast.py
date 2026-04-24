"""Tests for model cost forecasting."""

import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from cost_forecast import format_json_report, format_text_report, main
from evaluation.cost_forecast import build_cost_forecast, forecast_from_db


NOW = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)


def test_build_cost_forecast_reports_budget_and_safe_runs():
    rows = [
        {
            "operation_name": "synthesis.generate_candidates.x_post",
            "estimated_cost": 0.12,
            "pipeline_run_id": 1,
            "pipeline_content_type": "x_post",
        },
        {
            "operation_name": "synthesis.evaluate_candidates",
            "estimated_cost": 0.08,
            "pipeline_run_id": 1,
            "pipeline_content_type": "x_post",
        },
        {
            "operation_name": "synthesis.generate_candidates.x_thread",
            "estimated_cost": 0.30,
            "pipeline_run_id": 2,
            "pipeline_content_type": "x_thread",
        },
    ]

    forecast = build_cost_forecast(
        rows,
        today_spend=0.60,
        max_estimated_cost_per_run=0.40,
        max_daily_estimated_cost=1.00,
        lookback_days=14,
        now=NOW,
    )

    assert forecast.status == "near_limit"
    assert forecast.today_spend == 0.60
    assert forecast.remaining_daily_budget == 0.40
    assert forecast.average_recent_run_cost == 0.25
    assert forecast.safe_run_count_today == 1
    assert [item.content_type for item in forecast.content_types] == [
        "x_post",
        "x_thread",
    ]
    assert forecast.content_types[0].average_run_cost == 0.20
    assert forecast.content_types[0].safe_run_count_today == 2


def test_build_cost_forecast_marks_over_limit():
    forecast = build_cost_forecast(
        [
            {
                "operation_name": "synthesis.generate_candidates.x_post",
                "estimated_cost": 0.25,
                "pipeline_run_id": 1,
            }
        ],
        today_spend=1.20,
        max_estimated_cost_per_run=0.20,
        max_daily_estimated_cost=1.00,
        now=NOW,
    )

    assert forecast.status == "over_limit"
    assert forecast.content_types[0].status == "over_limit"
    assert forecast.safe_run_count_today == 0


def test_build_cost_forecast_handles_missing_budget_as_unlimited():
    forecast = build_cost_forecast(
        [],
        today_spend=0.0,
        max_estimated_cost_per_run=None,
        max_daily_estimated_cost=None,
        now=NOW,
    )

    assert forecast.status == "ok"
    assert forecast.budget_configured is False
    assert forecast.remaining_daily_budget is None
    assert forecast.safe_run_count_today is None
    assert "unlimited" in forecast.message


def test_format_json_report_is_stable_and_contains_status_values():
    forecast = build_cost_forecast(
        [
            {
                "operation_name": "synthesis.generate_candidates.x_post",
                "estimated_cost": 0.10,
                "pipeline_run_id": 1,
            }
        ],
        today_spend=0.10,
        max_daily_estimated_cost=1.00,
        now=NOW,
    )

    payload = json.loads(format_json_report(forecast))

    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["status"] == "ok"
    assert payload["content_types"][0]["status"] == "ok"
    assert payload["operations"][0]["content_type"] == "x_post"


def test_format_text_report_includes_forecast_fields():
    forecast = build_cost_forecast(
        [
            {
                "operation_name": "synthesis.refine.x_post",
                "estimated_cost": 0.10,
                "pipeline_run_id": 1,
            }
        ],
        today_spend=0.10,
        max_daily_estimated_cost=1.00,
        now=NOW,
    )

    output = format_text_report(forecast)

    assert "Model Cost Forecast" in output
    assert "Today spend:" in output
    assert "Remaining today:" in output
    assert "Safe runs today:" in output
    assert "synthesis.refine.x_post" in output


def test_forecast_from_db_reads_existing_usage_without_writes(db):
    run_id = db.insert_pipeline_run(
        batch_id="batch-1",
        content_type="x_post",
        candidates_generated=3,
        best_candidate_index=0,
        best_score_before_refine=7.0,
    )
    db.record_model_usage(
        "claude-sonnet-4-6",
        "synthesis.generate_candidates.x_post",
        100,
        25,
        estimated_cost=0.30,
        pipeline_run_id=run_id,
    )
    db.conn.execute(
        "UPDATE model_usage SET created_at = ?", ("2026-04-24 10:00:00",)
    )
    db.conn.commit()
    before = db.conn.total_changes

    forecast = forecast_from_db(
        db,
        max_daily_estimated_cost=1.00,
        lookback_days=30,
        now=NOW,
    )

    assert forecast.today_spend == 0.30
    assert forecast.content_types[0].content_type == "x_post"
    assert db.conn.total_changes == before


def test_main_prints_json_without_initializing_or_syncing_db(capsys):
    class ReadOnlyDb:
        def __init__(self, _path):
            self.conn = sqlite3.connect(":memory:")
            self.conn.row_factory = sqlite3.Row
            self.closed = False

        def connect(self):
            self.conn.executescript(
                """
                CREATE TABLE model_usage (
                    id INTEGER PRIMARY KEY,
                    operation_name TEXT,
                    estimated_cost REAL,
                    content_id INTEGER,
                    pipeline_run_id INTEGER,
                    created_at TEXT
                );
                CREATE TABLE pipeline_runs (id INTEGER PRIMARY KEY, content_type TEXT);
                CREATE TABLE generated_content (id INTEGER PRIMARY KEY, content_type TEXT);
                INSERT INTO pipeline_runs (id, content_type) VALUES (1, 'x_post');
                INSERT INTO model_usage
                  (id, operation_name, estimated_cost, pipeline_run_id, created_at)
                  VALUES (1, 'synthesis.generate_candidates.x_post', 0.20, 1, '2026-04-24 01:00:00');
                """
            )

        def close(self):
            self.closed = True
            self.conn.close()

    config = SimpleNamespace(
        paths=SimpleNamespace(database="ignored.db"),
        synthesis=SimpleNamespace(
            max_estimated_cost_per_run=0.50,
            max_daily_estimated_cost=1.00,
        ),
    )

    with patch("cost_forecast.load_config", return_value=config), patch(
        "cost_forecast.Database", ReadOnlyDb
    ), patch("sys.argv", ["cost_forecast.py", "--format", "json"]):
        main()

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "ok"
    assert payload["today_spend"] == 0.2
