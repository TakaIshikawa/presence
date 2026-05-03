"""Tests for model usage provider mix report."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.model_usage_provider_mix import (
    build_model_usage_provider_mix_report,
    format_model_usage_provider_mix_json,
    format_model_usage_provider_mix_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "model_usage_provider_mix.py"
spec = importlib.util.spec_from_file_location("model_usage_provider_mix_script", SCRIPT_PATH)
model_usage_provider_mix_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(model_usage_provider_mix_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _ts(days_ago: float) -> str:
    return (NOW - timedelta(days=days_ago)).strftime("%Y-%m-%d %H:%M:%S")


def _usage(
    db,
    *,
    model_name: str = "claude-sonnet-4-6",
    operation_name: str = "synthesis.generate_x_post",
    input_tokens: int = 100,
    output_tokens: int = 50,
    total_tokens: int = 150,
    days_ago: float = 1,
) -> int:
    cursor = db.conn.execute(
        """INSERT INTO model_usage
           (model_name, operation_name, input_tokens, output_tokens, total_tokens,
            estimated_cost, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            model_name,
            operation_name,
            input_tokens,
            output_tokens,
            total_tokens,
            0.01,
            _ts(days_ago),
        ),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def test_groups_by_provider_model_stage(db):
    _usage(db, model_name="claude-sonnet-4-6", operation_name="synthesis.generate", days_ago=1)
    _usage(db, model_name="claude-sonnet-4-6", operation_name="synthesis.generate", days_ago=1)
    _usage(db, model_name="gpt-4", operation_name="synthesis.evaluate", days_ago=1)
    _usage(db, model_name="gemini-pro", operation_name="engagement.reply", days_ago=1)

    report = build_model_usage_provider_mix_report(db, days=7, now=NOW)

    assert len(report.rows) == 3
    assert report.totals["call_count"] == 4
    assert report.totals["rows_scanned"] == 4

    # Check grouping
    claude_row = next(r for r in report.rows if r.model == "claude-sonnet-4-6")
    assert claude_row.provider == "anthropic"
    assert claude_row.stage == "synthesis"
    assert claude_row.call_count == 2
    assert claude_row.percentage_of_calls == 50.0

    gpt_row = next(r for r in report.rows if r.model == "gpt-4")
    assert gpt_row.provider == "openai"
    assert gpt_row.stage == "synthesis"
    assert gpt_row.call_count == 1
    assert gpt_row.percentage_of_calls == 25.0

    gemini_row = next(r for r in report.rows if r.model == "gemini-pro")
    assert gemini_row.provider == "google"
    assert gemini_row.stage == "engagement"
    assert gemini_row.call_count == 1
    assert gemini_row.percentage_of_calls == 25.0


def test_aggregates_tokens(db):
    _usage(
        db,
        model_name="claude-sonnet-4-6",
        operation_name="synthesis.generate",
        input_tokens=100,
        output_tokens=50,
        total_tokens=150,
        days_ago=1,
    )
    _usage(
        db,
        model_name="claude-sonnet-4-6",
        operation_name="synthesis.generate",
        input_tokens=200,
        output_tokens=100,
        total_tokens=300,
        days_ago=1,
    )

    report = build_model_usage_provider_mix_report(db, days=7, now=NOW)

    assert len(report.rows) == 1
    row = report.rows[0]
    assert row.call_count == 2
    assert row.input_tokens == 300
    assert row.output_tokens == 150
    assert row.total_tokens == 450


def test_handles_zero_token_values(db):
    _usage(
        db,
        model_name="claude-sonnet-4-6",
        operation_name="synthesis.generate",
        input_tokens=0,
        output_tokens=0,
        total_tokens=0,
        days_ago=1,
    )

    report = build_model_usage_provider_mix_report(db, days=7, now=NOW)

    assert len(report.rows) == 1
    row = report.rows[0]
    assert row.call_count == 1
    assert row.input_tokens == 0
    assert row.output_tokens == 0
    assert row.total_tokens == 0


def test_stage_grouping_from_operation_name(db):
    _usage(db, operation_name="synthesis.generate_x_post", days_ago=1)
    _usage(db, operation_name="synthesis.evaluate", days_ago=1)
    _usage(db, operation_name="evaluation.score", days_ago=1)
    _usage(db, operation_name="engagement.reply_draft", days_ago=1)
    _usage(db, operation_name="refinement.improve", days_ago=1)

    report = build_model_usage_provider_mix_report(db, days=7, now=NOW)

    stages = {row.stage for row in report.rows}
    assert stages == {"synthesis", "evaluation", "engagement", "refinement"}


def test_date_filtering(db):
    _usage(db, model_name="claude-sonnet-4-6", days_ago=1)
    _usage(db, model_name="claude-sonnet-4-6", days_ago=5)
    _usage(db, model_name="claude-sonnet-4-6", days_ago=10)

    report = build_model_usage_provider_mix_report(db, days=7, now=NOW)

    assert report.totals["rows_scanned"] == 2
    assert report.totals["call_count"] == 2


def test_sorts_by_call_count_descending(db):
    _usage(db, model_name="claude-sonnet-4-6", operation_name="synthesis.generate", days_ago=1)
    _usage(db, model_name="gpt-4", operation_name="synthesis.evaluate", days_ago=1)
    _usage(db, model_name="gpt-4", operation_name="synthesis.evaluate", days_ago=1)
    _usage(db, model_name="gpt-4", operation_name="synthesis.evaluate", days_ago=1)

    report = build_model_usage_provider_mix_report(db, days=7, now=NOW)

    assert len(report.rows) == 2
    assert report.rows[0].model == "gpt-4"
    assert report.rows[0].call_count == 3
    assert report.rows[1].model == "claude-sonnet-4-6"
    assert report.rows[1].call_count == 1


def test_provider_extraction(db):
    _usage(db, model_name="claude-sonnet-4-6", days_ago=1)
    _usage(db, model_name="gpt-4-turbo", days_ago=1)
    _usage(db, model_name="o1-preview", days_ago=1)
    _usage(db, model_name="gemini-pro", days_ago=1)
    _usage(db, model_name="llama-3", days_ago=1)
    _usage(db, model_name="mistral-large", days_ago=1)
    _usage(db, model_name="unknown-model", days_ago=1)

    report = build_model_usage_provider_mix_report(db, days=7, now=NOW)

    providers = {row.provider for row in report.rows}
    assert providers == {"anthropic", "openai", "google", "meta", "mistral", "unknown"}


def test_handles_missing_model_usage_table(db):
    # Drop the table to simulate missing table
    db.conn.execute("DROP TABLE IF EXISTS model_usage")
    db.conn.commit()

    report = build_model_usage_provider_mix_report(db, days=7, now=NOW)

    assert report.missing_tables == ("model_usage",)
    assert len(report.rows) == 0
    assert report.totals["call_count"] == 0
    assert report.totals["rows_scanned"] == 0


def test_json_format_stable(db):
    _usage(db, model_name="claude-sonnet-4-6", operation_name="synthesis.generate", days_ago=1)

    report = build_model_usage_provider_mix_report(db, days=7, now=NOW)
    output = format_model_usage_provider_mix_json(report)

    payload = json.loads(output)
    assert payload["artifact_type"] == "model_usage_provider_mix"
    assert "filters" in payload
    assert "totals" in payload
    assert "rows" in payload
    assert len(payload["rows"]) == 1
    assert payload["rows"][0]["provider"] == "anthropic"
    assert payload["rows"][0]["model"] == "claude-sonnet-4-6"
    assert payload["rows"][0]["stage"] == "synthesis"
    assert payload["rows"][0]["call_count"] == 1


def test_text_format_readable(db):
    _usage(db, model_name="claude-sonnet-4-6", operation_name="synthesis.generate", days_ago=1)

    report = build_model_usage_provider_mix_report(db, days=7, now=NOW)
    output = format_model_usage_provider_mix_text(report)

    assert "Model Usage Provider Mix" in output
    assert "provider=anthropic" in output
    assert "model=claude-sonnet-4-6" in output
    assert "stage=synthesis" in output
    assert "calls=1" in output


def test_cli_json_output(db, monkeypatch):
    _usage(db, model_name="claude-sonnet-4-6", operation_name="synthesis.generate", days_ago=1)

    monkeypatch.setattr(
        model_usage_provider_mix_script,
        "script_context",
        lambda: _script_context(db),
    )

    exit_code = model_usage_provider_mix_script.main(["--format", "json"])
    assert exit_code == 0


def test_cli_text_output(db, monkeypatch):
    _usage(db, model_name="claude-sonnet-4-6", operation_name="synthesis.generate", days_ago=1)

    monkeypatch.setattr(
        model_usage_provider_mix_script,
        "script_context",
        lambda: _script_context(db),
    )

    exit_code = model_usage_provider_mix_script.main(["--format", "text"])
    assert exit_code == 0


def test_cli_days_filter(db, monkeypatch):
    _usage(db, model_name="claude-sonnet-4-6", days_ago=1)

    monkeypatch.setattr(
        model_usage_provider_mix_script,
        "script_context",
        lambda: _script_context(db),
    )

    exit_code = model_usage_provider_mix_script.main(["--days", "7"])
    assert exit_code == 0


def test_handles_unknown_operation_name(db):
    _usage(db, operation_name="unknown", days_ago=1)

    report = build_model_usage_provider_mix_report(db, days=7, now=NOW)

    assert len(report.rows) == 1
    assert report.rows[0].stage == "unknown"


def test_handles_blank_operation_name(db):
    db.conn.execute(
        """INSERT INTO model_usage
           (model_name, operation_name, input_tokens, output_tokens, total_tokens,
            estimated_cost, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        ("claude-sonnet-4-6", "  ", 100, 50, 150, 0.01, _ts(1)),
    )
    db.conn.commit()

    report = build_model_usage_provider_mix_report(db, days=7, now=NOW)

    assert len(report.rows) == 1
    assert report.rows[0].stage == "unknown"


def test_mixed_token_values(db):
    # Some rows have real tokens, some have zeros
    _usage(
        db,
        model_name="claude-sonnet-4-6",
        operation_name="synthesis.generate",
        input_tokens=100,
        output_tokens=50,
        total_tokens=150,
        days_ago=1,
    )
    _usage(
        db,
        model_name="claude-sonnet-4-6",
        operation_name="synthesis.generate",
        input_tokens=0,
        output_tokens=0,
        total_tokens=0,
        days_ago=1,
    )

    report = build_model_usage_provider_mix_report(db, days=7, now=NOW)

    assert len(report.rows) == 1
    row = report.rows[0]
    assert row.call_count == 2
    # Should aggregate all token values, including zeros
    assert row.input_tokens == 100
    assert row.output_tokens == 50
    assert row.total_tokens == 150


def test_percentage_calculation(db):
    _usage(db, model_name="claude-sonnet-4-6", operation_name="synthesis.generate", days_ago=1)
    _usage(db, model_name="claude-sonnet-4-6", operation_name="synthesis.generate", days_ago=1)
    _usage(db, model_name="gpt-4", operation_name="synthesis.evaluate", days_ago=1)
    _usage(db, model_name="gemini-pro", operation_name="engagement.reply", days_ago=1)

    report = build_model_usage_provider_mix_report(db, days=7, now=NOW)

    total_percentage = sum(row.percentage_of_calls for row in report.rows)
    assert abs(total_percentage - 100.0) < 0.01  # Allow small floating point error
