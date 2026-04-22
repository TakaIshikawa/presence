"""Tests for model usage accounting and reporting."""

import json
import sys
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from model_usage import (
    anthropic_usage_tokens,
    estimate_anthropic_cost,
    record_anthropic_usage,
)
from model_usage_report import format_json_report, format_text_report, main


def _response_with_usage(input_tokens=123, output_tokens=45):
    response = MagicMock()
    response.content = [MagicMock(text="ok")]
    response.usage = MagicMock(
        input_tokens=input_tokens,
        output_tokens=output_tokens,
    )
    return response


def test_anthropic_usage_tokens_parses_attribute_usage():
    assert anthropic_usage_tokens(_response_with_usage(123, 45)) == (123, 45)


def test_anthropic_usage_tokens_parses_dict_usage():
    response = MagicMock()
    response.usage = {"input_tokens": "50", "output_tokens": 12}

    assert anthropic_usage_tokens(response) == (50, 12)


def test_anthropic_usage_tokens_tolerates_missing_usage_on_mock_response():
    response = MagicMock()
    response.content = [MagicMock(text="ok")]

    assert anthropic_usage_tokens(response) is None


def test_record_anthropic_usage_persists_when_usage_present(db):
    usage_id = record_anthropic_usage(
        db,
        _response_with_usage(100, 25),
        model_name="claude-sonnet-4-6",
        operation_name="synthesis.generate_x_post",
    )

    row = db.conn.execute(
        "SELECT * FROM model_usage WHERE id = ?", (usage_id,)
    ).fetchone()
    assert row["input_tokens"] == 100
    assert row["output_tokens"] == 25
    assert row["total_tokens"] == 125
    assert row["estimated_cost"] == estimate_anthropic_cost(
        "claude-sonnet-4-6", 100, 25
    )


def test_record_anthropic_usage_skips_missing_usage(db):
    response = MagicMock()
    response.content = [MagicMock(text="ok")]

    assert record_anthropic_usage(
        db,
        response,
        model_name="claude-sonnet-4-6",
        operation_name="synthesis.generate_x_post",
    ) is None
    assert db.conn.execute("SELECT COUNT(*) FROM model_usage").fetchone()[0] == 0


def _rows():
    return [
        {
            "day": "2026-04-23",
            "operation_name": "synthesis.generate_candidates.x_post",
            "model_name": "claude-sonnet-4-6",
            "call_count": 2,
            "input_tokens": 150,
            "output_tokens": 30,
            "total_tokens": 180,
            "estimated_cost": 0.0009,
        },
        {
            "day": "2026-04-23",
            "operation_name": "synthesis.evaluate_candidates",
            "model_name": "claude-opus-4-7",
            "call_count": 1,
            "input_tokens": 200,
            "output_tokens": 50,
            "total_tokens": 250,
            "estimated_cost": 0.00675,
        },
    ]


def test_format_text_report_includes_totals_and_group_rows():
    output = format_text_report(_rows(), days=7)

    assert "Model Usage Report (last 7 days)" in output
    assert "Total calls:  3" in output
    assert "Total tokens: 430" in output
    assert "synthesis.evaluate_candidates" in output
    assert "claude-opus-4-7" in output


def test_format_text_report_empty():
    assert format_text_report([], days=7) == "No model usage found in last 7 days."


def test_format_json_report_includes_totals():
    payload = json.loads(format_json_report(_rows(), days=7))

    assert payload["days"] == 7
    assert payload["totals"]["call_count"] == 3
    assert payload["totals"]["total_tokens"] == 430
    assert payload["rows"][0]["operation_name"] == "synthesis.generate_candidates.x_post"


def test_main_prints_json_report(db, capsys):
    db.record_model_usage(
        "claude-sonnet-4-6",
        "synthesis.generate_x_post",
        100,
        25,
        estimated_cost=0.000675,
    )

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("model_usage_report.script_context", fake_script_context), patch(
        "sys.argv", ["model_usage_report.py", "--days", "1", "--format", "json"]
    ):
        main()

    payload = json.loads(capsys.readouterr().out)
    assert payload["totals"]["call_count"] == 1
    assert payload["rows"][0]["operation_name"] == "synthesis.generate_x_post"
