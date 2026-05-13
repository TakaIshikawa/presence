from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
from pathlib import Path
import sqlite3

from evaluation.model_prompt_cost_regression import build_model_prompt_cost_regression_report


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "model_prompt_cost_regression.py"
spec = importlib.util.spec_from_file_location("model_prompt_cost_regression_script", SCRIPT)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE generated_content (id INTEGER PRIMARY KEY, content_type TEXT);
        CREATE TABLE model_usage (
            id INTEGER PRIMARY KEY, model_name TEXT, prompt_version TEXT, content_id INTEGER,
            estimated_cost REAL, total_tokens INTEGER, created_at TEXT
        );"""
    )
    return conn


def test_regression_detection():
    conn = _conn()
    conn.execute("INSERT INTO generated_content VALUES (1, 'blog_post')")
    conn.execute("INSERT INTO generated_content VALUES (2, 'blog_post')")
    conn.execute("INSERT INTO model_usage VALUES (1, 'gpt-a', 'v1', 1, 0.10, 100, '2026-04-20T00:00:00+00:00')")
    conn.execute("INSERT INTO model_usage VALUES (2, 'gpt-a', 'v1', 2, 0.20, 150, '2026-04-29T00:00:00+00:00')")

    report = build_model_prompt_cost_regression_report(conn, days=7, baseline_days=21, min_cost_increase_pct=50, now=NOW)

    regression = report.regressions[0]
    assert regression.model == "gpt-a"
    assert regression.content_type == "blog_post"
    assert regression.cost_increase_pct == 100.0
    assert regression.example_content_ids == (2,)


def test_no_regression_empty_state():
    conn = _conn()
    conn.execute("INSERT INTO model_usage VALUES (1, 'gpt-a', 'v1', NULL, 0.20, 100, '2026-04-20T00:00:00+00:00')")
    conn.execute("INSERT INTO model_usage VALUES (2, 'gpt-a', 'v1', NULL, 0.10, 100, '2026-04-29T00:00:00+00:00')")

    report = build_model_prompt_cost_regression_report(conn, now=NOW)

    assert report.regressions == ()
    assert report.empty_state["is_empty"] is True


def test_model_filtering():
    conn = _conn()
    conn.execute("INSERT INTO model_usage VALUES (1, 'gpt-a', 'v1', NULL, 0.10, 100, '2026-04-20T00:00:00+00:00')")
    conn.execute("INSERT INTO model_usage VALUES (2, 'gpt-a', 'v1', NULL, 0.30, 100, '2026-04-29T00:00:00+00:00')")
    conn.execute("INSERT INTO model_usage VALUES (3, 'gpt-b', 'v1', NULL, 0.10, 100, '2026-04-20T00:00:00+00:00')")
    conn.execute("INSERT INTO model_usage VALUES (4, 'gpt-b', 'v1', NULL, 0.50, 100, '2026-04-29T00:00:00+00:00')")

    report = build_model_prompt_cost_regression_report(conn, model="gpt-b", now=NOW)

    assert [item.model for item in report.regressions] == ["gpt-b"]


def test_invalid_percentage_handling(capsys):
    assert script.main(["--min-cost-increase-pct", "-1"]) == 2
    assert "non-negative" in capsys.readouterr().err
