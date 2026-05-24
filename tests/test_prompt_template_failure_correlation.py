from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.prompt_template_failure_correlation import build_prompt_template_failure_correlation_report_from_db, format_prompt_template_failure_correlation_json, format_prompt_template_failure_correlation_text

SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "prompt_template_failure_correlation.py"
spec = importlib.util.spec_from_file_location("prompt_template_failure_correlation_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE pipeline_runs (id INTEGER PRIMARY KEY, prompt_template TEXT, prompt_version TEXT, status TEXT, evaluation_score REAL)")
    return conn


def test_prompt_failure_correlation_and_cli(tmp_path, capsys):
    conn = _conn()
    rows = [(1, "reply", "v1", "failed", 0.2), (2, "reply", "v1", "rejected", 0.3), (3, "reply", "v1", "rejected", 0.4), (4, None, None, "success", 0.9)]
    conn.executemany("INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?)", rows)
    report = build_prompt_template_failure_correlation_report_from_db(conn, min_attempts=2)
    kinds = {f["finding_type"] for f in report["findings"]}
    assert {"high_failure_rate_template", "rejection_cluster", "low_score_cluster", "missing_prompt_version_link"} <= kinds
    assert json.loads(format_prompt_template_failure_correlation_json(report))["artifact_type"] == "prompt_template_failure_correlation"
    assert "Prompt Template Failure Correlation" in format_prompt_template_failure_correlation_text(report)
    conn.commit()
    dest = sqlite3.connect(tmp_path / "prompt.sqlite")
    conn.backup(dest)
    dest.close()
    assert script.main(["--db", str(tmp_path / "prompt.sqlite"), "--min-attempts", "2"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["finding_count"] >= 1


def test_schema_empty_and_validation():
    assert build_prompt_template_failure_correlation_report_from_db(sqlite3.connect(":memory:"))["missing_tables"]
    assert build_prompt_template_failure_correlation_report_from_db(_conn())["empty_state"]["is_empty"]
    with pytest.raises(SystemExit):
        script.parse_args(["--min-attempts", "0"])
