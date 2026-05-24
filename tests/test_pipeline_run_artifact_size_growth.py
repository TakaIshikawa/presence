from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.pipeline_run_artifact_size_growth import (
    build_pipeline_run_artifact_size_growth_report_from_db,
    format_pipeline_run_artifact_size_growth_json,
    format_pipeline_run_artifact_size_growth_text,
)

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "pipeline_run_artifact_size_growth.py"
spec = importlib.util.spec_from_file_location("pipeline_run_artifact_size_growth_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE pipeline_run_artifacts (id INTEGER PRIMARY KEY, pipeline_run_id TEXT, stage TEXT, size_bytes INTEGER, payload TEXT, created_at TEXT)")
    return conn


def test_artifact_size_findings_and_cli(tmp_path, capsys):
    conn = _conn()
    rows = [
        (1, "r1", "draft", 100, None, NOW - timedelta(days=3)),
        (2, "r2", "draft", 400, None, NOW - timedelta(days=1)),
        (3, "r3", "publish", 800, None, NOW - timedelta(days=1)),
        (4, "r4", "draft", None, None, NOW - timedelta(days=1)),
        (5, "r5", "publish", None, "x" * 20, NOW - timedelta(days=1)),
    ]
    for row in rows:
        conn.execute("INSERT INTO pipeline_run_artifacts VALUES (?, ?, ?, ?, ?, ?)", (row[0], row[1], row[2], row[3], row[4], row[5].isoformat()))
    report = build_pipeline_run_artifact_size_growth_report_from_db(conn, max_bytes=300, growth_ratio=2, now=NOW)
    kinds = {f["finding_type"] for f in report["findings"]}
    assert {"oversized_artifact", "rapid_size_growth", "missing_size_signal"} <= kinds
    assert json.loads(format_pipeline_run_artifact_size_growth_json(report))["artifact_type"] == "pipeline_run_artifact_size_growth"
    assert "Pipeline Run Artifact Size Growth" in format_pipeline_run_artifact_size_growth_text(report)
    conn.commit()
    dest = sqlite3.connect(tmp_path / "artifacts.sqlite")
    conn.backup(dest)
    dest.close()
    assert script.main(["--db", str(tmp_path / "artifacts.sqlite"), "--max-bytes", "300", "--growth-ratio", "2"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["finding_count"] >= 1


def test_schema_empty_and_validation():
    assert build_pipeline_run_artifact_size_growth_report_from_db(sqlite3.connect(":memory:"), now=NOW)["missing_tables"]
    empty = build_pipeline_run_artifact_size_growth_report_from_db(_conn(), now=NOW)
    assert empty["empty_state"]["is_empty"]
    with pytest.raises(SystemExit):
        script.parse_args(["--growth-ratio", "1"])
