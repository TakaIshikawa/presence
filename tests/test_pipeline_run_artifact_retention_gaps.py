from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
import pytest
from evaluation.pipeline_run_artifact_retention_gaps import build_pipeline_run_artifact_retention_gaps_report, build_pipeline_run_artifact_retention_gaps_report_from_db, format_pipeline_run_artifact_retention_gaps_json, format_pipeline_run_artifact_retention_gaps_text
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"pipeline_run_artifact_retention_gaps.py"
spec=importlib.util.spec_from_file_location("pipeline_run_artifact_retention_gaps_script", SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def test_pipeline_run_artifact_retention_gaps_builder_render_and_missing_db():
    report=build_pipeline_run_artifact_retention_gaps_report([])
    assert report["artifact_type"] == "pipeline_run_artifact_retention_gaps"
    assert json.loads(format_pipeline_run_artifact_retention_gaps_json(report))["artifact_type"] == "pipeline_run_artifact_retention_gaps"
    assert "Pipeline Run Artifact Retention Gaps" in format_pipeline_run_artifact_retention_gaps_text(report)
    missing=build_pipeline_run_artifact_retention_gaps_report_from_db(sqlite3.connect(":memory:"))
    assert "missing_tables" in missing

def test_pipeline_run_artifact_retention_gaps_cli_validation(tmp_path, capsys):
    assert script.main(["--db", str(tmp_path/"empty.sqlite"), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "pipeline_run_artifact_retention_gaps"
    with pytest.raises(SystemExit): script.parse_args(["--limit", "0"])
