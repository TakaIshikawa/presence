from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
import pytest
from evaluation.publication_attempt_retry_reason_drift import build_publication_attempt_retry_reason_drift_report, build_publication_attempt_retry_reason_drift_report_from_db, format_publication_attempt_retry_reason_drift_json, format_publication_attempt_retry_reason_drift_text
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"publication_attempt_retry_reason_drift.py"
spec=importlib.util.spec_from_file_location("publication_attempt_retry_reason_drift_script", SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def test_publication_attempt_retry_reason_drift_builder_render_and_missing_db():
    report=build_publication_attempt_retry_reason_drift_report([])
    assert report["artifact_type"] == "publication_attempt_retry_reason_drift"
    assert json.loads(format_publication_attempt_retry_reason_drift_json(report))["artifact_type"] == "publication_attempt_retry_reason_drift"
    assert "Publication Attempt Retry Reason Drift" in format_publication_attempt_retry_reason_drift_text(report)
    missing=build_publication_attempt_retry_reason_drift_report_from_db(sqlite3.connect(":memory:"))
    assert "missing_tables" in missing

def test_publication_attempt_retry_reason_drift_cli_validation(tmp_path, capsys):
    assert script.main(["--db", str(tmp_path/"empty.sqlite"), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "publication_attempt_retry_reason_drift"
    with pytest.raises(SystemExit): script.parse_args(["--limit", "0"])
