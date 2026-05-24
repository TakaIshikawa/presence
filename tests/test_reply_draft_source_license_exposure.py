from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
import pytest
from evaluation.reply_draft_source_license_exposure import build_reply_draft_source_license_exposure_report, build_reply_draft_source_license_exposure_report_from_db, format_reply_draft_source_license_exposure_json, format_reply_draft_source_license_exposure_text
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"reply_draft_source_license_exposure.py"
spec=importlib.util.spec_from_file_location("reply_draft_source_license_exposure_script", SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def test_reply_draft_source_license_exposure_builder_render_and_missing_db():
    report=build_reply_draft_source_license_exposure_report([])
    assert report["artifact_type"] == "reply_draft_source_license_exposure"
    assert json.loads(format_reply_draft_source_license_exposure_json(report))["artifact_type"] == "reply_draft_source_license_exposure"
    assert "Reply Draft Source License Exposure" in format_reply_draft_source_license_exposure_text(report)
    missing=build_reply_draft_source_license_exposure_report_from_db(sqlite3.connect(":memory:"))
    assert "missing_tables" in missing

def test_reply_draft_source_license_exposure_cli_validation(tmp_path, capsys):
    assert script.main(["--db", str(tmp_path/"empty.sqlite"), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "reply_draft_source_license_exposure"
    with pytest.raises(SystemExit): script.parse_args(["--limit", "0"])
