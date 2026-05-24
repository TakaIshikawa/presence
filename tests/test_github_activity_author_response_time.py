from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
import pytest
from evaluation.github_activity_author_response_time import build_github_activity_author_response_time_report, build_github_activity_author_response_time_report_from_db, format_github_activity_author_response_time_json, format_github_activity_author_response_time_text
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"github_activity_author_response_time.py"
spec=importlib.util.spec_from_file_location("github_activity_author_response_time_script", SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def test_github_activity_author_response_time_builder_render_and_missing_db():
    report=build_github_activity_author_response_time_report([])
    assert report["artifact_type"] == "github_activity_author_response_time"
    assert json.loads(format_github_activity_author_response_time_json(report))["artifact_type"] == "github_activity_author_response_time"
    assert "GitHub Activity Author Response Time" in format_github_activity_author_response_time_text(report)
    missing=build_github_activity_author_response_time_report_from_db(sqlite3.connect(":memory:"))
    assert "missing_tables" in missing

def test_github_activity_author_response_time_cli_validation(tmp_path, capsys):
    assert script.main(["--db", str(tmp_path/"empty.sqlite"), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "github_activity_author_response_time"
    with pytest.raises(SystemExit): script.parse_args(["--limit", "0"])
