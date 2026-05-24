from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
import pytest
from evaluation.publish_queue_schedule_window_utilization import build_publish_queue_schedule_window_utilization_report, build_publish_queue_schedule_window_utilization_report_from_db, format_publish_queue_schedule_window_utilization_json, format_publish_queue_schedule_window_utilization_text
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"publish_queue_schedule_window_utilization.py"
spec=importlib.util.spec_from_file_location("publish_queue_schedule_window_utilization_script", SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def test_publish_queue_schedule_window_utilization_builder_render_and_missing_db():
    report=build_publish_queue_schedule_window_utilization_report([])
    assert report["artifact_type"] == "publish_queue_schedule_window_utilization"
    assert json.loads(format_publish_queue_schedule_window_utilization_json(report))["artifact_type"] == "publish_queue_schedule_window_utilization"
    assert "Publish Queue Schedule Window Utilization" in format_publish_queue_schedule_window_utilization_text(report)
    missing=build_publish_queue_schedule_window_utilization_report_from_db(sqlite3.connect(":memory:"))
    assert "missing_tables" in missing

def test_publish_queue_schedule_window_utilization_cli_validation(tmp_path, capsys):
    assert script.main(["--db", str(tmp_path/"empty.sqlite"), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "publish_queue_schedule_window_utilization"
    with pytest.raises(SystemExit): script.parse_args(["--limit", "0"])
