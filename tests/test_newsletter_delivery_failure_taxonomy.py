from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
import pytest
from evaluation.newsletter_delivery_failure_taxonomy import build_newsletter_delivery_failure_taxonomy_report, build_newsletter_delivery_failure_taxonomy_report_from_db, format_newsletter_delivery_failure_taxonomy_json, format_newsletter_delivery_failure_taxonomy_text
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"newsletter_delivery_failure_taxonomy.py"
spec=importlib.util.spec_from_file_location("newsletter_delivery_failure_taxonomy_script", SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def test_newsletter_delivery_failure_taxonomy_builder_render_and_missing_db():
    report=build_newsletter_delivery_failure_taxonomy_report([])
    assert report["artifact_type"] == "newsletter_delivery_failure_taxonomy"
    assert json.loads(format_newsletter_delivery_failure_taxonomy_json(report))["artifact_type"] == "newsletter_delivery_failure_taxonomy"
    assert "Newsletter Delivery Failure Taxonomy" in format_newsletter_delivery_failure_taxonomy_text(report)
    missing=build_newsletter_delivery_failure_taxonomy_report_from_db(sqlite3.connect(":memory:"))
    assert "missing_tables" in missing

def test_newsletter_delivery_failure_taxonomy_cli_validation(tmp_path, capsys):
    assert script.main(["--db", str(tmp_path/"empty.sqlite"), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "newsletter_delivery_failure_taxonomy"
    with pytest.raises(SystemExit): script.parse_args(["--limit", "0"])
