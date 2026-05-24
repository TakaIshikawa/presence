from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
import pytest
from evaluation.newsletter_archive_metric_backfill_candidates import build_newsletter_archive_metric_backfill_candidates_report, build_newsletter_archive_metric_backfill_candidates_report_from_db, format_newsletter_archive_metric_backfill_candidates_json, format_newsletter_archive_metric_backfill_candidates_text
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"newsletter_archive_metric_backfill_candidates.py"
spec=importlib.util.spec_from_file_location("newsletter_archive_metric_backfill_candidates_script", SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def test_newsletter_archive_metric_backfill_candidates_builder_render_and_missing_db():
    report=build_newsletter_archive_metric_backfill_candidates_report([])
    assert report["artifact_type"] == "newsletter_archive_metric_backfill_candidates"
    assert json.loads(format_newsletter_archive_metric_backfill_candidates_json(report))["artifact_type"] == "newsletter_archive_metric_backfill_candidates"
    assert "Newsletter Archive Metric Backfill Candidates" in format_newsletter_archive_metric_backfill_candidates_text(report)
    missing=build_newsletter_archive_metric_backfill_candidates_report_from_db(sqlite3.connect(":memory:"))
    assert "missing_tables" in missing

def test_newsletter_archive_metric_backfill_candidates_cli_validation(tmp_path, capsys):
    assert script.main(["--db", str(tmp_path/"empty.sqlite"), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "newsletter_archive_metric_backfill_candidates"
    with pytest.raises(SystemExit): script.parse_args(["--limit", "0"])
