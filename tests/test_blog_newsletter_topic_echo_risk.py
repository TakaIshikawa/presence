from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
import pytest
from evaluation.blog_newsletter_topic_echo_risk import build_blog_newsletter_topic_echo_risk_report, build_blog_newsletter_topic_echo_risk_report_from_db, format_blog_newsletter_topic_echo_risk_json, format_blog_newsletter_topic_echo_risk_text
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"blog_newsletter_topic_echo_risk.py"
spec=importlib.util.spec_from_file_location("blog_newsletter_topic_echo_risk_script", SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def test_blog_newsletter_topic_echo_risk_builder_render_and_missing_db():
    report=build_blog_newsletter_topic_echo_risk_report([])
    assert report["artifact_type"] == "blog_newsletter_topic_echo_risk"
    assert json.loads(format_blog_newsletter_topic_echo_risk_json(report))["artifact_type"] == "blog_newsletter_topic_echo_risk"
    assert "Blog Newsletter Topic Echo Risk" in format_blog_newsletter_topic_echo_risk_text(report)
    missing=build_blog_newsletter_topic_echo_risk_report_from_db(sqlite3.connect(":memory:"))
    assert "missing_tables" in missing

def test_blog_newsletter_topic_echo_risk_cli_validation(tmp_path, capsys):
    assert script.main(["--db", str(tmp_path/"empty.sqlite"), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "blog_newsletter_topic_echo_risk"
    with pytest.raises(SystemExit): script.parse_args(["--limit", "0"])
