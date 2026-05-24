from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
import pytest
from evaluation.content_claim_evidence_domain_concentration import build_content_claim_evidence_domain_concentration_report, build_content_claim_evidence_domain_concentration_report_from_db, format_content_claim_evidence_domain_concentration_json, format_content_claim_evidence_domain_concentration_text
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"content_claim_evidence_domain_concentration.py"
spec=importlib.util.spec_from_file_location("content_claim_evidence_domain_concentration_script", SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def test_content_claim_evidence_domain_concentration_builder_render_and_missing_db():
    report=build_content_claim_evidence_domain_concentration_report([])
    assert report["artifact_type"] == "content_claim_evidence_domain_concentration"
    assert json.loads(format_content_claim_evidence_domain_concentration_json(report))["artifact_type"] == "content_claim_evidence_domain_concentration"
    assert "Content Claim Evidence Domain Concentration" in format_content_claim_evidence_domain_concentration_text(report)
    missing=build_content_claim_evidence_domain_concentration_report_from_db(sqlite3.connect(":memory:"))
    assert "missing_tables" in missing

def test_content_claim_evidence_domain_concentration_cli_validation(tmp_path, capsys):
    assert script.main(["--db", str(tmp_path/"empty.sqlite"), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "content_claim_evidence_domain_concentration"
    with pytest.raises(SystemExit): script.parse_args(["--limit", "0"])
