from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.pipeline_candidate_claim_specificity import build_pipeline_candidate_claim_specificity_report


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "pipeline_candidate_claim_specificity.py"
spec = importlib.util.spec_from_file_location("pipeline_candidate_claim_specificity_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_vague_candidates_sort_above_specific_examples_and_short_posts_work(tmp_path, capsys):
    report = build_pipeline_candidate_claim_specificity_report(
        [
            {"pipeline_run_id": "r1", "candidate_id": "vague", "format": "post", "text": "Most teams always see massive gains."},
            {"pipeline_run_id": "r1", "candidate_id": "short", "format": "post", "text": "Huge."},
            {"pipeline_run_id": "r1", "candidate_id": "specific", "format": "thread", "text": "The newsletter gained 42 subscribers in 7 days after the API release."},
        ]
    )
    assert [row["candidate_id"] for row in report["rows"]] == ["vague", "short"]
    assert {key for row in report["rows"] for key in row} >= {"pipeline_run_id", "candidate_id", "format", "vague_markers", "concrete_markers", "specificity_score", "recommendation"}

    db_path = tmp_path / "candidates.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE pipeline_candidates (pipeline_run_id TEXT, id TEXT, format TEXT, text TEXT)")
    conn.execute("INSERT INTO pipeline_candidates VALUES ('r2','c1','thread','Many customers love the best product')")
    conn.commit()
    assert script.main(["--db", str(db_path), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["rows"][0]["candidate_id"] == "c1"


def test_validation_errors():
    with pytest.raises(ValueError, match="min_score"):
        build_pipeline_candidate_claim_specificity_report([], min_score=101)
