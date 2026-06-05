from __future__ import annotations
import json, sqlite3
from evaluation.pipeline_candidate_style_entropy import build_pipeline_candidate_style_entropy_report, build_pipeline_candidate_style_entropy_report_from_db, format_pipeline_candidate_style_entropy_json, format_pipeline_candidate_style_entropy_text
def test_row_builder_groups_and_scores_entropy():
    rows=[{"run_id":"r1","content_id":1,"format":"thread","body":"Try this now."},{"run_id":"r1","content_id":1,"format":"thread","body":"Try that next."},{"run_id":"r2","content_id":2,"format":"post","body":"Ask a question."},{"run_id":"r2","content_id":2,"format":"list","body":"Build a guide."}]
    r=build_pipeline_candidate_style_entropy_report(rows,min_entropy=.8,min_unique_formats=2)
    f=r["findings"][0]
    assert f["run_id"]=="r1" and f["repeated_hook_count"]==1 and "low_unique_formats" in f["risk_reasons"]
def test_db_and_formatters():
    c=sqlite3.connect(":memory:"); c.executescript("CREATE TABLE pipeline_candidates(id INTEGER,run_id TEXT,content_id INTEGER,format TEXT,body TEXT); INSERT INTO pipeline_candidates VALUES(1,'r',1,'post','Try x'),(2,'r',1,'post','Try y');")
    r=build_pipeline_candidate_style_entropy_report_from_db(c,min_unique_formats=2)
    assert r["findings"][0]["candidate_count"]==2
    assert json.loads(format_pipeline_candidate_style_entropy_json(r))["artifact_type"]=="pipeline_candidate_style_entropy"
    assert "entropy" in format_pipeline_candidate_style_entropy_text(r)
