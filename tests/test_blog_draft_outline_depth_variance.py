from __future__ import annotations
import sqlite3
from evaluation.blog_draft_outline_depth_variance import build_blog_draft_outline_depth_variance_report, build_blog_draft_outline_depth_variance_report_from_db

def test_flags_skipped_heading_and_over_deep():
    r=build_blog_draft_outline_depth_variance_report([{"draft_id":1,"body":"# A\n### C\n##### E"}],max_depth=4)
    assert r["artifact_type"]=="blog_draft_outline_depth_variance"
    assert {"skipped_heading_level","heading_deeper_than_max_depth"} <= {f["gap_reason"] for f in r["findings"]}
def test_db_missing_schema_metadata():
    c=sqlite3.connect(":memory:"); c.execute("CREATE TABLE blog_drafts (id INTEGER)")
    assert build_blog_draft_outline_depth_variance_report_from_db(c)["missing_columns"]=={"blog_drafts":["body|content|markdown"]}
