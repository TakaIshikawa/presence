from __future__ import annotations
import json, sqlite3
from evaluation.blog_draft_code_block_language_gaps import build_blog_draft_code_block_language_gaps_report, build_blog_draft_code_block_language_gaps_report_from_db

def test_detects_unlabeled_unknown_and_metadata_mismatch():
    body="```\nno label\n```\n```wat\nx\n```\n```python\nx\n```"
    r=build_blog_draft_code_block_language_gaps_report([{"draft_id":1,"body":body,"metadata":json.dumps({"language":"javascript"})}],allowed_languages="python,javascript")
    assert r["artifact_type"]=="blog_draft_code_block_language_gaps"
    assert {"missing_language","unknown_language","metadata_language_mismatch"} <= {f["gap_reason"] for f in r["findings"]}
def test_db_reads_blog_drafts():
    c=sqlite3.connect(":memory:"); c.execute("CREATE TABLE blog_drafts (id INTEGER, markdown TEXT, metadata TEXT)"); c.execute("INSERT INTO blog_drafts VALUES (1,?,NULL)", ("```\nx\n```",))
    assert build_blog_draft_code_block_language_gaps_report_from_db(c)["findings"][0]["block_index"]==1
