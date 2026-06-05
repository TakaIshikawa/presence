from __future__ import annotations
import json, sqlite3
from evaluation.blog_draft_heading_structure_gaps import build_blog_draft_heading_structure_gaps_report, build_blog_draft_heading_structure_gaps_report_from_db, extract_headings, format_blog_draft_heading_structure_gaps_json, format_blog_draft_heading_structure_gaps_text
def test_markdown_and_html_heading_extraction():
    assert [(h["level"],h["text"]) for h in extract_headings("# A\n<h2>B</h2>\n### C")]==[(1,"A"),(2,"B"),(3,"C")]
def test_flags_invalid_structures_and_accepts_valid():
    r=build_blog_draft_heading_structure_gaps_report([{"id":1,"body":"## No h1"},{"id":2,"body":"# A\n# B"},{"id":3,"body":"# A\n### Skip"},{"id":4,"body":"# "+"x"*9}],max_heading_length=5)
    reasons=[f["reason"] for f in r["findings"]]
    assert {"missing_h1","multiple_h1","skipped_heading_level","long_heading"} <= set(reasons)
    assert build_blog_draft_heading_structure_gaps_report([{"id":5,"body":"# A\n## B\n### C"}])["findings"]==[]
def test_db_and_formatters():
    c=sqlite3.connect(":memory:"); c.executescript("CREATE TABLE blog_drafts(id INTEGER,title TEXT,body TEXT); INSERT INTO blog_drafts VALUES(1,'T','## Bad');")
    r=build_blog_draft_heading_structure_gaps_report_from_db(c)
    assert r["findings"][0]["title"]=="T"
    assert json.loads(format_blog_draft_heading_structure_gaps_json(r))["artifact_type"]=="blog_draft_heading_structure_gaps"
    assert "missing_h1" in format_blog_draft_heading_structure_gaps_text(r)
