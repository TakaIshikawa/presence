from __future__ import annotations
import json, sqlite3
from evaluation.knowledge_source_author_diversity import build_knowledge_source_author_diversity_report, build_knowledge_source_author_diversity_report_from_db

def test_summary_and_findings_for_dominant_and_missing_author():
    rows=[{"id":1,"author":"A"},{"id":2,"author":"A"},{"id":3,"author":""}]
    r=build_knowledge_source_author_diversity_report(rows,max_author_share=0.5)
    assert r["artifact_type"]=="knowledge_source_author_diversity"
    assert r["summary"]["author_count"]==1 and r["summary"]["missing_author_count"]==1
    assert {"dominant_author_concentration","missing_author_attribution"} <= {f["issue_type"] for f in r["findings"]}
def test_db_reads_metadata_author():
    c=sqlite3.connect(":memory:"); c.execute("CREATE TABLE curated_sources (id INTEGER, kind TEXT, metadata TEXT)")
    c.execute("INSERT INTO curated_sources VALUES (1,'article',?)",(json.dumps({"author":"A"}),)); c.execute("INSERT INTO curated_sources VALUES (2,'article',?)",(json.dumps({"author":"A"}),))
    assert build_knowledge_source_author_diversity_report_from_db(c,max_author_share=0.5)["summary"]["top_author_share"]==1.0
