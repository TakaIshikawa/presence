from __future__ import annotations
import sqlite3
from evaluation.newsletter_ab_test_sample_imbalance import build_newsletter_ab_test_sample_imbalance_report, build_newsletter_ab_test_sample_imbalance_report_from_db

def test_flags_imbalance_and_missing_outcomes_separately():
    rows=[{"experiment_id":"e1","variant":"a","sample_size":100,"opens":10},{"experiment_id":"e1","variant":"b","sample_size":10,"opens":0}]
    r=build_newsletter_ab_test_sample_imbalance_report(rows,max_imbalance_ratio=2)
    assert r["artifact_type"]=="newsletter_ab_test_sample_imbalance"
    assert {"sample_imbalance","missing_outcome_metrics"} <= {f["issue_type"] for f in r["findings"]}
def test_db_reads_subject_candidates():
    c=sqlite3.connect(":memory:"); c.execute("CREATE TABLE newsletter_subject_candidates (experiment_id TEXT, variant TEXT, sample_size INTEGER, opens INTEGER)")
    c.executemany("INSERT INTO newsletter_subject_candidates VALUES (?,?,?,?)",[("e","a",50,1),("e","b",1,0)])
    assert build_newsletter_ab_test_sample_imbalance_report_from_db(c,max_imbalance_ratio=2)["summary"]["finding_count"]>=1
