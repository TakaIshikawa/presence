from __future__ import annotations
import sqlite3
from evaluation.newsletter_spam_trigger_density import build_newsletter_spam_trigger_density_report, build_newsletter_spam_trigger_density_report_from_db

def test_case_insensitive_trigger_density_and_punctuation():
    r=build_newsletter_spam_trigger_density_report([{"issue_id":1,"subject":"FREE MONEY!!! ACT NOW!!!","preheader":"","body":""}],max_trigger_density=0.01,max_exclamation_count=2)
    assert r["artifact_type"]=="newsletter_spam_trigger_density"
    assert {"free money","act now","exclamation_abuse"} <= {f["trigger"] for f in r["findings"]}
def test_db_reads_drafts():
    c=sqlite3.connect(":memory:"); c.execute("CREATE TABLE newsletter_drafts (id INTEGER, subject TEXT, preheader TEXT, body TEXT)"); c.execute("INSERT INTO newsletter_drafts VALUES (1,'Guaranteed winner','', '')")
    assert build_newsletter_spam_trigger_density_report_from_db(c,max_trigger_density=0.01)["findings"][0]["issue_id"]==1
