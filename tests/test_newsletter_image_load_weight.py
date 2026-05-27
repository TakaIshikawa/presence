from __future__ import annotations
import json, sqlite3
from evaluation.newsletter_image_load_weight import build_newsletter_image_load_weight_report, build_newsletter_image_load_weight_report_from_db

def test_parses_html_and_metadata_images_without_network():
    rows=[{"issue_id":1,"html":"<img src='a.png'><img src='b.png' width='10' height='10' data-size-kb='900'>","metadata":json.dumps({"images":[{"url":"c.png","width":1,"height":1,"estimated_kb":10}]})}]
    r=build_newsletter_image_load_weight_report(rows,max_images=2,max_total_kb=800)
    assert r["artifact_type"]=="newsletter_image_load_weight"
    assert {f["issue_type"] for f in r["findings"]}>={"too_many_images","missing_dimensions","oversized_image"}
def test_db_supports_newsletter_drafts():
    c=sqlite3.connect(":memory:"); c.execute("CREATE TABLE newsletter_drafts (id INTEGER, html TEXT, metadata TEXT)"); c.execute("INSERT INTO newsletter_drafts VALUES (1,'<img src=x>',NULL)")
    assert build_newsletter_image_load_weight_report_from_db(c)["findings"][0]["issue_id"]==1
