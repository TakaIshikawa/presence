from __future__ import annotations
import importlib.util, sqlite3
from pathlib import Path
from ingestion.linkedin_newsletter_article_metric_import import parse_linkedin_newsletter_article_metrics, upsert_linkedin_newsletter_article_metrics
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_linkedin_newsletter_article_metrics.py"; spec=importlib.util.spec_from_file_location("import_linkedin_newsletter_article_metrics_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_linkedin_newsletter_article_metric_import_cli(tmp_path):
    rows=parse_linkedin_newsletter_article_metrics('{"articles":[{"newsletter_id":"n","article_id":"a1","views":"10","reactions_count":"2","comments_count":"3","shares_count":"4","subscribers":"5","captured_at":"t"}]}')
    assert rows[0]["impressions"]==10 and rows[0]["reactions"]==2
    assert parse_linkedin_newsletter_article_metrics("article_id,captured_at,impressions\na2,t,9\n")[0]["impressions"]==9
    c=sqlite3.connect(":memory:"); upsert_linkedin_newsletter_article_metrics(c,rows); upsert_linkedin_newsletter_article_metrics(c,[{**rows[0],"clicks":7}]); assert c.execute("SELECT clicks FROM linkedin_newsletter_article_metrics").fetchone()[0]==7
    p=tmp_path/"l.jsonl"; p.write_text('{"article_id":"a3","captured_at":"t"}\n'); db=tmp_path/"db.sqlite"; assert script.main(["--db",str(db),"--input",str(p),"--dry-run"])==0
