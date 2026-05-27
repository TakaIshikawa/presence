from __future__ import annotations
import importlib.util,json,sqlite3
from pathlib import Path
from ingestion.blog_rss_backlink_mention_import import *
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_blog_rss_backlink_mentions.py"; spec=importlib.util.spec_from_file_location("s",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_backlink_import_normalizes_and_cli(tmp_path,capsys):
 rows=parse_blog_rss_backlink_mention_payload('[{"source_url":"HTTPS://WWW.Ex.COM/a","target_url":"HTTPS://Mine.COM/p","anchor_text":" me "}]'); assert rows[0]["source_url"]=="https://www.ex.com/a" and rows[0]["source_domain"]=="ex.com"
 c=sqlite3.connect(":memory:"); assert import_blog_rss_backlink_mentions(c,rows)["summary"]["inserted_count"]==1; assert import_blog_rss_backlink_mentions(c,rows,dry_run=True)["summary"]["updated_count"]==1
 p=tmp_path/"in.json"; p.write_text(json.dumps(rows)); db=tmp_path/"d.sqlite"; assert script.main([str(p),"--db",str(db),"--format","json"])==0; assert json.loads(capsys.readouterr().out)["artifact_type"]=="blog_rss_backlink_mention_import"
