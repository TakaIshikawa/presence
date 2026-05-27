from __future__ import annotations
import importlib.util,json,sqlite3
from pathlib import Path
from ingestion.blog_search_console_query_import import *
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_blog_search_console_queries.py"; spec=importlib.util.spec_from_file_location("s",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_parser_upsert_dry_run_and_cli(tmp_path,capsys):
 rows=parse_blog_search_console_query_payload('date,page,query,clicks,impressions,ctr,position\n2026-05-26,HTTPS://Ex.COM/A?q=1,Test,1,10,0.1,2\n')
 assert rows[0]["page_url"]=="https://ex.com/A?q=1"; c=sqlite3.connect(":memory:"); r=import_blog_search_console_queries(c,rows); assert r["summary"]["applied_count"]==1
 r2=import_blog_search_console_queries(c,rows,dry_run=True); assert r2["summary"]["updated_count"]==1 and r2["summary"]["applied_count"]==0
 p=tmp_path/"in.jsonl"; p.write_text(json.dumps({"observed_at":"2026","page_url":"https://e","query":"q"})+"\n"); db=tmp_path/"d.sqlite"; assert script.main([str(p),"--db",str(db),"--format","json"])==0; assert json.loads(capsys.readouterr().out)["artifact_type"]=="blog_search_console_query_import"
