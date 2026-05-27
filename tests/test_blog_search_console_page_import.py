from __future__ import annotations
import importlib.util, sqlite3
from pathlib import Path
from ingestion.blog_search_console_page_import import parse_blog_search_console_pages, upsert_blog_search_console_pages
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_blog_search_console_pages.py"; spec=importlib.util.spec_from_file_location("script_gsc",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_search_console_page_import(tmp_path):
    rows=parse_blog_search_console_pages('[{"page_url":"EX.com/a#x","date":"2026-05-01","ctr":"12.5%","position":"3.2"}]'); assert rows[0]["page_url"]=="https://ex.com/a" and rows[0]["ctr"]==0.125
    c=sqlite3.connect(":memory:"); upsert_blog_search_console_pages(c,rows); upsert_blog_search_console_pages(c,[{**rows[0],"clicks":9}]); assert c.execute("SELECT count(*),clicks FROM blog_search_console_pages").fetchone()==(1,9)
    p=tmp_path/"g.csv"; p.write_text("page_url,date,ctr\nex.com/a,2026-05-01,0.5\n"); assert script.main(["--db",str(tmp_path/"db.sqlite"),"--input",str(p)])==0
