from __future__ import annotations
import importlib.util,sqlite3
from datetime import datetime,timezone
from pathlib import Path
from evaluation.knowledge_source_domain_concentration import build_knowledge_source_domain_concentration_report
NOW=datetime(2026,6,1,tzinfo=timezone.utc)
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"knowledge_source_domain_concentration.py"; spec=importlib.util.spec_from_file_location("script_knowledge_source_domain_concentration",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_normalizes_and_flags_domain_share():
 rows=[{"id":"1","url":"https://www.Example.com/a?x#f"},{"id":"2","url":"example.com/path"},{"id":"3","url":"https://other.test"}]
 r=build_knowledge_source_domain_concentration_report(rows,max_domain_share=.5,min_sources=3,now=NOW)
 assert r["concentrations"][0]["domain"]=="example.com"
def test_unknown_bucket_and_min_sources():
 assert build_knowledge_source_domain_concentration_report([{"id":"1","url":"not a url"}],min_sources=2,now=NOW)["concentrations"]==[]
def test_cli(tmp_path,capsys):
 db=tmp_path/"db.sqlite"; c=sqlite3.connect(db); c.execute("CREATE TABLE knowledge_sources (id TEXT,url TEXT)"); c.executemany("INSERT INTO knowledge_sources VALUES (?,?)",[("1","a.com"),("2","a.com"),("3","b.com")]); c.commit(); c.close()
 assert script.main(["--db",str(db),"--format","text","--max-domain-share",".5"])==0; assert capsys.readouterr().out
