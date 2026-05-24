from __future__ import annotations
import importlib.util, sqlite3
from pathlib import Path
from evaluation.content_variant_prompt_family_skew import build_content_variant_prompt_family_skew_report_from_db, format_content_variant_prompt_family_skew_text
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"content_variant_prompt_family_skew.py"; spec=importlib.util.spec_from_file_location("content_variant_prompt_family_skew_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_prompt_family_skew_and_cli(tmp_path,capsys):
 c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.execute("CREATE TABLE content_variants (content_id TEXT, campaign_id TEXT, platform TEXT, prompt_family TEXT, prompt_version TEXT)")
 c.executemany("INSERT INTO content_variants VALUES (?,?,?,?,?)",[("c1","camp","x","sales","v1"),("c1","camp","x","sales","v1"),("c2","camp","x","a","v1"),("c2","camp","x","b","v2")])
 r=build_content_variant_prompt_family_skew_report_from_db(c,min_family_count=2)
 assert [x["content_id"] for x in r["findings"]]==["c1"]
 assert "Prompt Family Skew" in format_content_variant_prompt_family_skew_text(r)
 c.commit(); path=tmp_path/"db.sqlite"
 with sqlite3.connect(path) as out: c.backup(out)
 assert script.main(["--db",str(path),"--format","text","--min-family-count","2"])==0; assert "Prompt Family" in capsys.readouterr().out
 assert script.main(["--db",str(path),"--min-family-count","0"])==2
def test_prompt_family_schema_gap(): assert build_content_variant_prompt_family_skew_report_from_db(sqlite3.connect(":memory:"))["missing_tables"]==["content_variants"]
