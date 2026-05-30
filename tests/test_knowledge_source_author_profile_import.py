from __future__ import annotations
import importlib.util, sqlite3, json
from pathlib import Path
from ingestion.knowledge_source_author_profile_import import parse_knowledge_source_author_profiles, upsert_knowledge_source_author_profiles
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_knowledge_source_author_profiles.py"; spec=importlib.util.spec_from_file_location("script_ksap",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_author_profile_import(tmp_path):
    rows=parse_knowledge_source_author_profiles('[{"source_url":"ex.com/a","author_name":"Ann","expertise_tags":"ai;ml","captured_at":"d"}]'); assert json.loads(rows[0]["expertise_tags"])==["ai","ml"]
    c=sqlite3.connect(":memory:"); upsert_knowledge_source_author_profiles(c,rows); upsert_knowledge_source_author_profiles(c,[{**rows[0],"bio":"bio"}]); assert c.execute("SELECT count(*),bio FROM knowledge_source_author_profiles").fetchone()==(1,"bio")
    p=tmp_path/"a.csv"; p.write_text("source_url,author_url,captured_at\nex.com/a,https://ex.com/ann,d\n"); assert script.main(["--db",str(tmp_path/"db.sqlite"),"--input",str(p)])==0
