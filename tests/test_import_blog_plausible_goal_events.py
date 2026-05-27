from __future__ import annotations
import importlib.util, sqlite3
from pathlib import Path
from ingestion.blog_plausible_goal_event_import import parse_blog_plausible_goal_events, upsert_blog_plausible_goal_events
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_blog_plausible_goal_events.py"; spec=importlib.util.spec_from_file_location("import_blog_plausible_goal_events_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_blog_plausible_goal_event_import_cli(tmp_path):
    rows=parse_blog_plausible_goal_events('{"goals":[{"goal_name":"Signup","url":"https://Example.com/?x=1","visitor_id":"v","occurred_at":"t","metadata":{"b":2,"a":1}}]}')
    assert rows[0]["path"]=="/" and rows[0]["metadata_json"]=='{"a":1,"b":2}'
    assert parse_blog_plausible_goal_events("goal_name,path,visitor_id,occurred_at\nG,/a,v,t\n")[0]["path"]=="/a"
    c=sqlite3.connect(":memory:"); upsert_blog_plausible_goal_events(c,rows); upsert_blog_plausible_goal_events(c,[{**rows[0],"source":"x"}]); assert c.execute("SELECT source FROM blog_plausible_goal_events").fetchone()[0]=="x"
    p=tmp_path/"g.jsonl"; p.write_text('{"goal":"G","path":"/","visitor_id_hash":"v","timestamp":"t"}\n'); db=tmp_path/"db.sqlite"; assert script.main(["--db",str(db),"--input",str(p),"--dry-run"])==0
