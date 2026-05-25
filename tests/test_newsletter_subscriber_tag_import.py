from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from ingestion.newsletter_subscriber_tag_import import parse_newsletter_subscriber_tags, upsert_newsletter_subscriber_tags
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_newsletter_subscriber_tags.py"; spec=importlib.util.spec_from_file_location("import_newsletter_subscriber_tags_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_subscriber_tag_import_cli(tmp_path,capsys):
    raw="email,tag,observed_at,source\nA@EXAMPLE.COM,vip,2026-05-01,esp\n"; rows=parse_newsletter_subscriber_tags(raw); assert rows[0]["email"]=="a@example.com"
    c=sqlite3.connect(":memory:"); assert upsert_newsletter_subscriber_tags(c,rows)["upserted_count"]==1
    p=tmp_path/"tags.csv"; p.write_text(raw); db=tmp_path/"db.sqlite"; assert script.main(["--db",str(db),"--input",str(p),"--dry-run","--format","json"])==0
    assert json.loads(capsys.readouterr().out)["parsed_count"]==1
    bad=tmp_path/"bad.json"; bad.write_text('{"tag":"x"}'); assert script.main(["--db",str(db),"--input",str(bad)])==1
