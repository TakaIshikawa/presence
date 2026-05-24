from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from ingestion.newsletter_subscriber_tag_import import import_newsletter_subscriber_tags
SCRIPT=Path(__file__).resolve().parent.parent/'scripts'/'import_newsletter_subscriber_tags.py'; spec=importlib.util.spec_from_file_location('import_newsletter_subscriber_tags_script',SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_parse_upsert_dry_run_and_cli(tmp_path,capsys):
    p=tmp_path/'tags.csv'; p.write_text('email,tag,observed_at,source,status\na@test,pro,2026-05-01,esp,active\n')
    c=sqlite3.connect(':memory:'); r=import_newsletter_subscriber_tags(c,p)
    assert r['upserted']==1
    assert c.execute('SELECT tag FROM newsletter_subscriber_tags').fetchone()[0]=='pro'
    assert import_newsletter_subscriber_tags(c,p,dry_run=True)['upserted']==0
    db=tmp_path/'db.sqlite'
    assert script.main(['--db',str(db),'--input',str(p),'--dry-run','--format','json'])==0
    assert json.loads(capsys.readouterr().out)['dry_run'] is True
    bad=tmp_path/'bad.json'; bad.write_text('[{"email":"a@test"}]')
    assert script.main(['--db',str(db),'--input',str(bad)])==1
