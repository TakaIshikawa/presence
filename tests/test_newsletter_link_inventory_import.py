from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from ingestion.newsletter_link_inventory_import import import_newsletter_link_inventory
SCRIPT=Path(__file__).resolve().parent.parent/'scripts'/'import_newsletter_link_inventory.py'; spec=importlib.util.spec_from_file_location('import_newsletter_link_inventory_script',SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_inventory_csv_jsonl_duplicate_dry_run_cli(tmp_path,capsys):
    p=tmp_path/'links.csv'; p.write_text('issue_id,url,link_text,section,position,utm_campaign,observed_at\ni1,https://EX.test/a?u=1#frag,A,top,1,c,2026-05-01\n')
    c=sqlite3.connect(':memory:'); import_newsletter_link_inventory(c,p)
    assert c.execute('SELECT url FROM newsletter_link_inventory').fetchone()[0]=='https://ex.test/a?u=1'
    j=tmp_path/'links.jsonl'; j.write_text('{"issue_id":"i1","url":"https://ex.test/a?u=1","link_text":"B","observed_at":"2026-05-02"}\n')
    import_newsletter_link_inventory(c,j)
    assert c.execute('SELECT link_text FROM newsletter_link_inventory').fetchone()[0]=='B'
    assert import_newsletter_link_inventory(c,j,dry_run=True)['upserted']==0
    db=tmp_path/'db.sqlite'; assert script.main(['--db',str(db),'--input',str(j),'--format','text'])==0
    assert 'upserted=1' in capsys.readouterr().out
