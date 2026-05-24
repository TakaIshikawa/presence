from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.newsletter_link_dead_clicks import build_newsletter_link_dead_clicks_report_from_db, format_newsletter_link_dead_clicks_json, format_newsletter_link_dead_clicks_text
SCRIPT=Path(__file__).resolve().parent.parent/'scripts'/'newsletter_link_dead_clicks.py'; spec=importlib.util.spec_from_file_location('newsletter_link_dead_clicks_script',SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c=sqlite3.connect(':memory:'); c.row_factory=sqlite3.Row; c.executescript('CREATE TABLE newsletter_link_clicks (issue_id TEXT, url TEXT, click_count INTEGER, clicked_at TEXT); CREATE TABLE newsletter_link_inventory (issue_id TEXT, url TEXT, status TEXT);')
    c.executemany('INSERT INTO newsletter_link_inventory VALUES (?,?,?)',[('i1','https://a.test','dead'),('i1','https://b.test','ok')])
    c.executemany('INSERT INTO newsletter_link_clicks VALUES (?,?,?,?)',[('i1','https://a.test',3,'2026-05-02'),('i1','https://b.test',4,'2026-05-02'),('i2','https://unknown.test',2,'2026-05-02')]); c.commit(); return c
def test_dead_error_unknown_clicks_and_cli(tmp_path,capsys):
    r=build_newsletter_link_dead_clicks_report_from_db(_db(),min_clicks=2)
    assert {f['status'] for f in r['findings']}=={'dead','unknown'}
    assert json.loads(format_newsletter_link_dead_clicks_json(r))['artifact_type']=='newsletter_link_dead_clicks'
    assert 'Newsletter Link' in format_newsletter_link_dead_clicks_text(r)
    db=tmp_path/'db.sqlite'; out=sqlite3.connect(db); _db().backup(out); out.close()
    assert script.main(['--db',str(db),'--format','text','--min-clicks','2'])==0
    assert 'add_to_inventory' in capsys.readouterr().out
