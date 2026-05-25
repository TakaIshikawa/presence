from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.newsletter_link_dead_clicks import build_newsletter_link_dead_clicks_report_from_db
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"newsletter_link_dead_clicks.py"; spec=importlib.util.spec_from_file_location("newsletter_link_dead_clicks_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.executescript("CREATE TABLE newsletter_link_clicks (issue_id TEXT, url TEXT, click_count INTEGER, clicked_at TEXT); CREATE TABLE newsletter_link_inventory (issue_id TEXT, url TEXT, status TEXT);")
    c.executemany("INSERT INTO newsletter_link_clicks VALUES (?,?,?,?)",[("i1","https://x/a",3,"2026-05-01"),("i1","https://x/b",1,"2026-05-01")]); c.executemany("INSERT INTO newsletter_link_inventory VALUES (?,?,?)",[("i1","https://x/a","dead"),("i1","https://x/b","ok")]); c.commit(); return c
def test_dead_clicks_cli(tmp_path,capsys):
    assert build_newsletter_link_dead_clicks_report_from_db(_db(),min_clicks=2)["findings"][0]["status"]=="dead"
    db=tmp_path/"db.sqlite"; out=sqlite3.connect(db); _db().backup(out); out.close()
    assert script.main(["--db",str(db),"--min-clicks","2","--format","json"])==0
    assert json.loads(capsys.readouterr().out)["findings"][0]["click_count"]==3
