from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from ingestion.github_release_note_import import import_github_release_notes
SCRIPT=Path(__file__).resolve().parent.parent/'scripts'/'import_github_release_notes.py'; spec=importlib.util.spec_from_file_location('import_github_release_notes_script',SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_release_note_upsert_invalid_and_cli(tmp_path,capsys):
    p=tmp_path/'rel.jsonl'; p.write_text('{"repo":"Org/Repo","tag_name":"v1","name":"One","body":"old","published_at":"2026-05-01","prerelease":true}\n')
    c=sqlite3.connect(':memory:'); assert import_github_release_notes(c,p)['upserted']==1
    assert tuple(c.execute('SELECT repo, title, prerelease FROM github_release_notes').fetchone())==('org/repo','One',1)
    p.write_text('{"repo":"Org/Repo","tag_name":"v1","name":"Two"}\n'); import_github_release_notes(c,p)
    assert c.execute('SELECT title FROM github_release_notes').fetchone()[0]=='Two'
    db=tmp_path/'db.sqlite'; assert script.main(['--db',str(db),'--input',str(p),'--dry-run','--format','json'])==0
    assert json.loads(capsys.readouterr().out)['parsed']==1
    bad=tmp_path/'bad.json'; bad.write_text('[{"repo":"x"}]')
    assert script.main(['--db',str(db),'--input',str(bad)])==1
