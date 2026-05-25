from __future__ import annotations
import importlib.util, sqlite3
from pathlib import Path
from ingestion.github_release_note_import import parse_github_release_notes, upsert_github_release_notes
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_github_release_notes.py"; spec=importlib.util.spec_from_file_location("import_github_release_notes_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_release_note_import_invalid_and_cli(tmp_path):
    rows=parse_github_release_notes('{"repo":"OWNER/Repo","tag_name":"v1","name":"One"}'); assert rows[0]["repo"]=="owner/repo"
    c=sqlite3.connect(":memory:"); upsert_github_release_notes(c,rows); upsert_github_release_notes(c,[{**rows[0],"title":"Two"}]); assert c.execute("SELECT title FROM github_release_notes").fetchone()[0]=="Two"
    p=tmp_path/"r.json"; p.write_text('{"repo":"x/y","tag_name":"v2"}'); db=tmp_path/"db.sqlite"; assert script.main(["--db",str(db),"--input",str(p),"--format","text"])==0
    bad=tmp_path/"bad.json"; bad.write_text('{"repo":"x"}'); assert script.main(["--db",str(db),"--input",str(bad)])==1
