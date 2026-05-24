from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.publication_media_attachment_mismatch import build_publication_media_attachment_mismatch_report_from_db, format_publication_media_attachment_mismatch_json, format_publication_media_attachment_mismatch_text
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"publication_media_attachment_mismatch.py"; spec=importlib.util.spec_from_file_location("publication_media_attachment_mismatch_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.executescript("CREATE TABLE generated_content (id TEXT, metadata TEXT); CREATE TABLE publication_attempts (id TEXT, content_id TEXT, platform TEXT, payload TEXT, created_at TEXT);")
    c.executemany("INSERT INTO generated_content VALUES (?,?)",[("c1",'{"media":["a","b"]}'),("c2",'{}'),("c3",'{"media":["x"]}')])
    c.executemany("INSERT INTO publication_attempts VALUES (?,?,?,?,?)",[("a1","c1","x",'{"media":[]}',"2026-05-01"),("a2","c2","x",'{"media":["z"]}',"2026-05-01"),("a3","c3","y",'{"media":["x"]}',"2026-05-01")]); c.commit(); return c
def test_mismatches_filter_and_cli(tmp_path,capsys):
    r=build_publication_media_attachment_mismatch_report_from_db(_db(),platform="x")
    assert [f["mismatch_type"] for f in r["findings"]]==["missing_media","unexpected_media"]
    assert "Publication Media" in format_publication_media_attachment_mismatch_text(r)
    assert json.loads(format_publication_media_attachment_mismatch_json(r))["artifact_type"]=="publication_media_attachment_mismatch"
    db=tmp_path/"db.sqlite"; out=sqlite3.connect(db); _db().backup(out); out.close()
    assert script.main(["--db",str(db),"--format","json","--platform","x"])==0
    assert json.loads(capsys.readouterr().out)["totals"]["findings"]==2
