from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.publication_media_attachment_mismatch import build_publication_media_attachment_mismatch_report_from_db, format_publication_media_attachment_mismatch_json, format_publication_media_attachment_mismatch_text
SCRIPT=Path(__file__).resolve().parent.parent/'scripts'/'publication_media_attachment_mismatch.py'; spec=importlib.util.spec_from_file_location('publication_media_attachment_mismatch_script',SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c=sqlite3.connect(':memory:'); c.row_factory=sqlite3.Row
    c.executescript('CREATE TABLE generated_content (id INTEGER, metadata TEXT); CREATE TABLE publication_attempts (id INTEGER, content_id INTEGER, platform TEXT, payload TEXT, attempted_at TEXT);')
    c.executemany('INSERT INTO generated_content VALUES (?,?)',[(1,'{"expected_media_count":2}'),(2,'{"text_only":true}'),(3,'{"expected_media_count":1}')])
    c.executemany('INSERT INTO publication_attempts VALUES (?,?,?,?,?)',[(10,1,'x','{"media":[]}','2026-05-01'),(11,2,'x','{"media":["u"]}','2026-05-02'),(12,3,'linkedin','{"media":["u"]}','2026-05-03')]); c.commit(); return c
def test_missing_unexpected_matching_platform_and_cli(tmp_path,capsys):
    r=build_publication_media_attachment_mismatch_report_from_db(_db())
    assert [f['mismatch_type'] for f in r['findings']]==['missing_media','unexpected_media']
    assert build_publication_media_attachment_mismatch_report_from_db(_db(),platform='linkedin')['findings']==[]
    assert json.loads(format_publication_media_attachment_mismatch_json(r))['artifact_type']=='publication_media_attachment_mismatch'
    assert 'Publication Media' in format_publication_media_attachment_mismatch_text(r)
    db=tmp_path/'db.sqlite'; out=sqlite3.connect(db); _db().backup(out); out.close()
    assert script.main(['--db',str(db),'--format','json','--platform','x'])==0
    assert json.loads(capsys.readouterr().out)['totals']['findings']==2
