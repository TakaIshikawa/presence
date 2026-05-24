from __future__ import annotations
from contextlib import contextmanager
import importlib.util, json, sqlite3
from pathlib import Path
from types import SimpleNamespace
import pytest
from evaluation.blog_draft_social_card_export import build_blog_draft_social_card_export, build_blog_draft_social_card_export_report_from_db
SCRIPT_PATH=Path(__file__).resolve().parent.parent/'scripts'/'export_blog_draft_social_cards.py'
spec=importlib.util.spec_from_file_location('export_blog_draft_social_cards_script', SCRIPT_PATH); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
@contextmanager
def _script_context(db): yield SimpleNamespace(), db

def test_exporter_and_cli(tmp_path, monkeypatch, capsys):
 r=build_blog_draft_social_card_export([{'id':1,'title':'T'*80,'description':'D'*220,'canonical_url':'u','tags':'a,b'}], platform=['x'])
 assert r['artifact_type']=='blog_draft_social_card_export'; assert len(r['packets'][0]['previews']['x']['title'])<=60
 assert build_blog_draft_social_card_export_report_from_db(sqlite3.connect(':memory:'))['missing_tables']==['blog_drafts|generated_content']
 p=tmp_path/'d.sqlite'; db=sqlite3.connect(p); db.executescript("CREATE TABLE blog_drafts(id INTEGER, title TEXT, description TEXT, body TEXT, canonical_url TEXT, image_url TEXT, alt_text TEXT, tags TEXT); INSERT INTO blog_drafts VALUES(1,'Title','Desc','Body','url','img','alt','tag');"); db.close()
 assert script.main(['--db',str(p),'--format','json','--platform','x'])==0; assert json.loads(capsys.readouterr().out)['packets'][0]['title']=='Title'
 monkeypatch.setattr(script,'script_context',lambda:_script_context(sqlite3.connect(':memory:'))); assert script.main(['--format','json'])==0
 with pytest.raises(SystemExit): script.parse_args(['--limit','0'])
