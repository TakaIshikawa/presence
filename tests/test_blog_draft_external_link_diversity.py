from __future__ import annotations
from contextlib import contextmanager
import importlib.util, json, sqlite3
from pathlib import Path
from types import SimpleNamespace
import pytest
from evaluation.blog_draft_external_link_diversity import build_blog_draft_external_link_diversity_report, build_blog_draft_external_link_diversity_report_from_db, format_blog_draft_external_link_diversity_json
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "blog_draft_external_link_diversity.py"
spec=importlib.util.spec_from_file_location("blog_draft_external_link_diversity_script", SCRIPT_PATH); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
@contextmanager
def _script_context(db): yield SimpleNamespace(), db

def test_builder_flags_link_diversity():
    r=build_blog_draft_external_link_diversity_report([{'id':1,'body':'none'},{'id':2,'body':'https://a.com/x'},{'id':3,'body':'https://a.com/1 https://a.com/2 https://b.com/3'}], min_domains=2, max_domain_share=.6)
    assert r['artifact_type']=='blog_draft_external_link_diversity'; assert [f['reason'] for f in r['findings']]==['dominant_domain','low_domain_diversity','no_external_links']
def test_db_and_cli(tmp_path, monkeypatch, capsys):
    assert build_blog_draft_external_link_diversity_report_from_db(sqlite3.connect(':memory:'))['missing_tables']==['blog_drafts|generated_content']
    p=tmp_path/'d.sqlite'; db=sqlite3.connect(p); db.executescript("CREATE TABLE blog_drafts(id INTEGER, body TEXT); INSERT INTO blog_drafts VALUES(1,'none');"); db.close()
    assert script.main(['--db',str(p),'--format','json'])==0; assert json.loads(capsys.readouterr().out)['findings'][0]['reason']=='no_external_links'
    monkeypatch.setattr(script,'script_context',lambda:_script_context(sqlite3.connect(':memory:'))); assert script.main(['--format','json'])==0
    with pytest.raises(SystemExit): script.parse_args(['--min-domains','0'])
