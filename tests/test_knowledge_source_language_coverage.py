from __future__ import annotations
from contextlib import contextmanager
import importlib.util, json, sqlite3
from pathlib import Path
from types import SimpleNamespace
import pytest
from evaluation.knowledge_source_language_coverage import build_knowledge_source_language_coverage_report, build_knowledge_source_language_coverage_report_from_db
SCRIPT_PATH=Path(__file__).resolve().parent.parent/'scripts'/'knowledge_source_language_coverage.py'
spec=importlib.util.spec_from_file_location('knowledge_source_language_coverage_script', SCRIPT_PATH); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
@contextmanager
def _script_context(db): yield SimpleNamespace(), db

def test_builder_and_cli(tmp_path, monkeypatch, capsys):
 r=build_knowledge_source_language_coverage_report([{'id':1,'language':'','campaign_id':'c'},{'id':2,'language':'jp','campaign_id':'c'},{'id':3,'language':'en','campaign_id':'c'}], supported_language=['en'], max_language_share=.3)
 assert r['artifact_type']=='knowledge_source_language_coverage'; assert {'missing_language','unsupported_language','campaign_language_skew'} <= {f['reason'] for f in r['findings']}
 assert build_knowledge_source_language_coverage_report_from_db(sqlite3.connect(':memory:'))['missing_tables']==['knowledge_sources']
 p=tmp_path/'d.sqlite'; db=sqlite3.connect(p); db.executescript("CREATE TABLE knowledge_sources(id INTEGER, language TEXT, campaign_id TEXT); INSERT INTO knowledge_sources VALUES(1,'','c');"); db.close()
 assert script.main(['--db',str(p),'--format','json'])==0; assert json.loads(capsys.readouterr().out)['artifact_type']=='knowledge_source_language_coverage'
 monkeypatch.setattr(script,'script_context',lambda:_script_context(sqlite3.connect(':memory:'))); assert script.main(['--format','json'])==0
 with pytest.raises(SystemExit): script.parse_args(['--limit','0'])
