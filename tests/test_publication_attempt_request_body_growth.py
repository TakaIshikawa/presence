from __future__ import annotations
from contextlib import contextmanager
import importlib.util, json, sqlite3
from pathlib import Path
from types import SimpleNamespace
import pytest
from evaluation.publication_attempt_request_body_growth import build_publication_attempt_request_body_growth_report, build_publication_attempt_request_body_growth_report_from_db
SCRIPT_PATH=Path(__file__).resolve().parent.parent/'scripts'/'publication_attempt_request_body_growth.py'
spec=importlib.util.spec_from_file_location('publication_attempt_request_body_growth_script', SCRIPT_PATH); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
@contextmanager
def _script_context(db): yield SimpleNamespace(), db

def test_builder_and_cli(tmp_path, monkeypatch, capsys):
 r=build_publication_attempt_request_body_growth_report([{'id':1,'provider':'p','platform':'x','request_payload':'a'},{'id':2,'provider':'p','platform':'x','request_payload':'a'*10}], max_bytes=5, growth_ratio=5)
 assert r['artifact_type']=='publication_attempt_request_body_growth'; assert {'max_payload_exceeded','growth_ratio_exceeded'} <= {f['reason'] for f in r['findings']}
 assert build_publication_attempt_request_body_growth_report_from_db(sqlite3.connect(':memory:'))['missing_tables']==['publication_attempts']
 p=tmp_path/'d.sqlite'; db=sqlite3.connect(p); db.executescript("CREATE TABLE publication_attempts(id INTEGER, provider TEXT, platform TEXT, request_payload TEXT); INSERT INTO publication_attempts VALUES(1,'p','x','aaaaaaaaaa');"); db.close()
 assert script.main(['--db',str(p),'--format','json','--max-bytes','5'])==0; assert json.loads(capsys.readouterr().out)['findings'][0]['reason']=='max_payload_exceeded'
 monkeypatch.setattr(script,'script_context',lambda:_script_context(sqlite3.connect(':memory:'))); assert script.main(['--format','json'])==0
 with pytest.raises(SystemExit): script.parse_args(['--limit','0'])
