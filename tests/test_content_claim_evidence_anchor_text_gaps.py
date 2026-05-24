from __future__ import annotations
from contextlib import contextmanager
import importlib.util, json, sqlite3
from pathlib import Path
from types import SimpleNamespace
import pytest
from evaluation.content_claim_evidence_anchor_text_gaps import build_content_claim_evidence_anchor_text_gaps_report, build_content_claim_evidence_anchor_text_gaps_report_from_db, format_content_claim_evidence_anchor_text_gaps_json
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_claim_evidence_anchor_text_gaps.py"
spec=importlib.util.spec_from_file_location("content_claim_evidence_anchor_text_gaps_script", SCRIPT_PATH); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
@contextmanager
def _script_context(db): yield SimpleNamespace(), db

def test_builder_flags_anchor_reasons():
    r=build_content_claim_evidence_anchor_text_gaps_report([{'id':1,'claim_id':1,'anchor_text':'','claim_text':'alpha beta'},{'id':2,'claim_id':1,'anchor_text':'click here','claim_text':'alpha beta'},{'id':3,'claim_id':2,'anchor_text':'same','claim_text':'different words'},{'id':4,'claim_id':3,'anchor_text':'same','claim_text':'same claim'}], min_overlap=.5)
    assert r['artifact_type']=='content_claim_evidence_anchor_text_gaps'; assert {'missing_anchor_text','generic_anchor_text','repeated_anchor_text','low_claim_overlap'} <= {f['reason'] for f in r['findings']}
def test_db_and_cli(tmp_path, monkeypatch, capsys):
    assert build_content_claim_evidence_anchor_text_gaps_report_from_db(sqlite3.connect(':memory:'))['missing_tables']==['content_claim_evidence']
    p=tmp_path/'d.sqlite'; db=sqlite3.connect(p); db.executescript("CREATE TABLE content_claim_evidence(id INTEGER, claim_id INTEGER, anchor_text TEXT, claim_text TEXT); INSERT INTO content_claim_evidence VALUES(1,1,'','claim');"); db.close()
    assert script.main(['--db',str(p),'--format','json'])==0; assert json.loads(capsys.readouterr().out)['findings'][0]['reason']=='missing_anchor_text'
    monkeypatch.setattr(script,'script_context',lambda:_script_context(sqlite3.connect(':memory:'))); assert script.main(['--format','json'])==0
    with pytest.raises(SystemExit): script.parse_args(['--limit','0'])
