from __future__ import annotations
from contextlib import contextmanager
import importlib.util, json, sqlite3
from pathlib import Path
from types import SimpleNamespace
import pytest
from evaluation.publish_queue_pause_resume_integrity import build_publish_queue_pause_resume_integrity_report, build_publish_queue_pause_resume_integrity_report_from_db, format_publish_queue_pause_resume_integrity_json
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publish_queue_pause_resume_integrity.py"
spec=importlib.util.spec_from_file_location("publish_queue_pause_resume_integrity_script", SCRIPT_PATH); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
@contextmanager
def _script_context(db): yield SimpleNamespace(), db

def test_builder_flags_pause_resume_reasons():
    r=build_publish_queue_pause_resume_integrity_report([
      {'id':1,'queue_item_id':'a','event_type':'resume','occurred_at':'2026-05-24T00:00:00+00:00'},
      {'id':2,'queue_item_id':'b','event_type':'pause','occurred_at':'2026-05-23T00:00:00+00:00'},
      {'id':3,'queue_item_id':'b','event_type':'pause','occurred_at':'2026-05-23T01:00:00+00:00'}], max_pause_hours=1, now=__import__('datetime').datetime(2026,5,24,tzinfo=__import__('datetime').timezone.utc))
    assert r['artifact_type']=='publish_queue_pause_resume_integrity'; assert {f['reason'] for f in r['findings']}=={'orphan_resume','overlapping_pause','pause_duration_exceeded'}
def test_db_and_cli(tmp_path, monkeypatch, capsys):
    c=sqlite3.connect(':memory:'); assert build_publish_queue_pause_resume_integrity_report_from_db(c)['missing_tables']==['publish_queue_events']
    p=tmp_path/'d.sqlite'; db=sqlite3.connect(p); db.executescript("CREATE TABLE publish_queue_events(id INTEGER, queue_item_id TEXT, event_type TEXT, actor TEXT, occurred_at TEXT); INSERT INTO publish_queue_events VALUES(1,'a','resume','me','2026-05-24T00:00:00+00:00');"); db.close()
    assert script.main(['--db',str(p),'--format','json'])==0; assert json.loads(capsys.readouterr().out)['findings'][0]['reason']=='orphan_resume'
    monkeypatch.setattr(script,'script_context',lambda:_script_context(sqlite3.connect(':memory:'))); assert script.main(['--format','json'])==0
    with pytest.raises(SystemExit): script.parse_args(['--limit','0'])
