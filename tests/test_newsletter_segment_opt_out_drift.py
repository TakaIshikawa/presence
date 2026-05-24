from __future__ import annotations
from contextlib import contextmanager
import importlib.util, json, sqlite3
from pathlib import Path
from types import SimpleNamespace
import pytest
from evaluation.newsletter_segment_opt_out_drift import build_newsletter_segment_opt_out_drift_report, build_newsletter_segment_opt_out_drift_report_from_db
SCRIPT_PATH=Path(__file__).resolve().parent.parent/'scripts'/'newsletter_segment_opt_out_drift.py'
spec=importlib.util.spec_from_file_location('newsletter_segment_opt_out_drift_script', SCRIPT_PATH); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
@contextmanager
def _script_context(db): yield SimpleNamespace(), db

def test_builder_and_cli(tmp_path, monkeypatch, capsys):
 import datetime as dt
 members=[{'subscriber_id':1,'segment':'a'},{'subscriber_id':2,'segment':'a'}]; events=[{'segment':'a','occurred_at':'2026-05-24T00:00:00+00:00'}]
 r=build_newsletter_segment_opt_out_drift_report(members, events, delta_threshold=.1, now=dt.datetime(2026,5,24,tzinfo=dt.timezone.utc))
 assert r['artifact_type']=='newsletter_segment_opt_out_drift'; assert r['findings'][0]['segment']=='a'
 assert build_newsletter_segment_opt_out_drift_report_from_db(sqlite3.connect(':memory:'))['missing_tables']==['newsletter_segment_members']
 p=tmp_path/'d.sqlite'; db=sqlite3.connect(p); db.executescript("CREATE TABLE newsletter_segment_members(subscriber_id INTEGER, segment TEXT); CREATE TABLE newsletter_opt_out_events(subscriber_id INTEGER, segment TEXT, occurred_at TEXT); INSERT INTO newsletter_segment_members VALUES(1,'a'); INSERT INTO newsletter_opt_out_events VALUES(1,'a','2026-05-24T00:00:00+00:00');"); db.close()
 assert script.main(['--db',str(p),'--format','json'])==0; assert json.loads(capsys.readouterr().out)['artifact_type']=='newsletter_segment_opt_out_drift'
 monkeypatch.setattr(script,'script_context',lambda:_script_context(sqlite3.connect(':memory:'))); assert script.main(['--format','json'])==0
 with pytest.raises(SystemExit): script.parse_args(['--window-days','0'])
