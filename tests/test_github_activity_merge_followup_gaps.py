from __future__ import annotations
from contextlib import contextmanager
import importlib.util, json, sqlite3
from pathlib import Path
from types import SimpleNamespace
import pytest
from evaluation.github_activity_merge_followup_gaps import build_github_activity_merge_followup_gaps_report, build_github_activity_merge_followup_gaps_report_from_db, format_github_activity_merge_followup_gaps_json
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "github_activity_merge_followup_gaps.py"
spec=importlib.util.spec_from_file_location("github_activity_merge_followup_gaps_script", SCRIPT_PATH); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
@contextmanager
def _script_context(db): yield SimpleNamespace(), db

def test_builder_flags_missing_followups():
    import datetime as dt
    r=build_github_activity_merge_followup_gaps_report([{'activity_id':1,'repository':'r','pr_number':2,'merged_at':'2026-05-01T00:00:00+00:00'}], [], max_age_days=1, now=dt.datetime(2026,5,24,tzinfo=dt.timezone.utc))
    assert r['artifact_type']=='github_activity_merge_followup_gaps'; assert r['findings'][0]['missing_followup_types']==['content_idea','generated_content','newsletter_mention']
def test_db_and_cli(tmp_path, monkeypatch, capsys):
    assert build_github_activity_merge_followup_gaps_report_from_db(sqlite3.connect(':memory:'))['missing_tables']==['github_activity']
    p=tmp_path/'d.sqlite'; db=sqlite3.connect(p); db.executescript("CREATE TABLE github_activity(id INTEGER, repository TEXT, pr_number INTEGER, event_type TEXT, merged_at TEXT); INSERT INTO github_activity VALUES(1,'r',2,'merge','2026-05-01T00:00:00+00:00');"); db.close()
    assert script.main(['--db',str(p),'--format','json'])==0; assert json.loads(capsys.readouterr().out)['artifact_type']=='github_activity_merge_followup_gaps'
    monkeypatch.setattr(script,'script_context',lambda:_script_context(sqlite3.connect(':memory:'))); assert script.main(['--format','json'])==0
    with pytest.raises(SystemExit): script.parse_args(['--max-age-days','0'])
