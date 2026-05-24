from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.reply_unanswered_question_gaps import build_reply_unanswered_question_gaps_report_from_db, format_reply_unanswered_question_gaps_json, format_reply_unanswered_question_gaps_text
SCRIPT=Path(__file__).resolve().parent.parent/'scripts'/'reply_unanswered_question_gaps.py'; spec=importlib.util.spec_from_file_location('reply_unanswered_question_gaps_script',SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c=sqlite3.connect(':memory:'); c.row_factory=sqlite3.Row; c.executescript('CREATE TABLE inbound_mentions (id TEXT, author TEXT, platform TEXT, text TEXT, received_at TEXT); CREATE TABLE reply_drafts (mention_id TEXT, status TEXT);')
    c.executemany('INSERT INTO inbound_mentions VALUES (?,?,?,?,?)',[('m1','a','x','How does this work','2026-05-01T00:00:00+00:00'),('m2','b','x','Nice post','2026-05-01T00:00:00+00:00'),('m3','c','mastodon','Can you help?','2026-05-01T00:00:00+00:00')]); c.execute('INSERT INTO reply_drafts VALUES (?,?)',('m3','queued')); c.commit(); return c
def test_questions_without_posted_or_queued_reply_and_cli(tmp_path,capsys):
    r=build_reply_unanswered_question_gaps_report_from_db(_db(),now='2026-05-03T00:00:00+00:00')
    assert [f['mention_id'] for f in r['findings']]==['m1']
    assert json.loads(format_reply_unanswered_question_gaps_json(r))['artifact_type']=='reply_unanswered_question_gaps'
    assert 'Reply Unanswered' in format_reply_unanswered_question_gaps_text(r)
    db=tmp_path/'db.sqlite'; out=sqlite3.connect(db); _db().backup(out); out.close()
    assert script.main(['--db',str(db),'--format','text','--platform','x'])==0
    assert 'm1' in capsys.readouterr().out
